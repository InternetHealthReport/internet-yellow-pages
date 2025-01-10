import argparse
import logging
import os
import sys
from urllib.error import HTTPError

import arrow
import iso3166
import pandas as pd

from iyp import BaseCrawler

# Data source Google (archived on GitHub by IHR)
ORG = 'Google'
URL = 'https://github.com/InternetHealthReport/crux-top-lists-country/raw/refs/heads/main/data/country'
NAME = 'google.crux_top1m_country'


def generate_url(country_code, date):
    joined_url = os.path.join(URL, country_code.lower(), f'{date.year}{date.month:02d}.csv.gz')
    return joined_url


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://developer.chrome.com/docs/crux/methodology'
        self.__reset_data()

    def __reset_data(self):
        self.rankings = set()
        self.hostnames = set()
        self.countries = set()
        self.country_links = list()
        self.rank_links = list()

    def __push_data(self):
        # Create/fetch corresponding nodes in IYP
        ranking_id = self.iyp.batch_get_nodes_by_single_prop('Ranking', 'name', self.rankings, all=False)
        hostname_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', self.hostnames, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', self.countries, all=False)

        # Replace link ends with QIDs
        for link in self.country_links:
            link['src_id'] = ranking_id[link['src_id']]
            link['dst_id'] = country_id[link['dst_id']]

        for link in self.rank_links:
            link['src_id'] = hostname_id[link['src_id']]
            link['dst_id'] = ranking_id[link['dst_id']]

        # Create the (:Ranking)-[:COUNTRY]-(:Country) relationship
        self.iyp.batch_add_links('COUNTRY', self.country_links)
        # Create the (:HostName)-[:RANK]-(:Ranking) relationship
        self.iyp.batch_add_links('RANK', self.rank_links)

    def run(self):
        """Fetch data and push to IYP."""
        end = arrow.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start = end.shift(months=-3)

        # Process data in batches, since fetching all hostnames at once crashes the
        # script.
        country_batch_size = 50
        count = 0

        for country in iso3166.countries:
            country_code = country.alpha2

            df = None

            for date in reversed(list(arrow.Arrow.range('month', start, end))):
                # Fetch data
                try:
                    url = generate_url(country_code, date)
                    df = pd.read_csv(url)

                    # Set the modification time to the time of the dump
                    self.reference['reference_time_modification'] = date.datetime
                    self.reference['reference_url_data'] = url
                    break
                except HTTPError:
                    # Data not available for this timestamp.
                    continue

            if df is None:
                # This country is not in the dataset or no data available within the
                # timeframe.
                continue

            # Extract hostname from "URLs"
            df['hostname'] = df['origin'].str.partition('://')[2]

            ranking_name = f'CrUX top 1M ({country_code})'

            self.hostnames.update(df['hostname'].unique())
            self.rankings.add(ranking_name)
            self.countries.add(country_code)

            self.country_links.append({
                'src_id': ranking_name,
                'dst_id': country_code,
                'props': [
                    self.reference.copy()
                ]
            })

            for row in df.itertuples():
                self.rank_links.append({
                    'src_id': row.hostname,
                    'dst_id': ranking_name,
                    'props': [
                        self.reference.copy(),
                        {'rank': row.rank, 'origin': row.origin, 'country_code': country_code}
                    ]
                })
            count += 1
            if count == country_batch_size:
                count = 0
                self.__push_data()
                self.__reset_data()

        # Push remaining data.
        self.__push_data()

    def unit_test(self):
        # Unit test checks for existence of relationships created by this crawler.
        return super().unit_test(['COUNTRY', 'RANK'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

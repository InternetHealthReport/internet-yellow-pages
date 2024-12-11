import argparse
import logging
import os
import sys
from urllib.error import HTTPError

import arrow
import iso3166
import pandas as pd

from iyp import BaseCrawler

# Data source Google (archived on github by IHR)
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

    def run(self):
        """Fetch data and push to IYP."""

        rankings = set()
        hostnames = set()
        countries = set()

        country_links = list()
        rank_links = list()
        for country in iso3166.countries:
            country_code = country.alpha2

            end = arrow.utcnow().replace(day=1, hour=0, minute=0, microsecond=0)
            start = end.shift(months=-3)
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

            # Create/fetch corresponding nodes in IYP
            hostnames.update(df['hostname'].unique())
            rankings.add(ranking_name)
            countries.add(country_code)

            country_links.append({
                'src_id': ranking_name,
                'dst_id': country_code,
                'props': [
                    self.reference.copy()
                ]
            })

            for row in df.itertuples():
                rank_links.append({
                    'src_id': row.hostname,
                    'dst_id': ranking_name,
                    'props': [
                        self.reference.copy(),
                        {'rank': row.rank, 'origin': row.origin, 'country_code': country_code}
                    ]
                })

        ranking_id = self.iyp.batch_get_nodes_by_single_prop('Ranking', 'name', rankings, all=False)
        hostname_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', hostnames, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries, all=False)

        for link in country_links:
            link['src_id'] = ranking_id[link['src_id']]
            link['dst_id'] = country_id[link['dst_id']]

        for link in rank_links:
            link['src_id'] = hostname_id[link['src_id']]
            link['dst_id'] = ranking_id[link['dst_id']]

        # Create the (:Ranking)-[:COUNTRY]-(:Country) relationship
        self.iyp.batch_add_links('COUNTRY', country_links)
        # Create the (:HostName)-[:RANK]-(:Ranking) relationship
        self.iyp.batch_add_links('RANK', rank_links)

    def unit_test(self):
        # Unit test checks for existence of relationships created by this crawler.
        return super().unit_test(['COUNTRY', 'RANK'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
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

import argparse
import logging
import os
import sys
from datetime import timezone
from urllib.error import HTTPError

import arrow
import pandas as pd
from iso3166 import countries

from iyp import BaseCrawler

# Data source Google (archived on github by IHR)
ORG = 'Google'
URL = 'https://github.com/InternetHealthReport/crux-top-lists-country/main/data/country/'
NAME = 'google.crux_top1m_country'


def generate_url(country_code, date):
    base_url = 'https://github.com/InternetHealthReport/crux-top-lists-country/raw/refs/heads/main/data/country/'
    joined_url = os.path.join(base_url, country_code.lower(), f'{date.year}{date.month:02d}.csv.gz')
    return joined_url


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://developer.chrome.com/docs/crux/methodology'

    def run(self):
        """Fetch data and push to IYP."""

        for country in countries:
            country_code = country.alpha2

            start = arrow.now().shift(months=-3)
            end = arrow.now()
            df = None

            for date in reversed(list(arrow.Arrow.range('month', start, end))):
                # Fetch data
                try:
                    df = pd.read_csv(generate_url(country_code, date))

                    # Set the modification time to the time of the dump
                    self.reference['reference_time_modification'] = date.replace(tzinfo=timezone.utc)

                except HTTPError:
                    continue

            if df is None:
                # this country is not in the dataset
                continue

            # Extract hostname from "URLs"
            df['hostname'] = df['origin'].str.partition('://')[2]

            # Create/fetch corresponding nodes in IYP
            nodes = set(df['hostname'].unique())
            node_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', nodes, all=False)
            ranking = self.iyp.get_node('Ranking', {'name': f'CrUX top 1M ({country_code})'})
            country = self.iyp.get_node('Country', {'country_code': country_code}, ['country_code'])

            # Create the (:Ranking)-[:COUNTRY]-(:Country) relationship
            self.iyp.add_links(ranking, [['COUNTRY', country, self.reference]])

            # Compute hostname/ranking relationships
            links = list()

            for _, row in df.iterrows():
                links.append({
                    'src_id': node_id[row['hostname']],
                    'dst_id': ranking,
                    'props': [
                        self.reference,
                        {'rank': row['rank'], 'origin': row['origin']}
                    ]
                })

            # Create the (:HostName)-[:RANK]-(:Ranking) relationship
            self.iyp.batch_add_links('RANK', links)

    def unit_test(self):
        # Unit test checks for existence of relationships created by this crawler.
        return super().unit_test(['RANK'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
        level=logging.WARNING,
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

import argparse
import logging
import os
import sys

import pandas as pd

from iyp.crawlers.openintel import OpenIntelCrawler

URL = 'https://data.openintel.nl'
ORG = 'OpenINTEL'
NAME = 'openintel.infra_mx'

DATASET = 'infra:mx'


class Crawler(OpenIntelCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, DATASET)

    def run(self):
        """Fetch the NS data, populate a data frame, and process lines one by one."""
        attempt = 5
        self.pandas_df_list = []  # List of Parquet file-specific Pandas DataFrames

        while len(self.pandas_df_list) == 0 and attempt > 0:
            self.get_parquet()
            attempt -= 1

        # Concatenate Parquet file-specific DFs
        pandas_df = pd.concat(self.pandas_df_list)

        # Select registered domain name (SLD) to IPv4 address mappings from the
        # measurement data
        df = pandas_df[
            # IPv4 record
            (pandas_df.response_type == 'A') &
            # Filter out non-apex records
            (~pandas_df.query_name.str.startswith('www.')) &
            # Filter missing IPv4 addresses (there is at least one...)
            (pandas_df.ip4_address.notnull())
        ][['query_name', 'ip4_address']].drop_duplicates()
        df.query_name = df.query_name.str[:-1]  # Remove root '.'

        print('Read {} unique A records from {} Parquet file(s).'.format(len(df), len(self.pandas_df_list)))

        mx_id = self.iyp.batch_get_nodes('MailServer', 'name', set(df['query_name']))
        ip_id = self.iyp.batch_get_nodes('IP', 'ip', set(df['ip4_address']))

        print('Computing links.')
        links = pd.DataFrame()
        links['src_id'] = df['query_name'].apply(lambda x: mx_id[x])
        links['dst_id'] = df['ip4_address'].apply(lambda x: ip_id[x])
        links['props'] = df['ip4_address'].apply(lambda _: [self.reference])

        # Push all links to IYP
        print('Pushing {} MX links.'.format(len(links)))
        self.iyp.batch_add_links('RESOLVES_TO', links.to_dict('records'))
        print('Done!')


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
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

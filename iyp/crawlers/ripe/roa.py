import argparse
import logging
import lzma
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO
from ipaddress import ip_network

import requests

from iyp import BaseCrawler

# URL to RIPE repository
URL = 'https://ftp.ripe.net/rpki/'
ORG = 'RIPE NCC'
NAME = 'ripe.roa'

TALS = ['afrinic.tal', 'apnic.tal', 'arin.tal', 'lacnic.tal', 'ripencc.tal']


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialize IYP and statements for pushed data."""

        now = datetime.now(tz=timezone.utc)
        self.date_path = f'{now.year}/{now.month:02d}/{now.day:02d}'

        # Check if today's data is available
        self.url = f'{URL}/afrinic.tal/{self.date_path}/roas.csv.xz'
        req = requests.head(self.url)
        if req.status_code != 200:
            now -= timedelta(days=1)
            self.date_path = f'{now.year}/{now.month:02d}/{now.day:02d}'
            logging.warning("Today's data not yet available!")
            logging.warning("Using yesterday's data: " + self.date_path)

        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://rpki-study.github.io/rpki-archive/'
        self.reference['reference_time_modification'] = now.replace(hour=0, minute=0, second=0)

    def run(self):
        """Fetch data from RIPE and push to IYP."""
        for tal in TALS:
            self.url = f'{URL}/{tal}/{self.date_path}/roas.csv.xz'
            logging.info(f'Fetching ROA file: {self.url}')
            req = requests.get(self.url)
            req.raise_for_status()

            # Decompress the .xz file and read it as CSV
            with lzma.open(BytesIO(req.content)) as xz_file:
                csv_content = xz_file.read().decode('utf-8').splitlines()

            # Aggregate data per prefix
            asns = set()
            prefix_info = defaultdict(list)
            for line in csv_content:
                url, asn, prefix, max_length, start, end = line.split(',')

                # Skip header
                if url == 'URI':
                    continue

                try:
                    prefix = ip_network(prefix).compressed
                except ValueError as e:
                    logging.warning(f'Ignoring malformed prefix: "{prefix}": {e}')
                    continue

                asn = int(asn.replace('AS', ''))
                asns.add(asn)
                prefix_info[prefix].append({
                    'url': url,
                    'asn': asn,
                    'max_length': max_length,
                    'start': start,
                    'end': end})

            # get ASNs and prefixes IDs
            asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
            prefix_id = self.iyp.batch_get_nodes_by_single_prop(
                    'RPKIPrefix', 'prefix', set(prefix_info.keys()), all=False)
            self.iyp.batch_add_node_label(list(prefix_id.values()), 'Prefix')

            links = []
            for prefix, attributes in prefix_info.items():
                for att in attributes:

                    vrp = {
                        'notBefore': att['start'],
                        'notAfter': att['end'],
                        'uri': att['url'],
                        'maxLength': att['max_length']
                    }
                    asn_qid = asn_id[att['asn']]
                    prefix_qid = prefix_id[prefix]
                    links.append({'src_id': asn_qid, 'dst_id': prefix_qid,
                                  'props': [self.reference, vrp]})  # Set AS name

            # Push all links to IYP
            self.iyp.batch_add_links('ROUTE_ORIGIN_AUTHORIZATION', links)

    def unit_test(self):
        return super().unit_test(['ROUTE_ORIGIN_AUTHORIZATION'])


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

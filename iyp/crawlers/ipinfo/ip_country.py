import argparse
import gzip
import json
import logging
import os
import sys
from ipaddress import ip_address, summarize_address_range

import requests

from iyp import (BaseCrawler, RequestStatusError,
                 set_modification_time_from_last_modified_header)

ORG = 'IPinfo'
URL = 'https://ipinfo.io/data/free/country.json.gz'
NAME = 'ipinfo.ip_country'

IPINFO_TOKEN = ''
if os.path.exists('config.json'):
    IPINFO_TOKEN = json.load(open('config.json', 'r'))['ipinfo']['token']


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://ipinfo.io/products/free-ip-database'

    def run(self):
        """Fetch data and push to IYP."""

        headers = {'Authorization': f'Bearer {IPINFO_TOKEN}'}
        req = requests.get(self.reference['reference_url_data'], headers=headers)
        if req.status_code != 200:
            logging.error(f'Cannot download data {req.status_code}: {req.text}')
            raise RequestStatusError('Error while fetching data file.')

        set_modification_time_from_last_modified_header(self.reference, req)
        rows = gzip.decompress(req.content)

        countries = set()
        prefixes = set()
        links = []

        for row in rows.splitlines():
            doc = json.loads(row)
            start, end = ip_address(doc['start_ip']), ip_address(doc['end_ip'])
            for prefix in summarize_address_range(start, end):
                country, prefix = doc['country'], str(prefix)
                countries.add(country)
                prefixes.add(prefix)
                links.append({'src_id': prefix, 'dst_id': country, 'props': [self.reference]})

        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries, all=False)
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes, all=False)

        for link in links:
            link['src_id'] = prefix_id[link['src_id']]
            link['dst_id'] = country_id[link['dst_id']]

        self.iyp.batch_add_links('COUNTRY', links)

    def unit_test(self):
        return super().unit_test(['COUNTRY'])


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

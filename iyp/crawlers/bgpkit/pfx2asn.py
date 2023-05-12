import argparse
import bz2
import json
import logging
import os
import sys

import requests

from iyp import BaseCrawler

URL = 'https://data.bgpkit.com/pfx2as/pfx2as-latest.json.bz2'
ORG = 'BGPKIT'
NAME = 'bgpkit.pfx2asn'


class Crawler(BaseCrawler):

    def run(self):
        """Fetch the prefix to ASN file from BGPKIT website and process lines one by
        one."""

        req = requests.get(URL, stream=True)
        if req.status_code != 200:
            sys.exit('Error while fetching pfx2as relationships')

        entries = []
        asns = set()
        prefixes = set()

        for entry in json.load(bz2.open(req.raw)):
            prefixes.add(entry['prefix'])
            asns.add(entry['asn'])
            entries.append(entry)

        req.close()

        logging.info('Pushing nodes to neo4j...\n')
        # get ASNs and prefixes IDs
        self.asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
        self.prefix_id = self.iyp.batch_get_nodes('Prefix', 'prefix', prefixes)

        # Compute links
        links = []
        for entry in entries:
            asn_qid = self.asn_id[entry['asn']]
            prefix_qid = self.prefix_id[entry['prefix']]

            links.append({'src_id': asn_qid, 'dst_id': prefix_qid, 'props': [self.reference, entry]})  # Set AS name

        logging.info('Pushing links to neo4j...\n')
        # Push all links to IYP
        self.iyp.batch_add_links('ORIGINATE', links)


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

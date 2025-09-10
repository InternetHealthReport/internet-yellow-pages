import argparse
import bz2
import json
import logging
import sys
from ipaddress import ip_network

import requests

from iyp import BaseCrawler, set_modification_time_from_last_modified_header

URL = 'https://data.bgpkit.com/pfx2as/pfx2as-latest.json.bz2'
ORG = 'BGPKIT'
NAME = 'bgpkit.pfx2asn'


class Crawler(BaseCrawler):

    def run(self):
        """Fetch the prefix to ASN file from BGPKIT website and process lines one by
        one."""

        req = requests.get(URL, stream=True)
        req.raise_for_status()

        set_modification_time_from_last_modified_header(self.reference, req)

        entries = []
        asns = set()
        prefixes = set()

        for entry in json.load(bz2.open(req.raw)):
            try:
                prefix = ip_network(entry['prefix']).compressed
            except ValueError as e:
                logging.warning(f'Ignoring malformed prefix: "{entry["prefix"]}": {e}')
                continue
            entry['prefix'] = prefix
            prefixes.add(prefix)
            asns.add(entry['asn'])
            entries.append(entry)

        req.close()

        # get ASNs and prefixes IDs
        self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        self.prefix_id = self.iyp.batch_get_nodes_by_single_prop('BGPPrefix', 'prefix', prefixes, all=False)
        # Add the Prefix label
        self.iyp.batch_add_node_label(list(self.prefix_id.values()), 'Prefix')

        # Compute links
        links = []
        for entry in entries:
            asn_qid = self.asn_id[entry['asn']]
            prefix_qid = self.prefix_id[entry['prefix']]

            links.append({'src_id': asn_qid, 'dst_id': prefix_qid, 'props': [self.reference, entry]})

        # Push all links to IYP
        self.iyp.batch_add_links('ORIGINATE', links)

    def unit_test(self):
        return super().unit_test(['ORIGINATE'])


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

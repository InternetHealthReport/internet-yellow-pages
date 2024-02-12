import argparse
import bz2
import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests

from iyp import BaseCrawler, RequestStatusError

URL = 'https://data.bgpkit.com/pfx2as/pfx2as-latest.json.bz2'
ORG = 'BGPKIT'
NAME = 'bgpkit.pfx2asn'


class Crawler(BaseCrawler):

    def run(self):
        """Fetch the prefix to ASN file from BGPKIT website and process lines one by
        one."""

        req = requests.get(URL, stream=True)
        if req.status_code != 200:
            raise RequestStatusError(f'Error while fetching pfx2as relationships: {req.status_code}')

        try:
            last_modified_str = req.headers['Last-Modified']
            # All HTTP dates are in UTC:
            # https://www.rfc-editor.org/rfc/rfc2616#section-3.3.1
            last_modified = datetime.strptime(last_modified_str,
                                              '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = last_modified
        except KeyError:
            logging.warning('No Last-Modified header; will not set modification time.')
        except ValueError as e:
            logging.error(f'Failed to parse Last-Modified header "{last_modified_str}": {e}')

        entries = []
        asns = set()
        prefixes = set()

        for entry in json.load(bz2.open(req.raw)):
            prefixes.add(entry['prefix'])
            asns.add(entry['asn'])
            entries.append(entry)

        req.close()

        logging.info('Pushing nodes to neo4j...')
        # get ASNs and prefixes IDs
        self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        self.prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes)

        # Compute links
        links = []
        for entry in entries:
            asn_qid = self.asn_id[entry['asn']]
            prefix_qid = self.prefix_id[entry['prefix']]

            links.append({'src_id': asn_qid, 'dst_id': prefix_qid, 'props': [self.reference, entry]})  # Set AS name

        logging.info('Pushing links to neo4j...')
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

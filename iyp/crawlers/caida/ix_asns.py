import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import arrow
import flatdict
import requests

from iyp import BaseCrawler, RequestStatusError

URL = 'https://publicdata.caida.org/datasets/ixps/'
ORG = 'CAIDA'
NAME = 'caida.ix_asns'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialization: Find the latest file and set the URL"""

        date = arrow.now()

        for _ in range(6):
            full_url = url + f'ix-asns_{date.year}{date.month:02d}.jsonl'
            req = requests.head(full_url)

            # Found the latest file
            if req.status_code == 200:
                url = full_url
                break

            date = date.shift(months=-1)

        else:
            # for loop was not 'broken', no file available
            raise Exception('No recent CAIDA ix-asns file available')
        date = date.datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        logging.info('going to use this URL: ' + url)
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://publicdata.caida.org/datasets/ixps/README.txt'
        self.reference['reference_time_modification'] = date

    def __set_modification_time_from_metadata_line(self, line):
        try:
            date_str = json.loads(line.lstrip('#'))['date']
            date = datetime.strptime(date_str, '%Y.%m.%d %H:%M:%S').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.warning(f'Failed to get modification date from metadata line: {line.strip()}')
            logging.warning(e)
            logging.warning('Using date from filename.')

    def run(self):
        """Fetch the latest file and process lines one by one."""

        req = requests.get(self.url)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching CAIDA ix-asns file')

        lines = []
        asns = set()

        # Find all possible values and create corresponding nodes
        for line in req.text.splitlines():
            if line.startswith('#'):
                self.__set_modification_time_from_metadata_line(line)
                continue

            ix = json.loads(line)
            lines.append(ix)
            asns.add(int(ix.get('asn')))

        # get node IDs for ASNs, names, and countries
        ixp_id = self.iyp.batch_get_node_extid('CaidaIXID')
        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)

        # Compute links and add them to neo4j
        member_links = []

        for mem in lines:
            ixp_qid = ixp_id.get(mem['ix_id'])
            asn_qid = as_id.get(mem['asn'])
            flat_mem = dict(flatdict.FlatDict(mem))

            member_links.append({'src_id': asn_qid, 'dst_id': ixp_qid,
                                 'props': [self.reference, flat_mem]})
        # Push all links to IYP
        self.iyp.batch_add_links('MEMBER_OF', member_links)


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

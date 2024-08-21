import argparse
import bz2
import json
import logging
import os
import sys
from datetime import datetime, time, timedelta, timezone

import requests

from iyp import BaseCrawler, RequestStatusError

MAIN_PAGE = 'https://data.bgpkit.com/peer-stats/'
URL = 'https://data.bgpkit.com/peer-stats/{collector}/{year}/{month:02d}/peer-stats_{collector}_{year}-{month:02d}-{day:02d}_{epoch}.bz2'  # noqa: E501
ORG = 'BGPKIT'
NAME = 'bgpkit.peerstats'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://data.bgpkit.com/peer-stats/README.md'

    def run(self):
        """Fetch peer stats for each collector."""

        req = requests.get(MAIN_PAGE)
        if req.status_code != 200:
            logging.error(f'Cannot fetch peer-stats page {req.status_code}: req.text')
            raise RequestStatusError('Error while fetching main page')

        # Find all collectors
        collectors = []
        for line in req.text.splitlines():
            if line.strip().startswith('<span class="name">') and line.endswith('/</span>'):
                collectors.append(line.partition('>')[2].partition('/')[0])

        # Find latest date
        prev_day = datetime.combine(datetime.utcnow(), time.min, timezone.utc)
        self.now = None
        req = None
        trials = 0

        while (req is None or req.status_code != 200) and trials < 7:
            self.now = prev_day
            # Check if today's data is available
            url = URL.format(collector='rrc10', year=self.now.year,
                             month=self.now.month, day=self.now.day,
                             epoch=int(self.now.timestamp()))
            req = requests.head(url)

            prev_day -= timedelta(days=1)
            logging.warning("Today's data not yet available!")

        self.reference['reference_time_modification'] = self.now
        for collector in collectors:
            url = URL.format(collector=collector, year=self.now.year,
                             month=self.now.month, day=self.now.day,
                             epoch=int(self.now.timestamp()))

            req = requests.get(url, stream=True)
            if req.status_code != 200:
                logging.warning(f'Data not available for {collector}')
                continue

            # keep track of collector and reference url
            stats = json.load(bz2.open(req.raw))
            collector_qid = self.iyp.get_node(
                'BGPCollector',
                {'name': stats['collector'], 'project': stats['project']}
            )
            self.reference['reference_url_data'] = url

            asns = set()

            # Collect all ASNs and names
            for peer in stats['peers'].values():
                asns.add(peer['asn'])

            # get ASNs' IDs
            self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)

            # Compute links
            links = []
            for peer in stats['peers'].values():
                as_qid = self.asn_id[peer['asn']]
                links.append({'src_id': as_qid, 'dst_id': collector_qid,
                             'props': [self.reference, peer]})  # Set AS name

            # Push all links to IYP
            self.iyp.batch_add_links('PEERS_WITH', links)

    def unit_test(self):
        super().unit_test(['PEERS_WITH'])


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

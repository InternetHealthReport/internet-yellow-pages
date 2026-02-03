import argparse
import bz2
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

import requests

from iyp import BaseCrawler, DataNotAvailableError

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
        req.raise_for_status()

        # Find all collectors
        available_collectors = list()
        for line in req.text.splitlines():
            if line.strip().startswith('<span class="name">') and line.endswith('/</span>'):
                available_collectors.append(line.partition('>')[2].partition('/')[0])

        # Find latest available data.
        curr_date = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        max_lookback_days = 7
        for _ in range(max_lookback_days):
            url = URL.format(collector='rrc10', year=curr_date.year,
                             month=curr_date.month, day=curr_date.day,
                             epoch=int(curr_date.timestamp()))
            req = requests.head(url)
            if req.ok:
                break
            curr_date -= timedelta(days=1)
        else:
            raise DataNotAvailableError('No recent data available.')

        logging.info(f'Using date: {curr_date.strftime("%Y-%m-%d")}')
        self.reference['reference_time_modification'] = curr_date

        collectors = list()
        asns = set()
        peers_with = list()

        logging.info(f'Fetching data for {len(available_collectors)} collectors.')
        for collector in available_collectors:
            url = URL.format(collector=collector, year=curr_date.year,
                             month=curr_date.month, day=curr_date.day,
                             epoch=int(curr_date.timestamp()))

            req = requests.get(url, stream=True)
            if req.status_code != 200:
                logging.warning(f'Data not available for {collector}')
                continue

            stats = json.load(bz2.open(req.raw))
            # Name should be the same as in URL, but just in case use name from file.
            collector_name = stats['collector']
            collectors.append({
                'name': collector_name,
                'project': stats['project']
            })

            # Copy since data URL is different per collector.
            reference = dict(self.reference)
            reference['reference_url_data'] = url

            # Collect all ASNs and relationships.
            for peer in stats['peers'].values():
                peer_asn = peer['asn']
                asns.add(peer_asn)
                peers_with.append({
                    'src_id': peer_asn,
                    'dst_id': collector_name,
                    'props': [reference, peer]
                })

        # Get nodes.
        collector_id = self.iyp.batch_get_nodes('BGPCollector', collectors, id_properties=['name'])
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)

        # Replace QID in links.
        for link in peers_with:
            link['src_id'] = asn_id[link['src_id']]
            link['dst_id'] = collector_id[link['dst_id']]

        self.iyp.batch_add_links('PEERS_WITH', peers_with)

    def unit_test(self):
        return super().unit_test(['PEERS_WITH'])


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

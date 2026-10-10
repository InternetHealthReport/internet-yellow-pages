import argparse
import json
import logging
import sys
from datetime import UTC, datetime

import flatdict
import requests
from neo4j.spatial import WGS84Point

from iyp import BaseCrawler

# URL to ASRank API
URL = 'https://api.asrank.caida.org/v2/restful/asns/?first=10000'
ORG = 'CAIDA'
NAME = 'caida.asrank'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://asrank.caida.org/'

    def __set_modification_time(self):
        try:
            date = requests.get('https://api.asrank.caida.org/v2/restful/datasets').json()['data'][
                0
            ]['date']
            self.reference['reference_time_modification'] = datetime.strptime(
                date, '%Y-%m-%d'
            ).replace(tzinfo=UTC)
            logging.info(f'Dataset modification time: {date}')
        except Exception as e:
            logging.warning(f'Failed to set modification time: {e}')
            return

    def generic_link_generator(self, pairs, src_id: dict, dst_id: dict):
        for src, dst in pairs:
            yield {'src_id': src_id[src], 'dst_id': dst_id[dst], 'props': [self.reference]}

    def rank_link_generator(self, pairs: dict, src_id: dict):
        for (src, dst), props in pairs.items():
            yield {'src_id': src_id[src], 'dst_id': dst, 'props': [self.reference, props]}

    def run(self):
        """Fetch networks information from ASRank and push to IYP."""
        nodes = list()

        has_next = True
        i = 0
        logging.info('Fetching AS Ranks...')
        while has_next:
            url = URL + f'&offset={i * 10000}'
            i += 1
            logging.debug(f'Fetching {url}')
            req = requests.get(url)
            req.raise_for_status()

            ranking = json.loads(req.text)['data']['asns']
            has_next = ranking['pageInfo']['hasNextPage']

            nodes += ranking['edges']

        logging.info(f'Fetched {len(nodes):,d} ranks.')
        self.__set_modification_time()

        # Collect all ASNs, names, and countries
        asrank_qid = self.iyp.get_node('Ranking', {'name': 'CAIDA ASRank'})
        asns = set()
        names = set()
        countries = set()
        points = set()

        country_links = set()
        located_in_links = set()
        name_links = set()
        rank_links = dict()
        for node in nodes:
            as_node = node['node']
            asn = int(as_node['asn'])
            asns.add(asn)
            rank_links[(asn, asrank_qid)] = dict(flatdict.FlatDict(as_node))
            if as_node['asnName']:
                names.add(as_node['asnName'])
                name_links.add((asn, as_node['asnName']))
            country_code = as_node['country']['iso']
            if country_code:
                countries.add(country_code)
                country_links.add((asn, country_code))
            if as_node['latitude'] and as_node['longitude']:
                lat = as_node['latitude']
                long = as_node['longitude']
                if long < -180 or long > 180 or lat < -90 or lat > 90:
                    logging.warning(f'Ignoring invalid geo coordinates of AS: {as_node}')
                else:
                    point = WGS84Point((as_node['longitude'], as_node['latitude']))
                    points.add(point)
                    located_in_links.add((asn, point))

        # Get/create ASNs, names, and country nodes
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)
        name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names, all=False)
        point_id = self.iyp.batch_get_nodes_by_single_prop('Point', 'position', points)

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', self.generic_link_generator(name_links, asn_id, name_id))
        self.iyp.batch_add_links(
            'COUNTRY', self.generic_link_generator(country_links, asn_id, country_id)
        )
        self.iyp.batch_add_links('RANK', self.rank_link_generator(rank_links, asn_id))
        self.iyp.batch_add_links(
            'LOCATED_IN', self.generic_link_generator(located_in_links, asn_id, point_id)
        )

    def unit_test(self):
        return super().unit_test(['NAME', 'COUNTRY', 'RANK', 'LOCATED_IN'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
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

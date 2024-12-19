import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import flatdict
import requests

from iyp import BaseCrawler, RequestStatusError

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
            date = requests.get('https://api.asrank.caida.org/v2/restful/datasets').json()['data'][0]['date']
            self.reference['reference_time_modification'] = datetime.strptime(date,
                                                                              '%Y-%m-%d').replace(tzinfo=timezone.utc)
            logging.info(f'Dataset modification time: {date}')
        except Exception as e:
            logging.warning(f'Failed to set modification time: {e}')
            return

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
            if req.status_code != 200:
                logging.error(f'Request failed with status: {req.status_code}')
                raise RequestStatusError('Error while fetching data from API')

            ranking = json.loads(req.text)['data']['asns']
            has_next = ranking['pageInfo']['hasNextPage']

            nodes += ranking['edges']

        logging.info(f'Fetched {len(nodes):,d} ranks.')
        self.__set_modification_time()

        # Collect all ASNs, names, and countries
        asns = set()
        names = set()
        countries = set()
        for node in nodes:
            asn = node['node']
            if asn['asnName']:
                names.add(asn['asnName'])
            country_code = asn['country']['iso']
            if country_code:
                countries.add(country_code)
            asns.add(int(asn['asn']))

        # Get/create ASNs, names, and country nodes
        self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        self.country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)
        self.name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names, all=False)
        self.asrank_qid = self.iyp.get_node('Ranking', {'name': 'CAIDA ASRank'})

        # Compute links
        country_links = list()
        name_links = list()
        rank_links = list()

        for node in nodes:
            asn = node['node']

            asn_qid = self.asn_id[int(asn['asn'])]

            # Some ASes do not have a country.
            country_code = asn['country']['iso']
            if country_code:
                country_qid = self.country_id[country_code]
                country_links.append({'src_id': asn_qid, 'dst_id': country_qid, 'props': [self.reference]})

            # Some ASes do not have a name.
            name = asn['asnName']
            if name:
                name_qid = self.name_id[name]
                name_links.append({'src_id': asn_qid, 'dst_id': name_qid, 'props': [self.reference]})

            # flatten all attributes into one dictionary
            flat_asn = dict(flatdict.FlatDict(asn))

            rank_links.append({'src_id': asn_qid, 'dst_id': self.asrank_qid, 'props': [self.reference, flat_asn]})

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('RANK', rank_links)

    def unit_test(self):
        return super().unit_test(['NAME', 'COUNTRY', 'RANK'])


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

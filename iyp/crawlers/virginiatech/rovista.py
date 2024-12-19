import argparse
import logging
import sys
from datetime import datetime, timezone

import requests

from iyp import BaseCrawler, RequestStatusError

URL = 'https://api.rovista.netsecurelab.org/rovista/api/overview'
ORG = 'Virginia Tech'
NAME = 'virginiatech.rovista'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://rovista.netsecurelab.org/'

    def __set_modification_time(self, entry):
        try:
            date_str = entry['lastUpdatedDate']
            date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except (KeyError, ValueError) as e:
            logging.warning(f'Failed to set modification time: {e}')

    def run(self):
        """Get RoVista data from their API."""
        batch_size = 1000
        offset = 0
        entries = []
        asns = set()

        while True:
            # Make a request with the current offset
            response = requests.get(URL, params={'offset': offset, 'count': batch_size})
            if response.status_code != 200:
                raise RequestStatusError(f'Error while fetching RoVista data: {response.status_code}')

            data = response.json().get('data', [])
            for entry in data:
                if not self.reference['reference_time_modification']:
                    self.__set_modification_time(entry)
                asns.add(entry['asn'])
                if entry['ratio'] > 0.5:
                    entries.append({'asn': entry['asn'], 'ratio': entry['ratio']})
                else:
                    entries.append({'asn': entry['asn'], 'ratio': entry['ratio']})

            # Move to the next page
            offset += 1
            # Break the loop if there's no more data
            if len(data) < batch_size:
                break

        # get ASNs and prefixes IDs
        self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        tag_id_not_valid = self.iyp.get_node('Tag', {'label': 'Not Validating RPKI ROV'})
        tag_id_valid = self.iyp.get_node('Tag', {'label': 'Validating RPKI ROV'})
        # Compute links
        links = []
        for entry in entries:
            asn_qid = self.asn_id[entry['asn']]
            if entry['ratio'] > 0.5:
                links.append({'src_id': asn_qid, 'dst_id': tag_id_valid,
                              'props': [self.reference, {'ratio': entry['ratio']}]})
            else:
                links.append({'src_id': asn_qid, 'dst_id': tag_id_not_valid,
                             'props': [self.reference, {'ratio': entry['ratio']}]})

        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED'])


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

import argparse
import logging
import os
import sys

import requests

from iyp import BaseCrawler, RequestStatusError

URL = 'https://api.rovista.netsecurelab.org/rovista/api/overview'
ORG = 'RoVista'
NAME = 'rovista.validating_rov'


class Crawler(BaseCrawler):

    def run(self):
        """Get RoVista data from their API."""
        batch_size = 1000  # Adjust batch size as needed
        offset = 0
        entries = []
        asns = set()

        while True:
            # Make a request with the current offset
            response = requests.get(URL, params={'offset': offset, 'count': batch_size})
            if response.status_code != 200:
                raise RequestStatusError('Error while fetching RoVista data')

            data = response.json().get('data', [])
            for entry in data:
                asns.add(entry['asn'])
                if entry['ratio'] > 0.5:
                    entries.append({'asn': entry['asn'], 'ratio': entry['ratio'], 'label': 'Validating RPKI ROV'})
                else:
                    entries.append({'asn': entry['asn'], 'ratio': entry['ratio'], 'label': 'Not Validating RPKI ROV'})

            # Move to the next page
            offset += 1
            # Break the loop if there's no more data
            if len(data) < batch_size:
                break
        logging.info('Pushing nodes to neo4j...')
        # get ASNs and prefixes IDs
        self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        tag_id_not_valid = self.iyp.get_node('Tag', {'label': 'Not Validating RPKI ROV'}, create=True)
        tag_id_valid = self.iyp.get_node('Tag', {'label': 'Validating RPKI ROV'}, create=True)
        # Compute links
        links = []
        for entry in entries:
            asn_qid = self.asn_id[entry['asn']]
            if entry['ratio'] > 0.5:
                links.append({'src_id': asn_qid, 'dst_id': tag_id_valid, 'props': [
                             self.reference, {'ratio': entry['ratio']}]})
            else:
                links.append({'src_id': asn_qid, 'dst_id': tag_id_not_valid,
                             'props': [self.reference, {'ratio': entry['ratio']}]})

        logging.info('Pushing links to neo4j...')
        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)


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

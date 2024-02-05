import argparse
import logging
import os
import sys

import requests

from iyp import BaseCrawler, RequestStatusError

URL = 'https://api.rovista.netsecurelab.org/rovista/api/overview'
ORG = 'ROV'
NAME = 'rov.rovista'


class Crawler(BaseCrawler):

    def run(self):
        """Fetch the prefix to ASN file from BGPKIT website and process lines one by
        one."""
        
        batch_size = 1000  # Adjust batch size as needed
        offset = 0
        while True:
            # Make a request with the current offset
            response = requests.get(URL, params={"offset": offset, "count": batch_size})
            if response.status_code != 200:
                raise RequestStatusError('Error while fetching RoVista data')
            data = response.json().get('data', [])
            for entry in data:
                asn = entry['asn']
                ratio = entry['ratio']
                if ratio > 0.5:
                    self.iyp.add_relationship_properties(node_label_properties=f"AS{{asn: {asn}}}",relationship="CATEGORIZED",connected_node_label_properties='Tag{label:"Validating RPKI ROV"}',properties={'ratio':ratio})
                else:
                    self.iyp.add_relationship_properties(node_label_properties=f"AS{{asn: {asn}}}",relationship="CATEGORIZED",connected_node_label_properties='Tag{label:"Not Validating RPKI ROV"}',properties={'ratio':ratio})
            # Move to the next page
            offset += batch_size
            # Break the loop if there's no more data
            if len(data) < batch_size:
                break


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

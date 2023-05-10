import argparse
import bz2
import json
import logging
import os
import requests
import sys

from iyp import BaseCrawler

URL = 'https://data.bgpkit.com/as2rel/as2rel-latest.json.bz2'
ORG = 'BGPKIT'
NAME = 'bgpkit.as2rel'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch the AS relationship file from BGPKIT website and process lines one by one"""

        req = requests.get(URL, stream=True)
        if req.status_code != 200:
            sys.exit('Error while fetching AS relationships')

        rels = []
        asns = set()

        # Collect all ASNs 
        for rel in json.load(bz2.open(req.raw)):
            asns.add(rel['asn1'])
            asns.add(rel['asn2'])
            rels.append(rel)

        # get ASNs IDs
        self.asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)

        # Compute links
        links = []
        for rel in rels:
            as1_qid = self.asn_id[rel['asn1']]
            as2_qid = self.asn_id[rel['asn2']]

            links.append( { 'src_id':as1_qid, 'dst_id':as2_qid, 'props':[self.reference, rel] } ) # Set AS name

        # Push all links to IYP
        self.iyp.batch_add_links('PEERS_WITH', links)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/'+scriptname+'.log',
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


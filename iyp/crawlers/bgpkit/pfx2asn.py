import sys
import logging
import requests
from iyp import BaseCrawler
import bz2
import json

URL = 'https://data.bgpkit.com/pfx2as/pfx2as-latest.json.bz2'
ORG = 'BGPKIT'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch the prefix to ASN file from BGPKIT website and process lines one by one"""

        req = requests.get(URL, stream=True)
        if req.status_code != 200:
            sys.exit('Error while fetching pfx2as relationships')

        entries = []
        asns = set()
        prefixes = set()

        for entry in json.load(bz2.open(req.raw)):
            prefixes.add(entry['prefix'])
            asns.add(entry['asn'])
            entries.append(entry)

        req.close()

        logging.warning('Pushing nodes to neo4j...\n')
        # get ASNs and prefixes IDs
        self.asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
        self.prefix_id = self.iyp.batch_get_nodes('PREFIX', 'prefix', prefixes)

        # Compute links
        links = []
        for entry in entries:
            asn_qid = self.asn_id[entry['asn']]
            prefix_qid = self.prefix_id[entry['prefix']]

            links.append( { 'src_id':asn_qid, 'dst_id':prefix_qid, 'props':[self.reference, entry] } ) # Set AS name

        logging.warning('Pushing links to neo4j...\n')
        # Push all links to IYP
        self.iyp.batch_add_links('ORIGINATE', links)


if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Start: %s" % sys.argv)

    asnames = Crawler(ORG, URL)
    asnames.run()
    asnames.close()

    logging.info("End: %s" % sys.argv)

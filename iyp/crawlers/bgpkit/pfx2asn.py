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

        for i, _ in enumerate(map(self.update_asn, json.load(bz2.open(req.raw)))):
            sys.stderr.write(f'\rProcessed {i} relationships')

            # commit every 1k lines
            if i % 1000 == 0:
                self.iyp.commit()

        sys.stderr.write('\n')

    def update_asn(self, entry):
        af = 6
        if '.' in entry['prefix']:
            af =4
        prefix_qid = self.iyp.get_node(
                                        'PREFIX', 
                                        {'prefix': entry['prefix'], 'af': af}, 
                                        create=True
                                       )
        asn_qid = self.iyp.get_node('AS', {'asn': entry['asn']}, create=True)

        statements = []
        statements.append( [
                'ORIGINATE', 
                prefix_qid, 
                dict({'nb_observers': entry['count']}, **self.reference)
            ] )  # Set relationship

        try:
            # Add link between AS and prefix
            self.iyp.add_links(asn_qid, statements)

        except Exception as error:
            # print errors and continue running
            print('Error for: ', entry)
            print(error)

        return asn_qid, prefix_qid

if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    asnames = Crawler(ORG, URL)
    asnames.run()
    asnames.close()


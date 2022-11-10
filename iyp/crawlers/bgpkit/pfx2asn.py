import sys
import logging
import requests
from datetime import datetime, time
from iyp import IYP
import bz2
import json

URL = 'https://data.bgpkit.com/pfx2as/pfx2as-latest.json.bz2'

class Crawler(object):
    def __init__(self):

        # Reference information for data pushed to IYP
        self.reference = {
            'source': 'BGPKIT',
            'reference_url': URL,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # connection to IYP database
        self.iyp = IYP()


    def run(self):
        """Fetch the prefix to ASN file from BGPKIT website and process lines one by one"""

        req = requests.get(URL, stream=True)
        if req.status_code != 200:
            sys.exit('Error while fetching pfx2as relationships')

        for i, _ in enumerate(map(self.update_asn, json.load(bz2.open(req.raw)))):
            sys.stderr.write(f'\rProcessed {i} relationships')

        sys.stderr.write('\n')
        self.iyp.close()

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

    asnames = Crawler()
    asnames.run()


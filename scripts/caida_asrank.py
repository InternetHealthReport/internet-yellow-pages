import sys
import logging
import requests
import json
from concurrent.futures import ThreadPoolExecutor
import wikihandy

# URL to ASRank API
URL_API = 'https://api.asrank.caida.org/v2/restful/asns/'

class ASRank(object):
    def __init__(self):
        """Initialize wikihandy and qualifiers for pushed data"""
    
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        # Added properties will have this additional information
        today = self.wh.today()
        self.caida_qid = self.wh.get_qid('CAIDA')

        # Get the QID for ASRank project
        self.asrank_qid = self.wh.get_qid('CAIDA ASRank',
            create={                                    # Create it if it doesn't exist
                'summary': 'add CAIDA ASRank',         # Commit message
                'description': "CAIDA's AS ranking derived from topological data collected by CAIDA's Archipelago Measurement Infrastructure and BGP routing data collected by the Route Views Project and RIPE NCC.", # Item description
                'statements': [[self.wh.get_pid('managed by'), self.caida_qid]]
                })

        self.reference = [
                (self.wh.get_pid('source'), self.caida_qid),
                (self.wh.get_pid('reference URL'), URL_API),
                (self.wh.get_pid('point in time'), today)
                ]

    def run(self):
        """Fetch networks information from ASRank and push to wikibase. """

        self.wh.login() # Login once for all threads
        pool = ThreadPoolExecutor()
        has_next = True
        i = 0
        while has_next:
            req = requests.get(URL_API+f'?offset={i}')
            if req.status_code != 200:
                sys.exit('Error while fetching data from API')
            
            ranking = json.loads(req.text)['data']['asns']
            has_next = ranking['pageInfo']['hasNextPage']

            for res in pool.map(self.update_net, ranking['edges']):
                sys.stderr.write(f'\rProcessing... {i+1}/{ranking["totalCount"]}')
                i+=1

        pool.shutdown()

    def update_net(self, asn):
        """Add the network to wikibase if it's not already there and update its
        properties."""
        
        asn = asn['node']

        # Properties
        statements = []

        if asn['asnName']:
                statements.append([self.wh.get_pid('name'), asn['asnName'], self.reference])

        # set countries
        cc = asn['country']['iso']
        if cc:
            statements.append([ self.wh.get_pid('country'), self.wh.country2qid(cc), self.reference])

        # set rank
        statements.append([ self.wh.get_pid('ranking'), {
            'amount': asn['rank'], 
            'unit': self.asrank_qid,
            },
            self.reference])

        # Commit to wikibase
        # Get the AS QID (create if AS is not yet registered) and commit changes
        net_qid = self.wh.asn2qid(asn['asn'], create=True) 
        self.wh.upsert_statements('update from CAIDA ASRank', net_qid, statements )
        
# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].rpartition('/')[2][0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    asrank = ASRank()
    asrank.run()

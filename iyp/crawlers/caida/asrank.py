import sys
import logging
import requests
import json
from datetime import datetime, time
from iyp import IYP

# URL to ASRank API
URL = 'https://api.asrank.caida.org/v2/restful/asns/'
ORG = 'CAIDA'

class Crawler(object):
    def __init__(self):
        """Initialize iyp connection and qualifiers for pushed data"""
    
        self.reference = {
            'source': ORG,
            'reference_url': URL,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # connection to IYP database
        self.iyp = IYP()

    def run(self):
        """Fetch networks information from ASRank and push to IYP. """

        self.asrank_qid = self.iyp.get_node('RANKING', {'name': f'CAIDA ASRank'}, create=True)

        has_next = True
        i = 0
        while has_next:
            req = requests.get(URL+f'?offset={i}')
            if req.status_code != 200:
                sys.exit('Error while fetching data from API')
            
            ranking = json.loads(req.text)['data']['asns']
            has_next = ranking['pageInfo']['hasNextPage']

            for _ in map(self.update_net, ranking['edges']):
                sys.stderr.write(f'\rProcessing... {i+1}/{ranking["totalCount"]}')
                i+=1

        self.iyp.close()


    def update_net(self, asn):
        """Add the network to iyp if it's not already there and update its
        properties."""
        
        asn = asn['node']

        # Properties
        statements = []

        if asn['asnName']:
            name_qid = self.iyp.get_node('NAME', {'name': asn['asnName']}, create=True) 
            statements.append([ 'NAME', name_qid, self.reference])

        # set countries
        cc = asn['country']['iso']
        if cc:
            cc_qid = self.iyp.get_node('COUNTRY', {'country_code': cc}, create=True)
            statements.append([ 'COUNTRY', cc_qid, self.reference])

        # set rank
        statements.append([ 'RANK', 
            self.asrank_qid, 
            dict({'rank': asn['rank']}, **self.reference), 
            ])

        # Commit to iyp
        # Get the AS (create if AS is not yet registered) and commit changes
        asn_qid = self.iyp.get_node('AS', {'asn': asn['asn']}, create=True) 
        self.iyp.add_links( asn_qid, statements )
        
# Main program
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

    asrank = Crawler()
    asrank.run()

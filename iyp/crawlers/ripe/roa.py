import sys
import logging
from collections import defaultdict
from datetime import datetime, timedelta
import requests
from iyp import BaseCrawler

# URL to RIPE repository
URL = 'https://ftp.ripe.net/rpki/'
ORG = 'RIPE NCC'
NAME = 'ripe.roa'

TALS = ['afrinic.tal', 'apnic.tal', 'arin.tal', 'lacnic.tal', 'ripencc.tal']

class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialize IYP and statements for pushed data"""

        now = datetime.utcnow()
        self.date_path = f'{now.year}/{now.month:02d}/{now.day:02d}'

        # Check if today's data is available
        self.url = f'{URL}/afrinic.tal/{self.date_path}/roas.csv'
        req = requests.head( self.url )
        if req.status_code != 200:
            now -= timedelta(days=1)
            self.date_path = f'{now.year}/{now.month:02d}/{now.day:02d}'
            logging.warning("Today's data not yet available!")
            logging.warning("Using yesterday's data: "+self.date_path)

        super().__init__(organization, url, name)

    def run(self):
        """Fetch data from RIPE and push to IYP. """

        for tal in TALS:

            self.url = f'{URL}/{tal}/{self.date_path}/roas.csv'
            logging.info(f'Fetching ROA file: {self.url}')
            req = requests.get( self.url )
            if req.status_code != 200:
                sys.exit('Error while fetching data for '+self.url)
            
            # Aggregate data per prefix
            asns = set()

            prefix_info = defaultdict(list)
            for line in req.text.splitlines():
                url, asn, prefix, max_length, start, end = line.split(',')
                
                # Skip header
                if url=='URI':
                    continue

                asn = int(asn.replace('AS', ''))
                asns.add( asn )
                prefix_info[prefix].append({
                    'url': url, 
                    'asn': asn, 
                    'max_length': max_length, 
                    'start': start, 
                    'end': end})

            # get ASNs and prefixes IDs
            asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
            prefix_id = self.iyp.batch_get_nodes('PREFIX', 'prefix', set(prefix_info.keys()))

            links = []
            for prefix, attributes in prefix_info.items():
                for att in attributes:
                
                    vrp = {
                            'notBefore': att['start'],
                            'notAfter': att['end'],
                            'uri': att['url'],
                            'maxLength': att['max_length']
                        }
                    asn_qid = asn_id[ att['asn'] ]
                    prefix_qid = prefix_id[ prefix ]
                    links.append( { 'src_id':asn_qid, 'dst_id':prefix_qid, 
                                   'props':[self.reference, vrp] } ) # Set AS name

            # Push all links to IYP
            self.iyp.batch_add_links('ROUTE_ORIGIN_AUTHORIZATION', links)

        
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
    logging.info("Start: %s" % sys.argv)

    crawler = Crawler(ORG, URL, NAME)
    crawler.run()
    crawler.close()

    logging.info("End: %s" % sys.argv)

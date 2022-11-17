import sys
import logging
import requests
from zipfile import ZipFile
import io
from datetime import datetime, time
from iyp import IYP

# URL to Tranco top 1M
URL = 'https://tranco-list.eu/top-1m.csv.zip'
ORG = 'imec-DistriNet'

class Crawler(object):
    def __init__(self):
        """IYP and references initialization"""

        self.reference = {
            'source': ORG,
            'reference_url': URL,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # connection to IYP database
        self.iyp = IYP()
    
    def run(self):
        """Fetch Tranco top 1M and push to IYP. """

        self.tranco_qid = self.iyp.get_node('RANKING', {'name': f'Tranco top 1M'}, create=True)

        sys.stderr.write('Downloading latest list...\n')
        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching Tranco csv file')

        # open zip file and read top list
        with  ZipFile(io.BytesIO(req.content)) as z:
            with z.open('top-1m.csv') as list:
                for i, row in enumerate(io.TextIOWrapper(list)):
                    row = row.rstrip()
                    sys.stderr.write(f'\rProcessed {i} domains \t {row}')
                    self.update(row)

        self.iyp.close()


    def update(self, one_line):
        """Add the network to wikibase if it's not already there and update its
        properties."""

        rank, domain = one_line.split(',')

        # set rank
        statements = [[ 'RANK', self.tranco_qid, self.reference ]]

        # Commit to IYP
        # Get the domain name QID (create if it is not yet registered) and commit changes
        domain_qid = self.iyp.get_node('DOMAIN_NAME', {'name': domain}, create=True) 
        self.iyp.add_links( domain_qid, statements )
        
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

    crawler = Crawler()
    crawler.run()

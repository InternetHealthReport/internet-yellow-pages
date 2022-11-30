import sys
import logging
import arrow
import requests
import lz4.frame
from datetime import datetime, time, timezone
from iyp import BaseCrawler

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/hegemony/ipv4/local/{year}/{month:02d}/{day:02d}/ihr_hegemony_ipv4_local_{year}-{month:02d}-{day:02d}.csv.lz4'
ORG = 'Internet Health Report'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch data from file and push to IYP. """

        today = arrow.utcnow()
        url = URL.format(year=today.year, month=today.month, day=today.day)
        req = requests.head(url)
        if req.status_code != 200:
            today = today.shift(days=-1)
            url = URL.format(year=today.year, month=today.month, day=today.day)
            req = requests.head(url)

        self.reference = {
            'reference_url': url,
            'reference_time': today
        }

        req = requests.get(url, stream=True)
        if req.status_code != 200:
            sys.exit('Error while fetching data '+url)
        
        
        with lz4.frame.open(req.raw, 'r') as fp:
            # header
            # timebin,originasn,asn,hege
            line = fp.readline()
            self.fields = line.decode('utf-8').rstrip().split(',')

            # first line of data
            line = fp.readline()
            i = 0

            while len(line) > 0:
                self.update(line)
                sys.stderr.write(f'\rProcessed {i+1} lines...')
                i+=1
                line = fp.readline()

    def update(self, line):
        """Add the AS to iyp if it's not already there and update its
        properties."""
        
        rec = dict( zip(self.fields, line.decode('utf-8').rstrip().split(',')) )

        asn_qid = self.iyp.get_node('AS', {'asn': rec['asn_id']}, create=True)

        # Properties
        statements = []

        # set dependency
        statements.append( [ 'DEPENDS_ON', asn_qid, dict({'hegemony': rec['hege']}, *self.reference) ])

        # Commit to IYP
        # Get the AS node ID (create if AS is not yet registered) and commit changes
        originasn_qid = self.iyp.get_node('AS', {'asn': rec['originasn_id']}, create=True)
        self.iyp.add_links( originasn_qid, statements )
        
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

    crawler = Crawler(ORG, URL)
    crawler.run()
    crawler.close()


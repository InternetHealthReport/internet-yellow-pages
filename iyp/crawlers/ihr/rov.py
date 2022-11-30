import sys
import logging
import arrow
import requests
import lz4.frame
from datetime import datetime, time, timezone
from iyp import BaseCrawler

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/rov/{year}/{month:02d}/{day:02d}/ihr_rov_{year}-{month:02d}-{day:02d}.csv.lz4'
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
            if req.status_code != 200:
                today = today.shift(days=-1)
                url = URL.format(year=today.year, month=today.month, day=today.day)

        self.reference = {
            'reference_url': url,
        }

        req = requests.get(url, stream=True)
        if req.status_code != 200:
            sys.exit('Error while fetching data '+url)
        
        
        with lz4.frame.open(req.raw, 'r') as fp:
            # header
            # id,timebin,prefix,hege,af,visibility,rpki_status,irr_status, delegated_prefix_status,
            #delegated_asn_status,descr,moas,asn_id,country_id,originasn_id
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
        """Add the prefix to iyp if it's not already there and update its
        properties."""
        
        rec = dict( zip(self.fields, line.decode('utf-8').rstrip().split(',')) )

        rpki_status = self.iyp.get_node('TAG', {'label': 'RPKI '+rec['rpki_status']}, create=True)
        irr_status = self.iyp.get_node('TAG', {'label': 'IRR '+rec['irr_status']}, create=True)
        asn_qid = self.iyp.get_node('AS', {'asn': rec['asn_id']}, create=True)
        originasn_qid = self.iyp.get_node('AS', {'asn': rec['originasn_id']}, create=True)
        country_qid = self.iyp.get_node('COUNTRY', {'country_code': rec['country_id']}, create=True)

        # Properties
        statements = []

        # set rank
        statements.append( [ 'CLASSIFIED', rpki_status, self.reference ])
        statements.append( [ 'CLASSIFIED', irr_status, self.reference ])
        statements.append( [ 'DEPENDS_ON', asn_qid, dict({'hegemony': rec['hege']}, *self.reference) ])
        statements.append( [ 'ORIGINATE', originasn_qid, self.reference ])
        statements.append( [ 'COUNTRY', country_qid, self.reference ])

        # Commit to IYP
        # Get the AS QID (create if AS is not yet registered) and commit changes
        prefix_qid = self.iyp.get_node('PREFIX', 
            {'prefix': rec['prefix'], 'af': rec['af'], 'description': rec['descr']}, create=True)
        self.iyp.add_links( prefix_qid, statements )
        
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

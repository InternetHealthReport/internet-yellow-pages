import sys
import os
import logging
import arrow
import requests
import lz4.frame
import csv
from datetime import datetime, time, timezone
from iyp import BaseCrawler

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/rov/{year}/{month:02d}/{day:02d}/ihr_rov_{year}-{month:02d}-{day:02d}.csv.lz4'
ORG = 'Internet Health Report'


class lz4Csv:
    def __init__(self, filename):
        """Start reading a lz4 compress csv file """
    
        self.fp = lz4.frame.open(filename)

    def __iter__(self):
        """Read file header line and set self.fields"""
        line = self.fp.readline()
        self.fields = line.decode('utf-8').rstrip().split(',')
        return self

    def __next__(self):
        line = self.fp.readline().decode('utf-8').rstrip()
    
        if len(line) > 0:
            return line
        else:
            raise StopIteration

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
            'reference_org': ORG,
            'reference_url': url,
            'reference_time': datetime.combine(today.date(), time.min, timezone.utc)
        }

        os.makedirs('tmp/', exist_ok=True)
        os.system(f'wget {url} -P tmp/')
        
        local_filename = 'tmp/'+url.rpartition('/')[2]
        self.csv = lz4Csv(local_filename)

        for i, line in  enumerate(csv.reader(self.csv, quotechar='"', delimiter=',', skipinitialspace=True) ):
            # header
            # id,timebin,prefix,hege,af,visibility,rpki_status,irr_status, delegated_prefix_status,
            #delegated_asn_status,descr,moas,asn_id,country_id,originasn_id

            self.update(line)
            sys.stderr.write(f'\rProcessed {i+1} lines...')

            # commit every 10k lines
            if i % 10000 == 0:
                self.iyp.commit()

        # Remove downloaded file
        os.remove(local_filename)


    def update(self, line):
        """Add the prefix to iyp if it's not already there and update its
        properties."""
        
        rec = dict( zip(self.csv.fields, line) )

        rpki_status = self.iyp.get_node('TAG', {'label': 'RPKI '+rec['rpki_status']}, create=True)
        irr_status = self.iyp.get_node('TAG', {'label': 'IRR '+rec['irr_status']}, create=True)
        asn_qid = self.iyp.get_node('AS', {'asn': rec['asn_id']}, create=True)
        originasn_qid = self.iyp.get_node('AS', {'asn': rec['originasn_id']}, create=True)
        country_qid = self.iyp.get_node('COUNTRY', {'country_code': rec['country_id']}, create=True)

        # Properties
        statements = []

        # set links
        statements.append( [ 'CLASSIFIED', rpki_status, self.reference ])
        statements.append( [ 'CLASSIFIED', irr_status, self.reference ])
        statements.append( [ 'DEPENDS_ON', asn_qid, dict({'hegemony': rec['hege']}, **self.reference) ])
        # TODO fix link direction?
        statements.append( [ 'ORIGINATE', originasn_qid, self.reference ])
        statements.append( [ 'COUNTRY', country_qid, self.reference ])

        # Commit to IYP
        # Get the prefix node ID (create if AS is not yet registered) and commit changes
        prefix_qid = self.iyp.get_node('PREFIX', 
            {'prefix': rec['prefix'], 'af': int(rec['af'])}, create=True)
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

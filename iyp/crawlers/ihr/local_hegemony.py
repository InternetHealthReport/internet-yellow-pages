import sys
import logging
import arrow
import requests
import lz4.frame
from datetime import datetime, time, timezone
import csv
from iyp import BaseCrawler
import os

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/hegemony/ipv4/local/{year}/{month:02d}/{day:02d}/ihr_hegemony_ipv4_local_{year}-{month:02d}-{day:02d}.csv.lz4'
ORG = 'Internet Health Report'

class lz4Csv:
    def __init__(self, filename):
        """Start reading a lz4 compress csv file """
    
        self.fp = lz4.frame.open(filename, 'rb')

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
                req = requests.head(url)


        self.reference = {
            'reference_url': url,
            'reference_org': ORG,
            'reference_time': datetime.combine(today.date(), time.min, timezone.utc)
        }

        os.makedirs('tmp/', exist_ok=True)
        os.system(f'wget {url} -P tmp/')
        
        local_filename = 'tmp/'+url.rpartition('/')[2]
        self.csv = lz4Csv(local_filename)

        self.timebin = None
        
        for i, line in  enumerate(csv.reader(self.csv, quotechar='"', delimiter=',', skipinitialspace=True) ):
            # header
            # timebin,originasn,asn,hege

            if not self.update(line):
                break

            sys.stderr.write(f'\rProcessed {i+1} lines...')

            # commit every 1k lines
            if i % 1000 == 0:
                self.iyp.commit()

        # Remove downloaded file
        os.remove(local_filename)


    def update(self, line):
        """Add the AS to iyp if it's not already there and update its
        properties.

        Return false if line is not for the first timebin
        """
        
        rec = dict( zip(self.csv.fields, line) )

        if self.timebin is None:
            self.timebin = rec['timebin']
        elif self.timebin != rec['timebin']:
            return False

        asn_qid = self.iyp.get_node('AS', {'asn': rec['asn']}, create=True)

        # Properties
        statements = []

        # set dependency
        statements.append( [ 'DEPENDS_ON', asn_qid, dict({'hegemony': rec['hege']}, **self.reference) ])

        # Commit to IYP
        # Get the AS node ID (create if AS is not yet registered) and commit changes
        originasn_qid = self.iyp.get_node('AS', {'asn': rec['originasn']}, create=True)
        self.iyp.add_links( originasn_qid, statements )

        return True
        
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


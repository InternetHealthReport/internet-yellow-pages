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
ORG = 'IHR'
NAME = 'ihr.local_hegemony'

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
            'reference_name': NAME,
            'reference_time': datetime.combine(today.date(), time.min, timezone.utc)
        }

        os.makedirs('tmp/', exist_ok=True)
        os.system(f'wget {url} -P tmp/')
        
        local_filename = 'tmp/'+url.rpartition('/')[2]
        self.csv = lz4Csv(local_filename)

        self.timebin = None
        asn_id = self.iyp.batch_get_nodes('AS', 'asn', set())
        
        links = []

        for line in  csv.reader(self.csv, quotechar='"', delimiter=',', skipinitialspace=True):
            # header
            # timebin,originasn,asn,hege

            rec = dict( zip(self.csv.fields, line) )
            rec['hege'] = float(rec['hege'])

            if self.timebin is None:
                self.timebin = rec['timebin']
            elif self.timebin != rec['timebin']:
                break

            originasn = int(rec['originasn'])
            if originasn not in asn_id:
                asn_id[originasn] = self.iyp.get_node('AS', {'asn': originasn}, create=True)
            
            asn = int(rec['asn'])
            if asn not in asn_id:
                asn_id[asn] = self.iyp.get_node('AS', {'asn': asn}, create=True)

            links.append( {
                'src_id': asn_id[originasn], 
                'dst_id': asn_id[asn], 
                'props':[self.reference, rec]
                } )

        # Push links to IYP
        self.iyp.batch_add_links('DEPENDS_ON', links)

        # Remove downloaded file
        os.remove(local_filename)

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
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()

    logging.info("End: %s" % sys.argv)

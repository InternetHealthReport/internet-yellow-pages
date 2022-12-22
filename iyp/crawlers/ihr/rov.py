import sys
import os
import logging
import arrow
import requests
import lz4.frame
import csv
from datetime import datetime, time, timezone
from iyp import BaseCrawler

# NOTE: Assumes ASNs and Prefixes are already registered in the database. Run
# bgpkit.pfx2asn before this one

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/rov/{year}/{month:02d}/{day:02d}/ihr_rov_{year}-{month:02d}-{day:02d}.csv.lz4'
ORG = 'IHR'
NAME = 'ihr.rov'

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

    def close(self):
        self.fp.close()

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
            'reference_name': NAME,
            'reference_time': datetime.combine(today.date(), time.min, timezone.utc)
        }

        os.makedirs('tmp/', exist_ok=True)
        os.system(f'wget {url} -P tmp/')
        
        local_filename = 'tmp/'+url.rpartition('/')[2]
        self.csv = lz4Csv(local_filename)

        logging.warning('Getting node IDs from neo4j...\n')
        asn_id = self.iyp.batch_get_nodes('AS', 'asn')
        prefix_id = self.iyp.batch_get_nodes('PREFIX', 'prefix')
        tag_id = self.iyp.batch_get_nodes('TAG', 'label')
        country_id = self.iyp.batch_get_nodes('COUNTRY', 'country_code')

        orig_links = []
        tag_links = []
        dep_links = []
        country_links = []

        logging.warning('Computing links...\n')
        for line in  csv.reader(self.csv, quotechar='"', delimiter=',', skipinitialspace=True):
            # header
            # id,timebin,prefix,hege,af,visibility,rpki_status,irr_status, delegated_prefix_status,
            #delegated_asn_status,descr,moas,asn_id,country_id,originasn_id

            rec = dict( zip(self.csv.fields, line) )
            rec['hege'] = float(rec['hege'])
            rec['visibility'] = float(rec['visibility'])
            rec['af'] = int(rec['af'])

            prefix = rec['prefix']
            if prefix not in prefix_id:
                prefix_id[prefix] = self.iyp.get_node('PREFIX', {'prefix': prefix}, create=True)

            # make status/country/origin links only for lines where asn=originasn
            if rec['asn_id'] == rec['originasn_id']:
                # Make sure all nodes exist
                originasn = int(rec['originasn_id'])
                if originasn not in asn_id:
                    asn_id[originasn] = self.iyp.get_node('AS', {'asn': originasn}, create=True)

                rpki_status = 'RPKI '+rec['rpki_status']
                if rpki_status not in tag_id:
                    tag_id[rpki_status] = self.iyp.get_node('TAG', {'label': rpki_status}, create=True)

                irr_status = 'IRR '+rec['irr_status']
                if irr_status not in tag_id:
                    tag_id[irr_status] = self.iyp.get_node('TAG', {'label': irr_status}, create=True)

                cc = rec['country_id']
                if cc not in country_id:
                    country_id[cc] = self.iyp.get_node('COUNTRY', {'country_code': cc}, create=True)

                # Compute links
                orig_links.append( {
                    'src_id': asn_id[originasn],
                    'dst_id': prefix_id[prefix],
                    'props': [self.reference, rec]
                    } )

                tag_links.append( {
                    'src_id': prefix_id[prefix],
                    'dst_id': tag_id[rpki_status],
                    'props': [self.reference, rec]
                    } )

                tag_links.append( {
                    'src_id': prefix_id[prefix],
                    'dst_id': tag_id[irr_status],
                    'props': [self.reference, rec]
                    } )

                country_links.append( {
                    'src_id': prefix_id[prefix],
                    'dst_id': country_id[cc],
                    'props': [self.reference]
                    } )

            # Dependency links
            asn = int(rec['asn_id'])
            if asn not in asn_id:
                asn_id[asn] = self.iyp.get_node('AS', {'asn': asn}, create=True)

            dep_links.append( {
                'src_id': prefix_id[prefix],
                'dst_id': asn_id[asn],
                'props': [self.reference, rec]
                } )


        self.csv.close()

        # Push links to IYP
        logging.warning('Pushing links to neo4j...\n')
        self.iyp.batch_add_links('ORIGINATE', orig_links)
        self.iyp.batch_add_links('CATEGORIZED', tag_links)
        self.iyp.batch_add_links('DEPENDS_ON', dep_links)
        self.iyp.batch_add_links('COUNTRY', country_links)

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
    crawler.run()
    crawler.close()

    logging.info("End: %s" % sys.argv)

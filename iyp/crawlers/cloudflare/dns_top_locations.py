import os
import flatdict
import glob
import sys
import json
import logging
import requests
from requests.adapters import HTTPAdapter, Retry
from collections import defaultdict
from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'Cloudflare'
URL = 'https://api.cloudflare.com/client/v4/radar/dns/top/locations/'
NAME = 'cloudflare.dns_top_locations'
BATCH_SIZE = 1
RANK_THRESHOLD = 10000
TOP_LIMIT = 100/BATCH_SIZE

# API credentials
USER_ID = ''
AUTH_EMAIL = ''
AUTH_KEY = ''
if os.path.exists('config.json'): 
    config = json.load(open('config.json', 'r'))
    API_KEY = config['cloudflare']['apikey']

class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference

    def __init__(self, organization, url, name):

        # Initialize IYP connection
        super().__init__(organization, url, name)

        # Fetch domain names registered in IYP
        existing_dn = self.iyp.tx.run(
                f"""MATCH (dn:DomainName)-[r:RANK]-(:Ranking) 
                WHERE r.rank < {RANK_THRESHOLD}
                RETURN ID(dn) AS _id, dn.name AS dname;""")


        self.domain_names_id = { node['dname']: node['_id'] for node in existing_dn }
        self.domain_names = list(self.domain_names_id.keys())


    def fetch(self):
        """Download top locations for top RANK_THRESHOLD domain names registered
        in IYP and save it on disk"""


        # setup HTTPS session with credentials and retry
        req_session = requests.Session()
        req_session.headers['Authorization'] = 'Bearer '+API_KEY
        req_session.headers['Content-Type'] = 'application/json'

        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[ 500, 502, 503, 504 ])

        req_session.mount('http://', HTTPAdapter(max_retries=retries))
        req_session.mount('https://', HTTPAdapter(max_retries=retries))

        tmp_dir = self.create_tmp_dir()

        # Query Cloudflare API in batches
        for i in range(0, len(self.domain_names), BATCH_SIZE):

            get_params = f'?limit={TOP_LIMIT}'
            for domain in self.domain_names[i:i+BATCH_SIZE]:
                get_params += f'&dateRange=7d&domain={domain}&name={domain}'

            url = self.url+get_params

            # Fetch data
            req = req_session.get(url)
            if req.status_code != 200:
                logging.error(f'Cannot download data {req.status_code}: {req.text}')
                # Cannot get the data? skip this one
                continue
                #sys.exit('Error while fetching data file')

            fname = 'data_'+'_'.join(self.domain_names[i:i+BATCH_SIZE])
            with open(f'{tmp_dir}/{fname}.json', 'wb') as fp:
                fp.write(req.content)
         

    def run(self):
        """Push data to IYP. """

        # FIXME this should be called before/separately 
        self.fetch()

        self.country_id = self.iyp.batch_get_nodes('Country', 'country_code')

        tmp_dir = self.get_tmp_dir()
         
        files = glob.glob(f'{tmp_dir}/data_*.json')
        for file in files:
            with open(file, 'rb') as fp:
                # Process line one after the other
                for i, _ in enumerate(map(self.update, json.load(fp)['result'].items())):
                    sys.stderr.write(f'\rProcessed {i} lines')

        sys.stderr.write('\n')

    def update(self, param):
        """Save the domain name' top countries and corresponding properties."""

        domain, countries = param
        statements = []

        if domain == 'meta':
            return

        for entry in countries:
            cc = entry['clientCountryAlpha2']

            # set link
            entry['value'] = float(entry['value'])
            flat_prop = dict(flatdict.FlatDict(entry))
            statements.append([ 'QUERIED_FROM', self.country_id[cc], dict(flat_prop, **self.reference) ])

        self.iyp.add_links( self.domain_names_id[domain], statements )
        
# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.WARNING, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    crawler = Crawler(ORG, URL, NAME)
    crawler.run()
    crawler.close()

    logging.info("Ended")

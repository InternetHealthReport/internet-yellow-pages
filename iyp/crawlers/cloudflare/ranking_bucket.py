import os
import sys
import json
import logging
import requests
from zipfile import ZipFile
import io
from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'Cloudflare'
URL_DATASETS = 'https://api.cloudflare.com/client/v4/radar/datasets?limit=10&offset=0&datasetType=RANKING_BUCKET&format=json' 
URL_DL = 'https://api.cloudflare.com/client/v4/radar/datasets/download'  

API_KEY = ''
if os.path.exists('config.json'): 
    API_KEY = json.load(open('config.json', 'r'))['cloudflare']['apikey']

class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference

    def run(self):
        """Fetch data and push to IYP. """

        s = requests.Session()
        s.headers.update( {
                'Authorization': 'Bearer '+API_KEY,
                'Content-Type': 'application/json'
                } )

        # Fetch rankings descriptions
        req = s.get(URL_DATASETS)
        if req.status_code != 200:
            logging.error(f'Cannot download data {req.status_code}: {req.text}')
            sys.exit('Error while fetching data file')

        for dataset in req.json()['result']['datasets']:
            self.ranking_qid = self.iyp.get_node(
                    'RANKING', 
                    {
                        'name': f'Cloudflare '+dataset['title'],
                        'description': dataset['description'],
                        'top': dataset['meta']['top']
                    }, 
                    create=True)

            # Get the dataset url
            req = s.post(URL_DL, json={'datasetId': dataset['id']})
            if req.status_code != 200:
                logging.error(f'Cannot get url for dataset {dataset["id"]} {req.status_code}: {req.text}')
                continue

            print(req.json())

            self.reference['reference_url'] = req.json()['result']['dataset']['url']
            req = requests.get(self.reference['reference_url'])
            if req.status_code != 200:
                logging.error(f'Cannot download dataset {dataset["id"]} {req.status_code}: {req.text}')
                continue

            # open zip file and read top list
            with  ZipFile(io.BytesIO(req.content)) as z:
                for fname in z.namelist():
                    with z.open(fname) as list:
                        for i, domain in enumerate(io.TextIOWrapper(list)):

                            #skip the header
                            if i == 0:
                                continue

                            domain = domain.rstrip()
                            sys.stderr.write(f'\rProcessed {i} domains \t {domain}')
                            self.update(domain)

            sys.stderr.write('\n')
    
    def update(self, domain):
        """Add the domain to IYP if it's not already there and update its
        properties."""

        # set rank
        statements = [[ 'RANK', self.ranking_qid, self.reference ]]

        # Commit to IYP
        # Get the AS's node ID (create if it is not yet registered) and commit changes
        domain_qid = self.iyp.get_node('DOMAIN_NAME', {'name': domain}, create=True) 
        self.iyp.add_links( domain_qid, statements )
        
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

    crawler = Crawler(ORG, '')
    crawler.run()
    crawler.close()

import os
import sys
import logging
import json
import iso3166
from iyp import BaseCrawler
import requests

ORG = 'PeeringDB'

# URL to peeringdb API for organizations
URL_PDB_ORGS = 'https://peeringdb.com/api/org'

# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PEERINGDB_ORG_ID' 

API_KEY = ""
if os.path.exists('config.json'): 
    API_KEY = json.load(open('config.json', 'r'))['peeringdb']['apikey']

class Crawler(BaseCrawler):
    def __init__(self, organization, url):
        """Initialisation for pushing peeringDB organizations to IYP. """

        self.headers = {"Authorization": "Api-Key " + API_KEY}

        super().__init__(organization, url)
    
    def run(self):
        """Fetch organizations information from PeeringDB and push to IYP"""

        sys.stderr.write('Fetching PeeringDB data...\n')
        req = requests.get(URL_PDB_ORGS, headers=self.headers)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')
        organizations = json.loads(req.text)['data']

        for i, _ in enumerate(map(self.update_org, organizations)):
            sys.stderr.write(f'\rProcessing... {i+1}/{len(organizations)}')

            # commit every 1k lines
            if i % 1000 == 0:
                self.iyp.commit()
                
        sys.stderr.write('\n')

    def update_org(self, organization):
        """Add the organization to wikibase if it's not there and update properties"""

        # set property name
        name_qid = self.iyp.get_node('NAME', {'name': organization['name'].strip()}, create=True)
        statements = [ ['NAME', name_qid, self.reference] ] 

        # set property website
        if organization['website']:
            website_qid = self.iyp.get_node('URL', {'name': organization['website'].strip()}, create=True)
            statements.append([ 'WEBSITE', website_qid, self.reference])

        # set property country
        if organization['country'] in iso3166.countries_by_alpha2:
            country_qid = self.iyp.get_node('COUNTRY', {'country_code': organization['country']}, create=True)
            statements.append(['COUNTRY', country_qid, self.reference])

        orgid_qid = self.iyp.get_node(ORGID_LABEL, {'id': organization['id']}, create=True)
        statements.append( ['EXTERNAL_ID', orgid_qid, self.reference] )

        # Add this organization to IYP
        org_qid = self.iyp.get_node('ORGANIZATION', {'name':organization['name'].strip()}, create=True)
        self.iyp.add_links(org_qid, statements )
        
        return org_qid


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

    pdbo = Crawler(ORG, '')
    pdbo.run()
    pdbo.close()

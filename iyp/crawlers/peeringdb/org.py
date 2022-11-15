import sys
import logging
import json
import iso3166
from datetime import datetime, time
from iyp import IYP
import requests

ORG = 'PeeringDB'

# URL to peeringdb API for organizations
URL_PDB_ORGS = 'https://peeringdb.com/api/org'

# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PEERINGDB_ORG_ID' 

class Crawler(object):
    def __init__(self):
        """Initialisation for pushing peeringDB organizations to IYP. """
    
        self.reference = {
            'source': ORG,
            'reference_url': URL_PDB_ORGS,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # connection to IYP database
        self.iyp = IYP()

    def run(self):
        """Fetch organizations information from PeeringDB and push to IYP"""

        sys.stderr.write('Fetching PeeringDB data...\n')
        req = requests.get(URL_PDB_ORGS)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')
        organizations = json.loads(req.text)['data']

        for i, _ in enumerate(map(self.update_org, organizations)):
            sys.stderr.write(f'\rProcessing... {i+1}/{len(organizations)}')

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

        statements.append( ['EXTERNAL_ID', organization['id'], self.reference] )

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

    pdbo = Crawler()
    pdbo.run()

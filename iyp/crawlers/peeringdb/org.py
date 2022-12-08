import flatdict
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

        sys.stderr.write('Compute nodes\n')
        # compute nodes
        orgs = set()
        names = set()
        websites = set()
        countries = set()
        orgids = set()

        for org in organizations:
            orgs.add( org['name'].strip() )
            names.add( org['name'].strip() )
            orgids.add( org['id'] )
            
            if org['website']:
                websites.add( org['website'].strip() )

            if org['country'] in iso3166.countries_by_alpha2:
                countries.add( org['country'] )

        # push nodes
        self.org_id = self.iyp.batch_get_nodes('ORGANIZATION', 'name', orgs)
        self.name_id = self.iyp.batch_get_nodes('NAME', 'name', names)
        self.website_id = self.iyp.batch_get_nodes('URL', 'url', websites)
        self.country_id = self.iyp.batch_get_nodes('COUNTRY', 'country_code', countries)
        self.orgid_id = self.iyp.batch_get_nodes(ORGID_LABEL, 'id', orgids)

        # compute links
        sys.stderr.write('Compute links\n')

        name_links = []
        website_links = []
        country_links = []
        orgid_links = []

        for org in organizations:

            org_qid = self.org_id[org['name'].strip()] 
            name_qid = self.name_id[org['name'].strip()] 
            website_qid = self.website_id[org['website'].strip()] 
            country_qid = self.website_id[org['country']] 
            orgid_qid = self.website_id[org['country']] 

            flat_org = dict(flatdict.FlatDict(org, delimiter='_'))

            name_links.append( { 'src_id':org_qid, 'dst_id':name_qid, 'props':[self.reference, flat_org] } ) 
            website_links.append( { 'src_id':org_qid, 'dst_id':website_qid, 'props':[self.reference, flat_org] } )
            country_links.append( { 'src_id':org_qid, 'dst_id':country_qid, 'props':[self.reference, flat_org] } )
            orgid_links.append( { 'src_id':org_qid, 'dst_id':orgid_qid, 'props':[self.reference, flat_org] } )

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('EXTERNAL_ID', orgid_links)
        

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

    pdbo = Crawler(ORG, '')
    pdbo.run()
    pdbo.close()

    logging.info("End: %s" % sys.argv)

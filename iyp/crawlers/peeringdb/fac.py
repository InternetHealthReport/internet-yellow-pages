import flatdict
import os
import sys
import logging
import json
import iso3166
from iyp import BaseCrawler
import requests_cache

# NOTES This script should be executed after peeringdb.org

ORG = 'PeeringDB'

# URL to peeringdb API for facilities
URL = 'https://peeringdb.com/api/fac'
NAME = 'peeringdb.fac'

# Label used for the nodes representing the organization and facility IDs
ORGID_LABEL = 'PeeringdbOrgID'
FACID_LABEL = 'PeeringdbFacID'

API_KEY = ""
if os.path.exists('config.json'): 
    API_KEY = json.load(open('config.json', 'r'))['peeringdb']['apikey']

class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialisation for pushing peeringDB facilities to IYP. """

        self.headers = {"Authorization": "Api-Key " + API_KEY}
        self.requests = requests_cache.CachedSession(ORG)

        super().__init__(organization, url, name)

    
    def run(self):
        """Fetch facilities information from PeeringDB and push to IYP"""

        sys.stderr.write('Fetching PeeringDB data...\n')
        req = self.requests.get(URL, headers=self.headers)
        if req.status_code != 200:
            logging.error('Error while fetching peeringDB data')
            raise Exception(f'Cannot fetch peeringdb data, status code={req.status_code}\n{req.text}')

        facilities = json.loads(req.text)['data']

        # compute nodes
        facs = set()
        names = set()
        websites = set()
        countries = set()
        facids = set()

        for fac in facilities:
            facs.add( fac['name'].strip() )
            names.add( fac['name'].strip() )
            facids.add( fac['id'] )
            
            if fac['website']:
                websites.add( fac['website'].strip() )

            if fac['country'] in iso3166.countries_by_alpha2:
                countries.add( fac['country'] )

        # push nodes
        self.fac_id = self.iyp.batch_get_nodes('Facility', 'name', facs)
        self.name_id = self.iyp.batch_get_nodes('Name', 'name', names)
        self.website_id = self.iyp.batch_get_nodes('URL', 'url', websites)
        self.country_id = self.iyp.batch_get_nodes('Country', 'country_code', countries)
        self.facid_id = self.iyp.batch_get_nodes(FACID_LABEL, 'id', facids)

        # get organization nodes
        self.org_id = self.iyp.batch_get_node_extid(ORGID_LABEL)

        # compute links
        name_links = []
        website_links = []
        country_links = []
        facid_links = []
        org_links = []

        for fac in facilities:

            flat_fac = {}
            try:
                flat_fac = dict(flatdict.FlatDict(fac))
            except Exception as e:
                sys.stderr.write(f'Cannot flatten dictionary {fac}\n{e}\n')
                logging.error(f'Cannot flatten dictionary {fac}\n{e}')



            facid_qid = self.facid_id[fac['id']] 
            fac_qid = self.fac_id[fac['name'].strip()] 
            facid_links.append( { 'src_id':fac_qid, 'dst_id':facid_qid, 'props':[self.reference, flat_fac] } )

            name_qid = self.name_id[fac['name'].strip()] 
            name_links.append( { 'src_id':fac_qid, 'dst_id':name_qid, 'props':[self.reference] } ) 

            if 'website' in fac and fac['website'] in self.website_id:
                website_qid = self.website_id[fac['website'].strip()] 
                website_links.append( { 'src_id':fac_qid, 'dst_id':website_qid, 'props':[self.reference] } )

            if 'country' in fac and fac['country'] in self.country_id:
                country_qid = self.country_id[fac['country']] 
                country_links.append( { 'src_id':fac_qid, 'dst_id':country_qid, 'props':[self.reference] } )

            org_qid = self.org_id[fac['org_id']]
            org_links.append( { 'src_id':fac_qid, 'dst_id':org_qid, 'props':[self.reference] } )

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('EXTERNAL_ID', facid_links)
        self.iyp.batch_add_links('MANAGED_BY', org_links)
        

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

    pdbo = Crawler(ORG, '', NAME)
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        pdbo.unit_test(logging)
    else:
        pdbo.run()
        pdbo.close()

    logging.info("End: %s" % sys.argv)


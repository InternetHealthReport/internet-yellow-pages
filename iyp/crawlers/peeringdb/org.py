import argparse
import flatdict
import iso3166
import json
import logging
import os
import requests_cache
import sys

from iyp import BaseCrawler

from iyp.crawlers.peeringdb.ix import handle_social_media

ORG = 'PeeringDB'

# URL to peeringdb API for organizations
URL = 'https://peeringdb.com/api/org'
NAME = 'peeringdb.org'

# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PeeringdbOrgID'

API_KEY = ""
if os.path.exists('config.json'): 
    API_KEY = json.load(open('config.json', 'r'))['peeringdb']['apikey']

class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialisation for pushing peeringDB organizations to IYP. """

        self.headers = {"Authorization": "Api-Key " + API_KEY}
        self.requests = requests_cache.CachedSession(ORG)

        super().__init__(organization, url, name)
    
    def run(self):
        """Fetch organizations information from PeeringDB and push to IYP"""

        req = self.requests.get(URL, headers=self.headers)
        if req.status_code != 200:
            logging.error('Error while fetching peeringDB data')
            raise Exception(f'Cannot fetch peeringdb data, status code={req.status_code}\n{req.text}')

        organizations = json.loads(req.text)['data']

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

            handle_social_media(org, websites)

        # push nodes
        self.org_id = self.iyp.batch_get_nodes('Organization', 'name', orgs)
        self.name_id = self.iyp.batch_get_nodes('Name', 'name', names)
        self.website_id = self.iyp.batch_get_nodes('URL', 'url', websites)
        self.country_id = self.iyp.batch_get_nodes('Country', 'country_code', countries)
        self.orgid_id = self.iyp.batch_get_nodes(ORGID_LABEL, 'id', orgids)

        # compute links
        name_links = []
        website_links = []
        country_links = []
        orgid_links = []

        for org in organizations:

            flat_org = {}
            try:
                flat_org = dict(flatdict.FlatDict(org))
            except Exception as e:
                sys.stderr.write(f'Cannot flatten dictionary {org}\n{e}\n')
                logging.error(f'Cannot flatten dictionary {org}\n{e}')

            orgid_qid = self.orgid_id[org['id']] 
            org_qid = self.org_id[org['name'].strip()] 
            orgid_links.append( { 'src_id':org_qid, 'dst_id':orgid_qid, 'props':[self.reference, flat_org] } )

            name_qid = self.name_id[org['name'].strip()] 
            name_links.append( { 'src_id':org_qid, 'dst_id':name_qid, 'props':[self.reference] } ) 

            if 'website' in org and org['website'] in self.website_id:
                website_qid = self.website_id[org['website'].strip()] 
                website_links.append( { 'src_id':org_qid, 'dst_id':website_qid, 'props':[self.reference] } )

            if 'country' in org and org['country'] in self.country_id:
                country_qid = self.country_id[org['country']] 
                country_links.append( { 'src_id':org_qid, 'dst_id':country_qid, 'props':[self.reference] } )

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('EXTERNAL_ID', orgid_links)
        

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/'+scriptname+'.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)


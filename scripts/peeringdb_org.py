import sys
import requests
import json
from concurrent.futures import ThreadPoolExecutor
import iso3166
import progressbar
import wikihandy

import datetime

# URL to peeringdb API for organizations
URL_PDB_ORGS = 'https://peeringdb.com/api/org'

# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PeeringDB organization ID' 

class PDBOrganizations(object):
    def __init__(self):
        """Create an item representing the 'PeeringDB organization ID' class if 
        doesn't already exist. And fetch QIDs for organizations already in the
        wikibase."""
    
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        # Added properties will have this additional information
        today = self.wh.today()
        self.qualifiers = [
                (self.wh.get_pid('reference URL'), URL_PDB_ORGS),
                (self.wh.get_pid('source'), self.wh.get_qid('PeeringDB')),
                (self.wh.get_pid('point in time'), today)
                ]

        # Check if there is an item representing the organization IDs
        # Create it if it doesn't exist
        orgid_qid = self.wh.get_qid(ORGID_LABEL)
        if ORGID_LABEL is None:
            orgid_qid = self.wh.add_item(
                'add PeeringDB org IDs',                                      # Commit message
                ORGID_LABEL,                                                  # Label 
                'Identifier for an organization in the PeeringDB database')   # Description

        # Load the QIDs for organizations already available in the wikibase
        self.orgid2qid = self.wh.extid2qid(qid=orgid_qid)

    def run(self):
        """Fetch organizations information from PeeringDB and push to wikibase"""

        req = requests.get(URL_PDB_ORGS)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')
        organizations = json.loads(req.text)['data']

        self.wh.login() # Login once for all threads

        pool = ThreadPoolExecutor()
        for i, res in enumerate(pool.map(self.update_org, organizations)):
            sys.stderr.write(f'\rProcessing... {i}/{len(organizations)}')
        pool.shutdown()

    def update_org(self, organization):
        """Add the organization to wikibase if it's not there and update properties"""

        # Check if the organization is in the wikibase
        if str(organization['id']) not in self.orgid2qid :
            # Add this organization to the wikibase
            org_qid = self.wh.add_item('add new peeringDB organization', organization['name'])
            # keep track of this QID
            self.orgid2qid[str(organization['id'])] = org_qid
            # Add properties to this new organization
            org_qualifiers = [
                    (self.wh.get_pid('instance of'), self.wh.get_qid(ORGID_LABEL)),
                    (self.wh.get_pid('reference URL'), URL_PDB_ORGS),
                    (self.wh.get_pid('source'), self.wh.get_qid('PeeringDB'))
                    ]
            self.wh.upsert_statement('add new peeringDB organization ID', 
                    org_qid, self.wh.get_pid('external ID'), str(organization['id']), org_qualifiers)

        # Update name, website, and country for this organization
        org_qid = self.orgid2qid[str(organization['id'])]

        self.wh.upsert_statement('update peeringDB organization', 
                org_qid, self.wh.get_pid('name'), organization['name'], self.qualifiers)

        if organization['website']:
            self.wh.upsert_statement('update peeringDB organization', 
                org_qid, self.wh.get_pid('website'), organization['website'], self.qualifiers)

        if organization['country'] in iso3166.countries_by_alpha2:
            country_qid = self.wh.get_qid(iso3166.countries_by_alpha2[organization['country']].name)
            if country_qid is not None:
                self.wh.upsert_statement('update peeringDB organization', 
                    org_qid, self.wh.get_pid('country'), country_qid, self.qualifiers)
        
        return org_qid

# Main program
if __name__ == '__main__':
    pdbo = PDBOrganizations()
    pdbo.run()

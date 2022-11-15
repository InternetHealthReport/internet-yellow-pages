import sys
import logging
import requests
import json
from datetime import datetime, time
from iyp import IYP

ORG = 'PeeringDB'

# URL to peeringdb API for exchange points
URL_PDB_IXS = 'https://peeringdb.com/api/ix?depth=2'
# API endpoint for LAN prefixes
URL_PDB_LAN = 'https://peeringdb.com/api/ixlan'


# Label used for nodes representing the exchange point IDs
IXID_LABEL = 'PEERINGDB_IX_ID' 
# Label used for nodes representing the organization IDs
ORGID_LABEL = 'PEERINGDB_ORG_ID' 

class Crawler(object):
    def __init__(self):
        """Initialisation for pushing peeringDB IXPs to IYP"""
    
        self.reference = {
            'source': ORG,
            'reference_url': URL_PDB_IXS,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # connection to IYP database
        self.iyp = IYP()

    def run(self):
        """Fetch ixs information from PeeringDB and push to IYP. 
        Using multiple threads for better performances."""

        req = requests.get(URL_PDB_IXS)
        if req.status_code != 200:
            sys.exit(f'Error while fetching IXs data ({req.status_code})')
        ixs = json.loads(req.text)['data']

        for i, ix_info in enumerate(ixs):

            # Get more info for this IX
            #req = requests.get(f'{URL_PDB_IXS}/{ix["id"]}')
            #if req.status_code != 200:
            #    sys.exit(f'Error while fetching IXs data ({req.status_code})')
            #ix_info = json.loads(req.text)['data'][0]

            # Update info in wiki
            self.update_ix(ix_info)

            sys.stderr.write(f'\rProcessing... {i+1}/{len(ixs)}')


    def update_ix(self, ix):
        """Add the IXP to IYP if it's not already there and update its
        properties."""

        ix_qid = self.ix_qid(ix) 

        # update LAN corresponding to this IX
        if 'ixlan_set' in ix:
            for ixlan in ix['ixlan_set']:
                pfx_url = f'{URL_PDB_LAN}/{ixlan["id"]}'
                pfx_ref = {
                    'source': ORG,
                    'reference_url': pfx_url,
                    'point_in_time': datetime.combine(datetime.utcnow(), time.min)
                    }

                req = requests.get(pfx_url)
                if req.status_code != 200:
                    sys.exit(f'Error while fetching IX LAN data ({req.status_code})')
                lans = json.loads(req.text)['data']

                for lan in lans:
                    for prefix in lan['ixpfx_set']:
                        pfx_qid = self.iyp.get_node(['PREFIX', 'PEERING_LAN'], {'prefix': prefix['prefix']}, create=True)

                        pfx_stmts = [ 
                                ['MANAGED_BY', ix_qid, pfx_ref]
                                ]

                        self.iyp.add_links( pfx_qid, pfx_stmts )

        return ix_qid


    def ix_qid(self, ix):
        """Find the ix QID for the given ix.
        If this ix is not yet registered in IYP then add it.

        Return the ix QID."""
        
        # Set properties for this ix
        statements = []

        # link to corresponding organization
        org_qid = self.iyp.get_node_extid(ORGID_LABEL, ix['org_id'])
        if org_qid is not None:
            statements.append( ['MANAGED_BY', org_qid, self.reference])
        else:
            print('Error this organization is not in IYP: ',ix['org_id'])

        # set property country
        if ix['country']:
            country_qid = self.iyp.get_node('COUNTRY', {'country_code': ix['country']}, create=True)
            statements.append(['COUNTRY', country_qid, self.reference])

        # set property website
        if ix['website']:
            website_qid = self.iyp.get_node('URL', {'url': ix['website']}, create=True)
            statements.append( ['WEBSITE', website_qid, self.reference] )

        # set traffic webpage 
        #if ix['url_stats']:
            #statements.append([ 
                #self.wh.get_pid('website'), ix['url_stats'],  # statement
                #self.reference,                               # reference 
                #[ (self.wh.get_pid('instance of'), self.wh.get_qid('traffic statistics')), ] # qualifier
                #])

        ixid_qid = self.iyp.get_node(IXID_LABEL, {'id': ix['id']}, create=True)
        statements.append( ['EXTERNAL_ID', ixid_qid, self.reference] )

        name_qid = self.iyp.get_node('NAME', {'name': ix['name'].strip()}, create=True)
        statements.append( ['NAME', name_qid, self.reference] )

        # Add this ix to the wikibase
        ixp_qid = self.iyp.get_node('IXP', {'name': ix['name']}, create=True)
        self.iyp.add_links(ixp_qid, statements)

        return ixp_qid


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

    pdbn = Crawler()
    pdbn.run()


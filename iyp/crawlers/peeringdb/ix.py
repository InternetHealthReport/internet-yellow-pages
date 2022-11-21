import os
import sys
import logging
import requests_cache
import json
from datetime import datetime, time
from iyp import BaseCrawler

# NOTES
# This script should be executed after peeringdb.org

ORG = 'PeeringDB'

# URL to peeringdb API for exchange points
URL_PDB_IXS = 'https://peeringdb.com/api/ix?depth=2'
# API endpoint for LAN prefixes
URL_PDB_LANS = 'https://peeringdb.com/api/ixlan?depth=2'

# Label used for nodes representing the exchange point IDs
IXID_LABEL = 'PEERINGDB_IX_ID' 
# Label used for nodes representing the organization IDs
ORGID_LABEL = 'PEERINGDB_ORG_ID' 
# Label used for the class/item representing the network IDs
NETID_LABEL = 'PEERINGDB_NET_ID' 

API_KEY = ""
if os.path.exists('config.json'): 
    API_KEY = json.load(open('config.json', 'r'))['peeringdb']['apikey']

class Crawler(BaseCrawler):
    def __init__(self, organization, url):
        """Initialisation for pushing peeringDB IXPs to IYP"""
    
        self.headers = {"Authorization": "Api-Key " + API_KEY}
    
        self.reference_ix = {
            'source': ORG,
            'reference_url': URL_PDB_IXS,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        self.reference_lan = {
            'source': ORG,
            'reference_url': URL_PDB_LANS,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # keep track of added networks
        self.nets = {}

        # Using cached queries
        self.requests = requests_cache.CachedSession(ORG)

        # connection to IYP database
        super().__init__(organization, url)

    def run(self):
        """Fetch ixs information from PeeringDB and push to IYP. 
        Using multiple threads for better performances."""

        req = self.requests.get( URL_PDB_IXS, headers=self.headers)
        if req.status_code != 200:
            sys.exit(f'Error while fetching IXs data\n({req.status_code}) {req.text}')
        self.ixs = json.loads(req.text)['data']

        req = self.requests.get( URL_PDB_LANS, headers=self.headers)
        if req.status_code != 200:
            sys.exit(f'Error while fetching IXLANs data\n({req.status_code}) {req.text}')
        ixlans = json.loads(req.text)['data']
        
        # index ixlans by their id
        self.ixlans = {}
        for ixlan in ixlans:
            self.ixlans[ixlan['id']] = ixlan

        for i, ix_info in enumerate(self.ixs):

            # Push data to IYP
            self.update_ix(ix_info)

            sys.stderr.write(f'\rProcessing... {i+1}/{len(self.ixs)}')

        sys.stderr.write('\n')

    def update_ix(self, ix):
        """Add the IXP to IYP if it's not already there and update its
        properties."""

        ix_qid = self.ix_qid(ix) 

        # update LAN corresponding to this IX
        if 'ixlan_set' in ix:
            for ixlan in ix['ixlan_set']:
                #req = requests.get( pfx_url, headers=self.headers )
                #if req.status_code != 200:
                #    sys.exit(f'Error while fetching IX LAN data ({req.status_code})')

                if ixlan['id'] not in self.ixlans:
                    logging.error(f'LAN not found: ixlan ID {ixlan["id"]} not in {self.ixlans}')
                    continue

                lan = self.ixlans[ ixlan["id"] ]

                for prefix in lan['ixpfx_set']:
                    af = 6
                    if '.' in prefix['prefix']:
                        af = 4
                    pfx_qid = self.iyp.get_node(
                            ['PREFIX', 'PEERING_LAN'], 
                            {'prefix': prefix['prefix'], 'af': af}, 
                            create=True
                            )

                    pfx_stmts = [ 
                            ['MANAGED_BY', ix_qid, self.reference_lan]
                            ]

                    self.iyp.add_links( pfx_qid, pfx_stmts )

                for network in lan['net_set']:
                    net_qid = self.update_net(network)

                    # Update membership
                    statements = [ ['MEMBER_OF', ix_qid, self.reference_lan] ]
                    self.iyp.add_links(net_qid, statements)

        return ix_qid


    def update_net(self, network):
        """Add the network to IYP and corresponding properties."""

        if network['id'] not in self.nets:
            # set property name
            name_qid = self.iyp.get_node('NAME', {'name': network['name'].strip()}, create=True)
            statements = [ ['NAME', name_qid, self.reference_lan] ] 

            # link to corresponding organization
            org_qid = self.iyp.get_node_extid(ORGID_LABEL, network['org_id'])
            if org_qid is not None:
                statements.append( ['MANAGED_BY', org_qid, self.reference_lan])
            else:
                logging.error(f'Error this organization is not in IYP: {network["org_id"]}')

            # set property website
            if network['website']:
                website_qid = self.iyp.get_node('URL', {'url': network['website']}, create=True)
                statements.append( ['WEBSITE', website_qid, self.reference_lan] )

            netid_qid = self.iyp.get_node(NETID_LABEL, {'id': network['id']}, create=True)
            statements.append( ['EXTERNAL_ID', netid_qid,  self.reference_lan] )

            # Add this network to IYP
            net_qid = self.iyp.get_node('AS', {'asn': network['asn']}, create=True)
            self.iyp.add_links( net_qid, statements)

            # keep track of the node id
            self.nets[network['id']] = net_qid

        return self.nets[network['id']]


    def ix_qid(self, ix):
        """Add the IX to IYP and return corresponding node's ID.
        """
        
        # Set properties for this ix
        statements = []

        # link to corresponding organization
        org_qid = self.iyp.get_node_extid(ORGID_LABEL, ix['org_id'])
        if org_qid is not None:
            statements.append( ['MANAGED_BY', org_qid, self.reference_ix])
        else:
            logging.error(f'Error this organization is not in IYP: {ix["org_id"]}')

        # set property country
        if ix['country']:
            country_qid = self.iyp.get_node('COUNTRY', {'country_code': ix['country']}, create=True)
            statements.append(['COUNTRY', country_qid, self.reference_ix])

        # set property website
        if ix['website']:
            website_qid = self.iyp.get_node('URL', {'url': ix['website']}, create=True)
            statements.append( ['WEBSITE', website_qid, self.reference_ix] )

        # set traffic webpage 
        #if ix['url_stats']:
            #statements.append([ 
                #self.wh.get_pid('website'), ix['url_stats'],  # statement
                #self.reference,                               # reference 
                #[ (self.wh.get_pid('instance of'), self.wh.get_qid('traffic statistics')), ] # qualifier
                #])

        ixid_qid = self.iyp.get_node(IXID_LABEL, {'id': ix['id']}, create=True)
        statements.append( ['EXTERNAL_ID', ixid_qid, self.reference_ix] )

        name_qid = self.iyp.get_node('NAME', {'name': ix['name'].strip()}, create=True)
        statements.append( ['NAME', name_qid, self.reference_ix] )

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
            level=logging.WARNING, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    pdbn = Crawler(ORG, '')
    pdbn.run()
    pdbn.close()

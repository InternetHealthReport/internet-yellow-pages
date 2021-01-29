import sys
import requests
import json
from concurrent.futures import ThreadPoolExecutor
import wikihandy

# URL to peeringdb API for networks
URL_PDB_NETS = 'https://peeringdb.com/api/net'

# Label used for the class/item representing the network IDs
NETID_LABEL = 'PeeringDB network ID' 
# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PeeringDB organization ID' 
# Label used for the class/item representing the exchange point IDs
IXID_LABEL = 'PeeringDB IX ID' 

class PDBNetworks(object):
    def __init__(self):
        """Create an item representing the PeeringDB network ID class if 
        doesn't already exist. And fetch QIDs for networks already in the
        wikibase."""
    
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        # Get the QID of the item representing PeeringDB network IDs
        netid_qid = self.wh.get_qid(NETID_LABEL,
                create={                                                            # Create it if it doesn't exist
                    'summary': 'add PeeringDB net IDs',                             # Commit message
                    'description': 'Identifier for a network in the PeeringDB database' # Description
                    })

        # Load the QIDs for networks already available in the wikibase
        self.netid2qid = self.wh.extid2qid(qid=netid_qid)
        # Load the QIDs for peeringDB organizations
        self.orgid2qid = self.wh.extid2qid(label=ORGID_LABEL)
        # Load the QIDs for peeringDB IXs
        self.ixid2qid = self.wh.extid2qid(label=IXID_LABEL)

        # Added properties will have this reference information
        today = self.wh.today()
        self.reference = [
                (self.wh.get_pid('source'), self.wh.get_qid('PeeringDB')),
                (self.wh.get_pid('reference URL'), URL_PDB_NETS),
                (self.wh.get_pid('point in time'), today)
                ]


    def run(self):
        """Fetch networks information from PeeringDB and push to wikibase. 
        Using multiple threads for better performances."""

        req = requests.get(URL_PDB_NETS)
        if req.status_code != 200:
            sys.exit('Error while fetching data from API')
        networks = json.loads(req.text)['data']

        self.wh.login() # Login once for all threads

        pool = ThreadPoolExecutor()
        for i, res in enumerate(pool.map(self.update_net, networks)):
            sys.stderr.write(f'\rProcessing... {i+1}/{len(networks)}')
        pool.shutdown()


    def update_net(self, network):
        """Add the network to wikibase if it's not already there and update its
        properties."""

        # set property name
        statements = [ [self.wh.get_pid('name'), network['name'].strip(), self.reference] ] 

        # link to corresponding organization
        org_qid = self.orgid2qid.get(str(network['org_id']))
        if org_qid is not None:
            statements.append( [self.wh.get_pid('managed by'), org_qid, self.reference])
        else:
            print('Error this organization is not in wikibase: ',network['org_id'])

        # set property website
        if network['website']:
            statements.append([ self.wh.get_pid('website'), network['website'], self.reference])

        # Update IX membership
        # Fetch membership for this network
        netixlan_url = URL_PDB_NETS+f'/{network["id"]}'
        req = requests.get(netixlan_url)
        if req.status_code != 200:
            sys.exit(f'Error while fetching network data (id={network["id"]})')
        net_details = json.loads(req.text)['data']
        if len(net_details)>1:
            print(net_details)

        net_details = net_details[0]

        # Push membership to wikidata
        today = self.wh.today()
        netixlan_ref = [
                (self.wh.get_pid('source'), self.wh.get_qid('PeeringDB')),
                (self.wh.get_pid('reference URL'), netixlan_url),
                (self.wh.get_pid('point in time'), today)
                ]

        for ixlan in net_details['netixlan_set']:
            ix_qid = self.ixid2qid[str(ixlan['ix_id'])]
            statements.append( [self.wh.get_pid('member of'), ix_qid, netixlan_ref] )

        # Update name, website, and organization for this network
        net_qid = self.net_qid(network) 
        self.wh.upsert_statements('update peeringDB networks', net_qid, statements )
        
        return net_qid


    def net_qid(self, network):
        """Find the network QID for the given network.
        If this network is not yet registered in the wikibase then find (or 
        create) the item corresponding to the network ASN and register 
        the peeringDB network ID with this item.

        Return the network QID."""

        # Check if the network is in the wikibase
        if str(network['id']) not in self.netid2qid :
            # Find or create the corresponding ASN item
            net_qid = self.wh.asn2qid(network['asn'], create=True)
            # Set properties for this new network
            net_qualifiers = [
                    (self.wh.get_pid('instance of'), self.wh.get_qid(NETID_LABEL)),
                    (self.wh.get_pid('reference URL'), URL_PDB_NETS),
                    (self.wh.get_pid('source'), self.wh.get_qid('PeeringDB'))
                    ]
            statements = [ [self.wh.get_pid('external ID'), str(network['id']), net_qualifiers] ]

            # Add this network to the wikibase
            self.wh.upsert_statements('add new peeringDB network', net_qid,
                    statements=statements)
            # keep track of this QID
            self.netid2qid[str(network['id'])] = net_qid

        return self.netid2qid[str(network['id'])]


# Main program
if __name__ == '__main__':
    pdbn = PDBNetworks()
    pdbn.run()

import sys
import logging
from datetime import datetime, time
from iyp import IYP
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json

ORG= 'PeeringDB'

# URL to peeringdb API for networks
URL_PDB_NETS = 'https://peeringdb.com/api/net'

# Label used for the class/item representing the network IDs
NETID_LABEL = 'PEERINGDB_NET_ID' 
# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PEERINGDB_ORG_ID' 
# Label used for the class/item representing the exchange point IDs
IXID_LABEL = 'PEERINGDB_IX_ID' 

class Crawler(object):
    def __init__(self):
        """Initialisation for pushing peeringDB IXPs to IYP"""
    
        self.reference = {
            'source': ORG,
            'reference_url': URL_PDB_NETS,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # connection to IYP database
        self.iyp = IYP()

        # Session object to fetch peeringdb data
        retries = Retry(total=5,
                backoff_factor=0.1,
                status_forcelist=[ 104, 500, 502, 503, 504 ])

        self.http_session = requests.Session()
        self.http_session.mount('https://', HTTPAdapter(max_retries=retries))

    def run(self):
        """Fetch networks information from PeeringDB and push to IYP."""

        req = self.http_session.get(URL_PDB_NETS)
        if req.status_code != 200:
            sys.exit('Error while fetching data from API')
        networks = json.loads(req.text)['data']

        for i, _ in enumerate(map(self.update_net, networks)):
            sys.stderr.write(f'\rProcessing... {i+1}/{len(networks)}')


    def update_net(self, network):
        """Add the network to wikibase if it's not already there and update its
        properties."""


        # set property name
        name_qid = self.iyp.get_node('NAME', {'name': network['name'].strip()}, create=True)
        statements = [ ['NAME', name_qid, self.reference] ] 

        # link to corresponding organization
        org_qid = self.iyp.get_node_extid(ORGID_LABEL, network['org_id'])
        if org_qid is not None:
            statements.append( ['MANAGED_BY', org_qid, self.reference])
        else:
            print('Error this organization is not in IYP: ',network['org_id'])

        # set property website
        if network['website']:
            website_qid = self.iyp.get_node('URL', {'url': network['website']}, create=True)
            statements.append( ['WEBSITE', website_qid, self.reference] )

        # Update IX membership
        # Fetch membership for this network
        netixlan_url = URL_PDB_NETS+f'/{network["id"]}'

        req = self.http_session.get(netixlan_url)
        if req.status_code != 200:
            sys.exit(f'Error while fetching network data (id={network["id"]})')

        net_details = json.loads(req.text)['data']
        if len(net_details)>1:
            print(net_details)

        net_details = net_details[0]

        # Push membership to IYP
        netixlan_ref = {
            'source': ORG,
            'reference_url': netixlan_url,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        for ixlan in net_details['netixlan_set']:
            ix_qid = self.iyp.get_node_extid(IXID_LABEL, ixlan['ix_id'])
            if ix_qid is None:
                print(f'Unknown IX: ix_id={ixlan["ix_id"]}')
                continue
            statements.append( ['MEMBER_OF', ix_qid, netixlan_ref] )

        netid_qid = self.iyp.get_node(NETID_LABEL, {'id': network['id']}, create=True)
        statements.append( ['EXTERNAL_ID', netid_qid,  self.reference] )

        # Add this network to the wikibase
        net_qid = self.iyp.get_node('AS', {'asn': network['asn']}, create=True)
        self.iyp.add_links( net_qid, statements)

        return net_qid




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
    logging.warning("Started: %s" % sys.argv)


    pdbn = Crawler()
    pdbn.run()

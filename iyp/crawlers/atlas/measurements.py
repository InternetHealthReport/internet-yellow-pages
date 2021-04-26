import sys
import logging
import requests
import json
from iyp.wiki.wikihandy import Wikihandy

# URL to Atlas probe informations
URL = 'https://atlas.ripe.net/api/v2/measurements/'

PROBEID_LABEL = 'RIPE Atlas probe ID'

class Crawler(object):
    def __init__(self):
        """
        """
    
        # Helper for wiki access
        self.wh = Wikihandy(preload=True)

        # Get the QID for RIPE Atlas
        self.atlas_qid = self.wh.get_qid('RIPE Atlas',
            create={                                    # Create it if it doesn't exist
                'summary': 'add RIPE Atlas',         # Commit message
                'description': 'RIPE Atlas is a global, open, distributed Internet measurement platform, consisting of thousands of measurement devices that measure Internet connectivity in real time.',    # Item description
                'aliases': 'Atlas|atlas',
                'statements': [ [self.wh.get_pid('managed by'), self.wh.get_qid('RIPE NCC')]]
                })

        # Get the QID for Atlas Probe
        self.atlas_probe_qid = self.wh.get_qid('Atlas probe',
            create={                                    # Create it if it doesn't exist
                'summary': 'add RIPE Atlas',         # Commit message
                'description': 'RIPE Atlas probes form the backbone of the RIPE Atlas infrastructure.',    # Item description
                'aliases': 'RIPE Atlas probe|atlas probe|RIPE atlas probe',
                'statements': [ [self.wh.get_pid('part of'), self.atlas_qid] ]
                })

        # Get the QID for Atlas Anchor
        self.atlas_anchor_qid = self.wh.get_qid('Atlas anchor',
            create={                                    # Create it if it doesn't exist
                'summary': 'add RIPE Atlas',         # Commit message
                'description': 'RIPE Atlas Anchors are located at hosts that can provide sufficient bandwidth to support a large number of incoming and outgoing measurements.',    # Item description
                'aliases': 'RIPE Atlas anchor|atlas anchor|RIPE atlas anchor',
                'statements': [ [self.wh.get_pid('part of'), self.atlas_qid] ]
                })

        # Get the QID of the item representing PeeringDB IX IDs
        self.probeid_qid = self.wh.get_qid(PROBEID_LABEL,
                create={                                                            # Create it if it doesn't exist
                    'summary': 'add RIPE Atlas probes',                             # Commit message
                    'description': 'Identifier for a probe in the RIPE Atlas measurement platform' # Description
                    })

        # Load the QIDs for probes already available in the wikibase
        self.probeid2qid = self.wh.extid2qid(qid=self.probeid_qid)

        # Added properties will have this additional information
        today = self.wh.today()
        self.reference = [
                (self.wh.get_pid('source'), self.wh.get_qid('RIPE NCC')),
                (self.wh.get_pid('reference URL'), URL),
                (self.wh.get_pid('point in time'), today)
                ]

        self.v4_qualifiers = [ 
                (self.wh.get_pid('IP version'), self.wh.get_qid('IPv4'))
                ]

        self.v6_qualifiers = [ 
                (self.wh.get_pid('IP version'), self.wh.get_qid('IPv6'))
                ]

    def run(self):
        """Fetch probe information from Atlas API and push to wikibase. """

        next_page = URL

        while next_page is not None: 
            req = requests.get(next_page)
            if req.status_code != 200:
                sys.exit('Error while fetching the blocklist')

            info = json.loads(req.text)
            next_page = info['next']

            for i, probe in enumerate( info['results']):

                self.update_probe(probe)
                sys.stderr.write(f'\rProcessed {i+1} probes')
            sys.stderr.write(f'\n')


    def update_probe(self, probe):
        """Add the probe to wikibase if it's not already there and update its
        properties."""

        # TODO add status, geometry (geo-location) and IPs?

        # Properties for this probe
        statements = []

        if probe['is_anchor']:
            statements.append([ self.wh.get_pid('instance of'), self.atlas_probe_qid])
            statements.append([ self.wh.get_pid('instance of'), self.atlas_anchor_qid])
        if probe['asn_v4']:
            as_qid = self.wh.asn2qid(probe['asn_v4'])
            if as_qid:
                statements.append([ self.wh.get_pid('part of'), as_qid, self.reference, self.v4_qualifiers ])
        if probe['asn_v6']:
            as_qid = self.wh.asn2qid(probe['asn_v6'])
            if as_qid:
                statements.append([ self.wh.get_pid('part of'), as_qid, self.reference, self.v6_qualifiers ])
        if probe['prefix_v4']:
            prefix_qid = self.wh.prefix2qid(probe['prefix_v4'])
            if prefix_qid:
                statements.append([ self.wh.get_pid('part of'), prefix_qid, self.reference ])
        if probe['prefix_v6']:
            prefix_qid = self.wh.prefix2qid(probe['prefix_v6'])
            if prefix_qid:
                statements.append([ self.wh.get_pid('part of'), prefix_qid, self.reference ])
        if probe['country_code']:
            statements.append([ self.wh.get_pid('country'), self.wh.country2qid(probe['country_code']), self.reference ])
        if probe['first_connected']:
            statements.append([ self.wh.get_pid('start time'), self.wh.to_wbtime(probe['first_connected']), self.reference ])

        if 'name' in probe['status']:
            # Get the QIDs for probes status
            status_qid = self.wh.get_qid(f'RIPE Atlas probe status: {probe["status"]["name"]}',
                create={                                    # Create it if it doesn't exist
                    'summary': 'add RIPE Atlas probe status',         # Commit message
                    })

            if probe['status_since']:
                statements.append([ self.wh.get_pid('status'), status_qid, self.reference, 
                    [ ( self.wh.get_pid('start time'), self.wh.to_wbtime(probe['status_since']) ) ]
                    ])

            # set end time if the probe is abandonned
            if probe['status']['name'] == 'Abandoned' and probe['status_since']:
                statements.append( [ self.wh.get_pid('end time'), self.wh.to_wbtime(probe['status_since']) ] )

        # Add probe tags
        for tag in probe['tags']:
            statements.append( [ self.wh.get_pid('tag'), 
                self.wh.get_qid(tag['name'], create={ 'summary': 'Add RIPE Atlas tag', })])

        # Commit to wikibase
        # Get the probe QID (create if probe is not yet registered) and commit changes
        probe_qid = self.probe_qid(probe)
        self.wh.upsert_statements('update from RIPE Atlas probes', probe_qid, statements )
        
    def probe_qid(self, probe):
        """Find the ix QID for the given probe ID.
        If this probe is not yet registered in the wikibase then add it.

        Return the probe QID."""

        id = str(probe['id'])

        # Check if the IX is in the wikibase
        if id not in self.probeid2qid :
            # Set properties for this new probe
            probeid_qualifiers = [
                    (self.wh.get_pid('instance of'), self.probeid_qid),
                    ]
            statements = [ 
                    (self.wh.get_pid('instance of'), self.atlas_probe_qid),
                    (self.wh.get_pid('external ID'), id, [],  probeid_qualifiers) ]

            # Add this probe to the wikibase
            probe_qid = self.wh.add_item('add new RIPE Atlas probe', 
                    label=f'RIPE Atlas probe #{id}', description=probe['description'], 
                    statements=statements)
            # keep track of this QID
            self.probeid2qid[id] = probe_qid

        return self.probeid2qid[id]

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

    crawler = Crawler()
    crawler.run()

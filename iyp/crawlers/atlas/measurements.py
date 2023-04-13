import sys
import logging
import requests
import json
import ipaddress
from iyp.wiki.wikihandy import Wikihandy

# URL to Atlas measurement informations
URL = 'https://atlas.ripe.net/api/v2/measurements/?optional_fields=probes'

PROBEID_LABEL = 'RIPE Atlas probe ID'
MSMID_LABEL = 'RIPE Atlas measurement ID'

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

        # Get the QID of the item representing Atlas measurement IDs
        self.msmid_qid = self.wh.get_qid(MSMID_LABEL,
                create={                                                            # Create it if it doesn't exist
                    'summary': 'add RIPE Atlas measurements',                             # Commit message
                    'description': 'Identifier for a measurement in the RIPE Atlas platform' # Description
                    })
        # Get the QID of the item representing Atlas probe IDs
        self.probeid_qid = self.wh.get_qid(PROBEID_LABEL)

        # Load the QIDs for probes available in the wikibase
        self.probeid2qid = self.wh.extid2qid(qid=self.probeid_qid)
        # Load the QIDs for measurement already available in the wikibase
        self.measurementid2qid = self.wh.extid2qid(qid=self.msmid_qid)

    def run(self):
        """Fetch probe information from Atlas API and push to wikibase. """

        next_page = URL

        while next_page is not None: 
            # Added properties will have this additional information
            today = self.wh.today()
            self.reference = [
                    (self.wh.get_pid('source'), self.wh.get_qid('RIPE NCC')),
                    (self.wh.get_pid('reference URL'), next_page),
                    (self.wh.get_pid('point in time'), today)
                    ]

            req = requests.get(next_page)
            if req.status_code != 200:
                sys.exit('Error while fetching the blocklist')

            info = json.loads(req.text)
            next_page = info['next']

            for i, msm in enumerate( info['results']):

                self.update_msm(msm)
                sys.stderr.write(f'\rProcessed {i+1} measurements')
            sys.stderr.write(f'\n')


    def update_msm(self, msm):
        """Add the measurement to wikibase if it's not already there."""

        # Properties for this measurement
        statements = []

        # Measurement type, protocol, and address family
        if msm['type']:
            # Get the QIDs for measurement type
            type_qid = self.wh.get_qid(f'RIPE Atlas {msm["type"]} measurement',
                create={                                    # Create it if it doesn't exist
                    'summary': 'add RIPE Atlas measurement type',         # Commit message
                    })
            statements.append([ self.wh.get_pid('instance of'), type_qid, self.reference])

        if 'protocol' in msm and msm['protocol']:
            # Get the QIDs for the protocol
            protocol = self.wh.get_qid(msm['protocol'].upper())
            if protocol is None:
                logging.error(f"protocol unknown: msm['protocol']")
                print(f"protocol unknown: msm['protocol']")
            else:
                statements.append([ self.wh.get_pid('uses'), protocol, self.reference])

        if msm['af']:
            if msm['af'] == 4:
                statements.append([ self.wh.get_pid('IP version'), self.wh.get_qid('IPv4'), self.reference])
            elif msm['af'] == 6:
                statements.append([ self.wh.get_pid('IP version'), self.wh.get_qid('IPv6'), self.reference])

        # Measurement target
        if msm['target']:
            statements.append( [ self.wh.get_pid('target'), self.target_qid(msm['target'], msm['target_prefix']), self.reference ])

        if not msm['resolve_on_probe'] and msm['target_ip']:
            statements.append( [ self.wh.get_pid('target'), self.target_qid(msm['target_ip'], msm['target_prefix']), self.reference ])
            

        # Measurement status
        if msm['status']:
            # Get the QIDs for measurement type
            status_qid = self.wh.get_qid(f'RIPE Atlas measurement status: {msm["status"]["name"]}',
                create={                                    # Create it if it doesn't exist
                    'summary': 'add RIPE Atlas measurement status',         # Commit message
                    })
            qualifier = []
            if msm['status']['when']:
                qualifier = [ ( self.wh.get_pid('point in time'), self.wh.to_wbtime(msm['status']['when']) ) ]
            statements.append([ self.wh.get_pid('instance of'), status_qid, self.reference, qualifier])

        # Measurement start and end time
        if msm['start_time']:
            statements.append([ self.wh.get_pid('start time'), self.wh.to_wbtime(msm['start_time']), self.reference ])
        if msm['stop_time']:
            statements.append([ self.wh.get_pid('stop time'), self.wh.to_wbtime(msm['stop_time']), self.reference ])

        # Measurement tags
        for tag in msm['tags']:
            statements.append( [ self.wh.get_pid('tag'), 
                self.wh.get_qid(tag['name'], create={ 'summary': 'Add RIPE Atlas tag', })])

        if msm['probes']:
            for probe in msm['probes']:
                probe_qid = self.probeid2qid(probe['id'])
                if probe_qid is not None:
                    statements.append( [ self.wh.get_pid('vantage point'), probe_qid])
                else:
                    logging.error(f'Unknown probe id: {probe["id"]}')

        # Commit to wikibase
        # Get the measurement QID (create if measurement is not yet registered) and commit changes
        msm_qid = self.msm_qid(msm)
        self.wh.upsert_statements('update from RIPE Atlas measurements', msm_qid, statements )
        
    def msm_qid(self, msm):
        """Find the measurement QID for the given probe ID.
        If this measurement is not yet registered in the wikibase then add it.

        Return the measurement QID."""

        id = str(msm['id'])

        # Check if the measurement is in the wikibase
        if id not in self.measurementid2qid :
            # Set properties for this new probe
            msmid_qualifiers = [
                    (self.wh.get_pid('instance of'), self.msmid_qid),
                    ]
            statements = [ 
                    (self.wh.get_pid('instance of'), self.msm_qid),
                    (self.wh.get_pid('external ID'), id, [],  msmid_qualifiers) ]

            # Add this measurement to the wikibase
            msm_qid = self.wh.add_item('add new RIPE Atlas measurement', 
                    label=f'RIPE Atlas measurement #{id}', description=msm['description'], 
                    statements=statements)
            # keep track of this QID
            self.measurementid2qid[id] = msm_qid

        return self.measurementid2qid[id]

    def target_qid(self, target, prefix):
        """Find the target QID if it exists, otherwise create the item for the 
        measurement target (an IP or a domain)."""

        # Prepare target statements (IP version, mapped/covering prefix)
        statements = []
        type = 'IP address'
        target_qid = self.wh.ip2qid(target, create=True)
        if target_qid is None:
            type = 'domain name'

        if type == 'domain name':
            target_qid = self.wh.domain2qid(target, create=True)
            if prefix:
                prefix_qid = self.wh.prefix2qid(prefix,create=True)
                statements.append( [self.wh.get_pid('forward DNS'), self.wh.get_qid(prefix_qid), self.reference] )
        else:
            if target:
                prefix_qid = self.wh.prefix2qid(prefix,create=True)
                statements.append( [self.wh.get_pid('part of'), self.wh.get_qid(prefix_qid), self.reference] )

        # Update target statements
        self.wh.upsert_statements(
                'Update RIPE Atlas measurement target',
                target_qid,
                statements
                )
            

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
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        crawler.unit_test(logging) 
    else :
        crawler.run()


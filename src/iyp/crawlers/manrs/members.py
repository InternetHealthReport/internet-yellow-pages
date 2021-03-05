import sys
import logging
import requests
from iyp.lib.wikihandy import Wikihandy

# URL to MANRS csv file
URL_MANRS = 'https://www.manrs.org/wp-json/manrs/v1/csv/4'

class MANRS(object):
    def __init__(self):
        """Fetch QIDs for MANRS actions (create them if they are not in the 
        wikibase)."""
    
        # Helper for wiki access
        self.wh = Wikihandy()

        # Actions defined by MANRS
        self.actions = [
              {
                'label': 'MANRS Action 1: Filtering',
                'description': 'Prevent propagation of incorrect routing information'
              },
              { 
                'label': 'MANRS Action 2: Anti-spoofing',
                'description': 'Prevent traffic with spoofed source IP addresses'
              },
              {
                'label': 'MANRS Action 3: Coordination',
                'description': 'Facilitate global operational communication and coordination'
              },
              {
                'label': 'MANRS Action 4: Global Validation',
                'description': 'Facilitate routing information on a global scale'
              }
            ]

        # Get the QID for the four items representing MANRS actions
        for action in self.actions:
            action['qid'] = self.wh.get_qid(action['label'],
                create={                                    # Create it if it doesn't exist
                    'summary': 'add MANRS actions',         # Commit message
                    'description': action['description']    # Item description
                    })

        # Added properties will have this additional information
        today = self.wh.today()
        self.reference = [
                (self.wh.get_pid('source'), self.wh.get_qid('MANRS')),
                (self.wh.get_pid('reference URL'), URL_MANRS),
                (self.wh.get_pid('point in time'), today)
                ]



    def run(self):
        """Fetch networks information from MANRS and push to wikibase. """

        req = requests.get(URL_MANRS)
        if req.status_code != 200:
            sys.exit('Error while fetching MANRS csv file')

        for i, row in enumerate( req.text.splitlines() ):
            # Skip the header
            if i == 0:
                continue

            self.update_net(row)
            sys.stderr.write(f'\rProcessed {i} organizations')


    def update_net(self, one_line):
        """Add the network to wikibase if it's not already there and update its
        properties."""

        _, areas, asns, act1, act2, act3, act4 = [col.strip() for col in one_line.split(',')]

        # Properties
        statements = [ 
                [self.wh.get_pid('member of'), self.wh.get_qid('MANRS'), self.reference],
                ] 

        # set countries
        for cc in areas.split(';'):
            statements.append([ self.wh.get_pid('country'), self.wh.country2qid(cc), self.reference])

        # set actions
        for i, action_bool in enumerate([act1, act2, act3, act4]):
            if action_bool == 'Yes':
                statements.append([ self.wh.get_pid('implements'), self.actions[i]['qid'], self.reference])

        # Commit to wikibase
        for asn in asns.split(';'):
            if asn:     # ignore organizations with no ASN
                # Get the AS QID (create if AS is not yet registered) and commit changes
                net_qid = self.wh.asn2qid(asn, create=True) 
                self.wh.upsert_statements('update from MANRS membership', net_qid, statements )
        
# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].rpartition('/')[2][0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    manrs = MANRS()
    manrs.run()

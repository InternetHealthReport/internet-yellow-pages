import sys
import logging
import requests
from iyp.lib.wikihandy import Wikihandy

# URL to spamhaus data
URL = 'https://www.spamhaus.org/drop/edrop.txt'

class Crawler(object):
    def __init__(self):
        """
        """
    
        # Helper for wiki access
        self.wh = Wikihandy(preload=True)

        # Get the QID for Spamhaus organization
        self.spamhaus_qid = self.wh.get_qid('Spamhaus',
            create={                                    # Create it if it doesn't exist
                'summary': 'add Spamhaus organization',         # Commit message
                'description': 'The Spamhaus Project is an international organisation to track email spammers and spam-related activity',    # Item description
                'aliases': 'The Spamhaus Project|the spamhaus project',
                'statements': [ [self.wh.get_pid('instance of'), self.wh.get_qid('organization')]]
                })

        # Get the QID for Spamhaus DROP project
        self.drop_qid = self.wh.get_qid('Spamhaus DROP lists',
            create={                                    # Create it if it doesn't exist
                'summary': 'add Spamhaus block list',         # Commit message
                'description': "The Spamhaus Don't Route Or Peer Lists",    # Item description
                'statements': [[self.wh.get_pid('managed by'), self.spamhaus_qid]]
                })

        # Get the QID for Spamhaus EDROP list
        self.drop_qid = self.wh.get_qid('Spamhaus EDROP list',
            create={                                    # Create it if it doesn't exist
                'summary': 'add Spamhaus block list',         # Commit message
                'description': 'EDROP is an extension of the DROP list that includes suballocated netblocks controlled by spammers or cyber criminals',
                'statements': [[self.wh.get_pid('managed by'), self.spamhaus_qid],
                                [self.wh.get_pid('part of'), self.drop_qid]]
                })

        # Added properties will have this additional information
        today = self.wh.today()
        self.reference = [
                (self.wh.get_pid('source'), self.spamhaus_qid),
                (self.wh.get_pid('reference URL'), URL),
                (self.wh.get_pid('point in time'), today)
                ]



    def run(self):
        """Fetch blocklist from Spamhaus and push to wikibase. """

        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching the blocklist')

        for i, row in enumerate( req.text.splitlines() ):
            # Skip the header
            if row.startswith(';'):
                continue

            self.update_net(row)
            sys.stderr.write(f'\rProcessed {i+1} prefixes')
        sys.stderr.write(f'\n')


    def update_net(self, one_line):
        """Add the prefix to wikibase if it's not already there and update its
        properties."""

        prefix, _, _ = one_line.partition(';')

        # Properties for this prefix
        statements = [ 
                [self.wh.get_pid('reported in'), self.drop_qid, self.reference],
                ] 


        # Commit to wikibase
        # Get the prefix QID (create if prefix is not yet registered) and commit changes
        net_qid = self.wh.prefix2qid(prefix, create=True) 
        self.wh.upsert_statements('update from Spamhaus EDROP list', net_qid, statements )
        
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

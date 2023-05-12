import logging
import sys

import requests

from iyp.wiki.wikihandy import Wikihandy

# URL to ASN Drop List
URL = 'https://www.spamhaus.org/drop/asndrop.txt'


class Crawler(object):
    def __init__(self):
        """"""

        # Helper for wiki access
        self.wh = Wikihandy(preload=True)

        # Get the QID for Spamhaus organization
        self.spamhaus_qid = self.wh.get_qid('Spamhaus',
                                            create={  # Create it if it doesn't exist
                                                # Commit message
                                                'summary': 'add Spamhaus organization',
                                                # Item description
                                                'description': 'The Spamhaus Project is an international organisation '
                                                               'to track email spammers and spam-related activity',
                                                'aliases': 'The Spamhaus Project|the spamhaus project',
                                                'statements': [[self.wh.get_pid('instance of'),
                                                                self.wh.get_qid('organization')]]
                                            })

        # Get the QID for Spamhaus DROP project
        self.drop_qid = self.wh.get_qid('Spamhaus DROP lists',
                                        create={  # Create it if it doesn't exist
                                            # Commit message
                                            'summary': 'add Spamhaus block list',
                                            # Item description
                                            'description': "The Spamhaus Don't Route Or Peer Lists",
                                            'statements': [[self.wh.get_pid('managed by'), self.spamhaus_qid]]
                                        })

        # Get the QID for Spamhaus ASN-DROP list
        self.asn_drop_qid = self.wh.get_qid('Spamhaus ASN-DROP list',
                                            create={  # Create it if it doesn't exist
                                                # Commit message
                                                'summary': 'add Spamhaus block list',
                                                # Item description
                                                'description': 'ASN-DROP contains a list of Autonomous System Numbers '
                                                               'controlled by spammers or cyber criminals, as well as '
                                                               '"hijacked" ASNs.',
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
        """Fetch blocklist from Spamhaus and push to wikibase."""

        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching the blocklist')

        for i, row in enumerate(req.text.splitlines()):
            # Skip the header
            if row.startswith(';'):
                continue

            self.update_net(row)
            sys.stderr.write(f'\rProcessed {i+1} ASes')
        sys.stderr.write('\n')

        self.iyp.close()

    def update_net(self, one_line):
        """Add the network to wikibase if it's not already there and update its
        properties."""

        asn, _, cc_name = one_line.partition(';')
        asn = int(asn[2:])
        cc, name = [word.strip() for word in cc_name.split('|')]

        # Properties for this AS
        statements = [
            [self.wh.get_pid('reported in'), self.asn_drop_qid, self.reference],
            [self.wh.get_pid('name'), name, self.reference],
        ]

        # set countries
        if len(cc) == 2:
            cc_qid = self.wh.country2qid(cc)
            if cc_qid is not None:
                statements.append([self.wh.get_pid('country'), cc_qid, self.reference])

        # Commit to wikibase
        # Get the AS QID (create if AS is not yet registered) and commit changes
        net_qid = self.wh.asn2qid(asn, create=True)
        self.wh.upsert_statements('update from Spamhaus ASN DROP list', net_qid, statements)


# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info('Started: %s' % sys.argv)

    crawler = Crawler()
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        crawler.unit_test(logging)
    else:
        crawler.run()

import sys
import logging
import requests
import socket
import csv
from zipfile import ZipFile
import io
import wikihandy

sys.path.append('../ip2asn/')
from ip2asn import ip2asn

# URL to Tranco top 1M
URL = 'https://tranco-list.eu/top-1m.csv.zip'

class Crawler(object):
    def __init__(self):
        """Fetch QIDs for MANRS actions (create them if they are not in the 
        wikibase)."""
    
        sys.stderr.write('Initialization...\n')
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        self.tranco_qid = self.wh.get_qid('Tranco Top 1M',
            create={                                    # Create it if it doesn't exist
                'summary': 'add Tranco ranking',         # Commit message
                'description': 'A Research-Oriented Top Sites Ranking Hardened Against Manipulation',    # Item description
                'statements': [
                    [self.wh.get_pid('website'), 'https://tranco-list.eu/'],
                    [self.wh.get_pid('publication'), 'https://tranco-list.eu/assets/tranco-ndss19.pdf'],
                    [self.wh.get_pid('source code repository'), 'https://github.com/DistriNet/tranco-list'],
                    ]
                })

        self.org_qid =  self.wh.get_qid('imec-DistriNet',
            create={                                    # Create it if it doesn't exist
                'summary': 'add Tranco ranking',         # Commit message
                'description': 'The imec-DistriNet research group is part of the Department of Computer Science at the KU Leuven and part of the imec High Impact Initiative Distributed Trust.',    # Item description
                'statements': [
                    [self.wh.get_pid('website'), 'https://distrinet.cs.kuleuven.be/'],
                    ]
                })

        # Added properties will have this additional information
        today = self.wh.today()
        self.reference = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), URL),
                (self.wh.get_pid('point in time'), today)
                ]

        # TODO use latest rib
        self.ia = ip2asn("../ip2asn/db/rib.20210201.pickle.bz2")


    def run(self):
        """Fetch Tranco top 1M and push to wikibase. """

        sys.stderr.write('Downloading latest list...\n')
        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching Tranco csv file')

        # open zip file and read top list
        with  ZipFile(io.BytesIO(req.content)) as z:
            with z.open('top-1m.csv') as list:
                for i, row in enumerate(io.TextIOWrapper(list)):
                    row = row.rstrip()
                    sys.stderr.write(f'\rProcessed {i} domains \t {row}')
                    self.update(row)


    def update(self, one_line):
        """Add the network to wikibase if it's not already there and update its
        properties."""

        rank, domain = one_line.split(',')

        # set rank
        statements = [
                [ self.wh.get_pid('ranking'), 
                    {
                        'amount': rank, 
                        'unit': self.tranco_qid,
                    },
                self.reference]
             ] 

        # Find corresponding AS
        try:
            ip = socket.gethostbyname(domain)
            asn = self.ia.ip2asn(ip)
            if asn > 0:
                asn_qid = self.wh.asn2qid(asn)
                statements.append( [ self.wh.get_pid('part of'), asn_qid] )
        except Exception as e:
            print(e)


        # Commit to wikibase
        # Get the domain name QID (create if it is not yet registered) and commit changes
        dn_qid = self.wh.get_qid(domain, create={
            'summary': 'add Tranco ranking',
            'statements': [
                [self.wh.get_pid('instance of'), self.wh.get_qid('domain name')] 
                ]}
            ) 
        self.wh.upsert_statements('update from tranco top 1M', dn_qid, statements )
        
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

    crawler = Crawler()
    crawler.run()

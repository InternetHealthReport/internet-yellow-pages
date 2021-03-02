import sys
import requests
from concurrent.futures import ThreadPoolExecutor
import wikihandy
from ftplib import FTP
import arrow

# URL to RIPE repository
URL_API = 'https://ftp.ripe.net/ripe/rpki/'
FTP_URL = 'ftp.ripe.net'
FTP_ROOT = 'ripe/rpki'

class Crawler(object):
    def __init__(self):
        """Initialize wikihandy and qualifiers for pushed data"""
    
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        # Added properties will have this additional information
        today = self.wh.today()
        self.org_qid = self.wh.get_qid('RIPE NCC')
        self.url = URL_API  # url will change for each country
        self.reference = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), self.url),
                (self.wh.get_pid('point in time'), today)
                ]

    def get_last_line(self,line):
        """Keep the end of the last given line"""

        self.last_line = line.rpartition(' ')[2]

    def get_all_lines(self, line):
        """Keep the end of each given lines"""

        self.all_lines.append(line.rpartition(' ')[2])

    def run(self):
        """Fetch data from RIPE and push to wikibase. """

        self.wh.login() # Login once for all threads
        pool = ThreadPoolExecutor(max_workers=4)

        # Find latest roa files
        filepaths = []
        ftp = FTP(FTP_URL)
        ftp.login()
        ftp.cwd(FTP_ROOT)

        self.all_lines = []
        self.last_line = ''
        ftp.retrlines('LIST', callback=self.get_all_lines)

        for dir in self.all_lines:
            path = ''
            while self.last_line != 'roas.csv':
                ftp.cwd(self.last_line)
                path += self.last_line + '/'
                ftp.retrlines('LIST', callback=self.get_last_line)

            path += 'roas.csv'
            filepaths.append(path)

        for filepath in filepaths:
            self.url = URL_API+filepath
            req = requests.get( self.url )
            if req.status_code != 200:
                sys.exit('Error while fetching data for '+cc)
            
            # Push data to wiki
            for i, res in enumerate(pool.map(self.update, req.text.splitlines() )):
                sys.stderr.write(f'\rProcessing {filepath}... {i+1} prefixes')

        pool.shutdown()

    def update(self, line):
        """Add the prefix to wikibase if it's not already there and update its
        properties."""

        uri, asn, prefix, max_length, start, end = line.split(',')
        
        # Skip header
        if uri=='URI':
            return
        
        # FIXME
        qualifiers = [
                # [self.wh.get_pid('start time'), self.wh.to_wbtime(start)],
                # [self.wh.get_pid('end time'), self.wh.to_wbtime(end)],
                # [self.wh.get_pid('maxLength'), ,{'amount': max_length} ] #
                ]

        # Properties
        asn_qid = self.wh.asn2qid(asn, create=True)
        if asn_qid is None:
            print('Error: ', line)
            return

        statements = [
                    [ self.wh.get_pid('route origin authorization'), 
                        asn_qid,
                        qualifiers+self.reference
                    ]
                ]

        # Commit to wikibase
        # Get the prefix QID (create if prefix is not yet registered) and commit changes
        prefix_qid = self.wh.prefix2qid(prefix, create=True) 
        self.wh.upsert_statements('update from RIPE RPKI data', prefix_qid, statements )
        
# Main program
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run()

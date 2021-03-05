import sys
import logging
import datetime
import requests
import wikihandy 
from concurrent.futures import ThreadPoolExecutor

URL_RIPE_AS_NAME = 'https://ftp.ripe.net/ripe/asnames/asn.txt'

class ASNames(object):
    def __init__(self):

        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        # Reference information for data pushed to the wikibase
        self.reference = [
            (self.wh.get_pid('source'), self.wh.get_qid('RIPE NCC')),
            (self.wh.get_pid('reference URL'), URL_RIPE_AS_NAME),
            (self.wh.get_pid('point in time'), self.wh.today())
            ]

    def run(self):
        """Fetch the AS name file from RIPE website and process lines one by one"""

        req = requests.get(URL_RIPE_AS_NAME)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')

        self.wh.login() # Login once for all threads, not needed with OAuth
        
        pool = ThreadPoolExecutor()
        for i, res in enumerate(pool.map(self.update_asn, req.text.splitlines())):
            sys.stderr.write(f'\rProcessed {i} ASes')
        pool.shutdown()
            

    def update_asn(self, one_line):
        # Parse given line to get ASN, name, and country code 
        asn, _, name_cc = one_line.partition(' ')
        name, _, cc = name_cc.rpartition(', ')

        asn_qid = self.wh.asn2qid(asn, create=True)
        cc_qid = self.wh.country2qid(cc, create=True)

        statements = []
        statements.append( [self.wh.get_pid('country'), cc_qid, self.reference] )  # Set country
        if cc_qid is not None:
            statements.append( [self.wh.get_pid('name'), name, self.reference] )       # Set AS name

        try:
            # Update AS name and country
            self.wh.upsert_statements('updates from RIPE AS names', asn_qid, statements)

        except Exception as error:
            # print errors and continue running
            print('Error for: ', one_line)
            print(error)

        return asn_qid

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

    asnames = ASNames()
    asnames.run()

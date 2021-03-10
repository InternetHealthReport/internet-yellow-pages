import sys
import os
import json
import progressbar
import logging
import requests
import gzip
from collections import defaultdict
import tldextract
from iyp.wiki.wikihandy import Wikihandy

sys.path.append('../../../../ip2asn/')
from ip2asn import ip2asn

# URL to Tranco top 1M
# TODO automatically fetch file
# TODO remove all data with URL regex
# TODO remove downloaded file
URL = 'https://opendata.rapid7.com/sonar.fdns_v2/2021-02-26-1614298023-fdns_a.json.gz'

def download_file(url, local_filename):
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename

class Crawler(object):
    def __init__(self):
        """Fetch QID for Rapid7 and initialize wikihandy."""
    
        sys.stderr.write('Initialization...\n')
        # Helper for wiki access
        self.wh = Wikihandy()

        self.org_qid = self.wh.get_qid('Rapid7',
            create={                                    # Create it if it doesn't exist
                'summary': 'add Rapid7 forward DNS data',         # Commit message
                'description': 'Rapid7, a security company that provides unified vulnerability management solutions',    # Item description
                'statements': [
                    [self.wh.get_pid('instance of'), self.wh.get_qid('organization')],
                    [self.wh.get_pid('website'), 'https://www.rapid7.com/'],
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

        # keep track of all resolved prefixes so we just make one push per
        # domain
        self.tld_pfx = defaultdict(set)


    def run(self):
        """Fetch Rapid7 DNS forward data, find corresponding BGP prefixes 
        and push resolution for domains already in the wikibase. """

        # download rapid7 data and find corresponding prefixes
        sys.stderr.write('Downloading latest dataset...\n')
        local_filename = url.split('/')[-1]
        if not os.path.exists(local_filename):
            fname = download_file(URL, local_filename)
        sys.stderr.write('Downloading latest dataset...\n')
        with gzip.open(fname, 'rt') as finput:
            for line in progressbar.progressbar(finput):
                datapoint = json.loads(line)
                if ( datapoint['type'] in ['a', 'aaaa'] 
                    and 'value' in datapoint
                    and 'name' in datapoint ):

                    ext = tldextract.extract(datapoint['name'])
                    tld = '.'.join(ext[:2]) 

                    # skip domains not in the wiki
                    if tld not in self.wh._label2id:
                        continue

                    pfx = self.ia.ip2prefix(datapoint['value'])
                    if pfx is None:
                        continue

                    self.tld_pfx[tld].add(pfx)

        # push data to wiki
        for tld, pfxs in self.tld_pfx.items():
            self.update(tld, pfxs)

    def update(self, tld, pfxs):
        """Update statements for the given domain name."""

        # make all statements
        statements = []
        for pfx in pfxs:
            pfx_qid = self.wh.prefix2qid(pfx, create=True)
            statements.append( 
                    [ self.wh.get_pid('forward DNS'), pfx_qid, self.reference]  
                    )

        # Commit to wikibase
        # Get the domain name QID  and commit changes
        dn_qid = self.wh.domain2qid(tld)
        # TODO remove old data with URL regex
        self.wh.upsert_statements('update from Rapid7 forward DNS data', dn_qid, statements )
        
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

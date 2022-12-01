import sys
import math
import logging
import requests
from iyp import BaseCrawler

# NOTE: this script is not adding new ASNs. It only adds links for existing ASNs

URL = 'https://ftp.ripe.net/pub/stats/ripencc/nro-stats/latest/nro-delegated-stats'
ORG = 'RIPE NCC'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch the delegated stat file from RIPE website and process lines one by one"""

        req = requests.get(URL, stream=True)
        if req.status_code != 200:
            sys.exit('Error while fetching delegated file')

        # Read delegated-stats file. see documentation:
        # https://www.nro.net/wp-content/uploads/nro-extended-stats-readme5.txt
        self.fields_name = ['registry', 'cc', 'type', 'start', 'value', 'date', 'status', 'opaque-id']

        for i, _ in enumerate(map(self.update, req.text.splitlines())):
            # commit every 10k lines
            if i % 10000 ==0:
                self.iyp.commit()

            sys.stderr.write(f'\rProcessed {i} lines')

        sys.stderr.write('\n')

    def update(self, line):

        # skip comments
        if line.strip().startswith('#'):
            return

        # skip version and summary lines
        fields_value = line.split('|')
        if len(fields_value) < 8:
            return

        # parse records
        rec = dict( zip(self.fields_name, fields_value))
        rec['value'] = int(rec['value'])

        statements = []
        self.reference['registry'] = rec['registry']

        # Add country statement
        cc_qid = self.iyp.get_node('COUNTRY', {'country_code': rec['cc']}, create=True)
        statements.append( ['COUNTRY', cc_qid, self.reference] )

        # Add opaque-id statement
        oid_qid = self.iyp.get_node('OPAQUE_ID', {'id': rec['opaque-id']}, create=True)
        statements.append( [rec['status'].upper(), oid_qid, self.reference] )

        # ASN records
        if rec['type'] == 'asn':
            rec['start'] = int(rec['start'])

            # Find all nodes corresponding to the range of ASN values
            # NOTE: this is not adding new AS nodes!
            for as_qid in self.iyp.tx.run(f"""MATCH (a:AS)
            WHERE a.asn >= {rec['start']} and a.asn < {rec['start']+rec['value']}
            RETURN ID(a)""").values():

                # Update AS 
                self.iyp.add_links(as_qid[0], statements)

        # prefix records
        elif rec['type'] == 'ipv4' or rec['type'] == 'ipv6':

            # compute prefix length
            prefix_len = rec['value']
            af = 6
            if rec['type'] == 'ipv4':
                prefix_len = int(32-math.log2(rec['value']))
                af = 4

            prefix = f"{rec['start']}/{prefix_len}"
            prefix_qid = self.iyp.get_node('PREFIX', {'prefix': prefix, 'af': af}, create=True)

            # Update prefix 
            self.iyp.add_links(prefix_qid, statements)

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

    asnames = Crawler(ORG, URL)
    asnames.run()
    asnames.close()


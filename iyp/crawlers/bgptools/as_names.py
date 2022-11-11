import sys
import logging
import requests
from datetime import datetime, time
from iyp import IYP

#curl -s https://bgp.tools/asns.csv | head -n 5
URL = 'https://bgp.tools/asns.csv'
ORG = 'BGP.Tools'

class Crawler(object):
    def __init__(self):

        self.headers = {
            'user-agent': 'IIJ/Internet Health Report - romain@iij.ad.jp'
        }

        # Reference information for data pushed to the wikibase
        self.reference = {
            'source': ORG,
            'reference_url': URL,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        # connection to IYP database
        self.iyp = IYP()


    def run(self):
        """Fetch the AS name file from BGP.Tools website and process lines one by one"""

        req = requests.get(URL, headers=self.headers)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')

        for i, _ in enumerate(map(self.update_asn, req.text.splitlines())):
            sys.stderr.write(f'\rProcessed {i} ASes')

        sys.stderr.write('\n')
        self.iyp.close()

    def update_asn(self, one_line):

        # skip header
        if one_line.startswith('asn'):
            return

        # Parse given line to get ASN, name, and country code 
        asn, _, name = one_line.partition(',')

        asn_qid = self.iyp.get_node('AS', {'asn': asn}, create=True)
        name_qid = self.iyp.get_node('NAME', {'name': name}, create=True)

        statements = [ [ 'NAME', name_qid, self.reference ] ] # Set AS name

        try:
            # Update AS name and country
            self.iyp.add_links(asn_qid, statements)

        except Exception as error:
            # print errors and continue running
            print('Error for: ', one_line)
            print(error)

        return asn_qid

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

    asnames = Crawler()
    asnames.run()

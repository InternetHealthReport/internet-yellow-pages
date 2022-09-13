import sys
import logging
import requests
import datetime
from iyp import IYP
# TODO move iyp to a better place
#from ...lib.wikihandy import Wikihandy

URL_RIPE_AS_NAME = 'https://ftp.ripe.net/ripe/asnames/asn.txt'

class Crawler(object):
    def __init__(self):

        # Reference information for data pushed to the wikibase
        self.reference = {
            'source': 'RIPE NCC',
            'reference_url': URL_RIPE_AS_NAME,
            'point_in_time': datetime.datetime.utcnow()
                .replace(hour=0, minute=0, second=0, microsecond=0)
            }

        # connection to IYP database
        self.iyp = IYP()


    def run(self):
        """Fetch the AS name file from RIPE website and process lines one by one"""

        req = requests.get(URL_RIPE_AS_NAME)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')

        for i, _ in enumerate(map(self.update_asn, req.text.splitlines())):
            sys.stderr.write(f'\rProcessed {i} ASes')

        sys.stderr.write('\n')
        self.iyp.close()

    def update_asn(self, one_line):
        # Parse given line to get ASN, name, and country code 
        asn, _, name_cc = one_line.partition(' ')
        name, _, cc = name_cc.rpartition(', ')

        asn_qid = self.iyp.get_node('AS', {'asn': asn}, create=True)
        cc_qid = self.iyp.get_node('COUNTRY', {'country_code': cc}, create=True)
        name_qid = self.iyp.get_node('NAME', {'name': name}, create=True)

        statements = []
        statements.append( ['COUNTRY', cc_qid, self.reference] )  # Set country
        if cc_qid is not None:
            statements.append( ['NAME', name_qid, self.reference] ) # Set AS name

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

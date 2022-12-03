import sys
import logging
import requests
from iyp import BaseCrawler

#curl -s https://bgp.tools/asns.csv | head -n 5
URL = 'https://bgp.tools/asns.csv'
ORG = 'BGP.Tools'

class Crawler(BaseCrawler):
    def __init__(self, organization, url):

        self.headers = {
            'user-agent': 'IIJ/Internet Health Report - admin@ihr.live'
        }

        super().__init__(organization, url)

    def run(self):
        """Fetch the AS name file from BGP.Tools website and process lines one by one"""

        req = requests.get(URL, headers=self.headers)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')

        for i, _ in enumerate(map(self.update_asn, req.text.splitlines())):
            sys.stderr.write(f'\rProcessed {i} ASes')

            # commit every 1k lines
            if i % 1000 == 0:
                self.iyp.commit()

        sys.stderr.write('\n')

    def update_asn(self, one_line):

        # skip header
        if one_line.startswith('asn'):
            return

        # Parse given line to get ASN, name, and country code 
        asn, _, name = one_line.partition(',')

        asn_qid = self.iyp.get_node('AS', {'asn': asn[2:]}, create=True)
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

    asnames = Crawler(ORG, URL)
    asnames.run()
    asnames.close()

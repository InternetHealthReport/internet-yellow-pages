import sys
import logging
import requests
from datetime import datetime, time, timezone
from iyp import BaseCrawler

#curl -s https://bgp.tools/asns.csv | head -n 5
URL = 'https://bgp.tools/tags/'
ORG = 'BGP.Tools'

TAGS = {
        'cdn': 'Content Delivery Network', 
        'dsl': 'Home ISP', 
        'a10k': 'Alexa 10k Host', 
        'icrit': 'Internet Critical Infra', 
        'tor': 'ToR Services', 
        'anycast': 'Anycast', 
        'perso': 'Personal ASN', 
        'ddosm': 'DDoS Mitigation',
        'vpn': 'VPN Host',
        'vpsh': 'Server Hosting',
        'uni': 'Academic',
        'gov': 'Government',
        'event': 'Event',
        'mobile': 'Mobile Data/Carrier',
        'satnet': 'Satellite Internet',
        'biznet': 'Business Broadband',
        'corp': 'Corporate/Enterprise'
       }

class Crawler(BaseCrawler):
    def __init__(self, organization, url):

        self.headers = {
            'user-agent': 'IIJ/Internet Health Report - admin@ihr.live'
        }

        super().__init__(organization, url)

    def run(self):
        """Fetch the AS name file from BGP.Tools website and process lines one by one"""

        for tag, label in TAGS.items():
            sys.stderr.write(f'{tag}:\n')

            url = URL+tag+'.csv'
            # Reference information for data pushed to the wikibase
            self.reference = {
                'reference_org': ORG,
                'reference_url': url,
                'reference_time': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
                }

            req = requests.get(url, headers=self.headers)
            if req.status_code != 200:
                print(req.text)
                sys.exit('Error while fetching AS names')

            self.tag_qid = self.iyp.get_node('TAG', {'label': label}, create=True)
            for i, _ in enumerate(map(self.update_asn, req.text.splitlines())):
                sys.stderr.write(f'\rProcessed {i} ASes')

            sys.stderr.write('\n')
        self.iyp.close()

    def update_asn(self, one_line):

        # skip header
        if one_line.startswith('asn'):
            return

        # Parse given line to get ASN, name, and country code 
        asn, _, _ = one_line.partition(',')

        asn_qid = self.iyp.get_node('AS', {'asn': asn[2:]}, create=True)

        statements = [ [ 'CATEGORIZED', self.tag_qid, self.reference ] ] # Set AS name

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

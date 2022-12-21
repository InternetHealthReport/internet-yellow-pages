import sys
import logging
import requests
from iyp import BaseCrawler

#curl -s https://bgp.tools/asns.csv | head -n 5
URL = 'https://bgp.tools/asns.csv'
ORG = 'BGP.Tools'
NAME = 'bgptools.as_names'

class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):

        self.headers = {
            'user-agent': 'IIJ/Internet Health Report - admin@ihr.live'
        }

        super().__init__(organization, url, name)

    def run(self):
        """Fetch the AS name file from BGP.Tools website and push it to IYP"""

        req = requests.get(URL, headers=self.headers)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')

        lines = []
        asns = set()
        names = set()

        # Collect all ASNs and names
        for line in req.text.splitlines():
            if line.startswith('asn,'):
                continue

            asn, _, name = line.partition(',')
            name = name.rpartition(',')[0]
            asn = int(asn[2:])
            asns.add(asn)
            names.add(name)
            lines.append( [asn, name] )

        # get ASNs and names IDs
        self.asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
        self.name_id = self.iyp.batch_get_nodes('NAME', 'name', names)

        # Compute links
        links = []
        for (asn, name) in lines:

            asn_qid = self.asn_id[asn] 
            name_qid = self.name_id[name]

            links.append( { 'src_id':asn_qid, 'dst_id':name_qid, 'props':[self.reference] } ) # Set AS name

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', links)


if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Start: %s" % sys.argv)

    asnames = Crawler(ORG, URL, NAME)
    asnames.run()
    asnames.close()

    logging.info("End: %s" % sys.argv)

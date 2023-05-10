import sys
import logging
import requests
from iyp import BaseCrawler

URL = 'https://ftp.ripe.net/ripe/asnames/asn.txt'
ORG = 'RIPE NCC'
NAME = 'ripe.as_names'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch the AS name file from RIPE website and process lines one by one"""

        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching AS names')

        lines = []
        asns = set()
        names = set()
        countries = set()

        # Read asn file  
        for line in req.text.splitlines():
            asn, _, name_cc = line.partition(' ')
            name, _, cc = name_cc.rpartition(', ')

            # Country codes are two digits
            if len(cc) > 2:
                print(cc)
                continue

            asn = int(asn)
            lines.append([ asn, name, cc ])

            asns.add( asn )
            names.add( name )
            countries.add( cc )
            

        # get node IDs for ASNs, names, and countries 
        asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
        name_id = self.iyp.batch_get_nodes('Name', 'name', names)
        country_id = self.iyp.batch_get_nodes('Country', 'country_code', countries)

        # Compute links
        name_links = []
        country_links = []

        for asn, name, cc in lines:
            asn_qid = asn_id[asn] 
            name_qid = name_id[name]
            country_qid = country_id[cc]

            name_links.append( { 'src_id':asn_qid, 'dst_id':name_qid, 
                                'props':[self.reference] } ) # Set AS name
            country_links.append( { 'src_id':asn_qid, 'dst_id':country_qid, 
                                   'props':[self.reference] } ) # Set country

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('COUNTRY', country_links)

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
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        asnames.unit_test(logging)
    else:
        asnames.run()
        asnames.close()
    logging.info("End: %s" % sys.argv)

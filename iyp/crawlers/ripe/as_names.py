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
            asn = int(asn)
            lines.append([ asn, name, cc ])

            asns.add( asn )
            names.add( name )
            countries.add( cc )
            

        # get node IDs for ASNs, names, and countries 
        logging.warning('getting node ids\n')
        asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
        logging.warning('getting node ids\n')
        name_id = self.iyp.batch_get_nodes('NAME', 'name', names)
        logging.warning('getting node ids\n')
        country_id = self.iyp.batch_get_nodes('COUNTRY', 'country_code', countries)

        # Compute links
        logging.warning('computing links\n')
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
        logging.warning('pushing links\n')
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
    asnames.run()
    asnames.close()

    logging.info("End: %s" % sys.argv)

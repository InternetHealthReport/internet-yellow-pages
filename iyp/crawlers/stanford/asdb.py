asdb.pyimport sys
import logging
import requests
import csv
from iyp import BaseCrawler
from get_latest_asdb_dataset import get_latest_asdb_dataset_url

# TODO automate the file date
INITIAL_URL = 'https://asdb.stanford.edu/#data'
URL = get_latest_asdb_dataset_url(INITIAL_URL, '%Y-%m_categorized_ases.csv')
ORG = 'Stanford'
NAME = 'stanford.asdb'

class Crawler(BaseCrawler):
    def run(self):
        """Fetch the ASdb file and push it to IYP"""

        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching ASdb')

        lines = []
        asns = set()
        categories = set()

        # Collect all ASNs and names
        for line in  csv.reader(req.text.splitlines(), quotechar='"', delimiter=',', skipinitialspace=True):
            if not line:
                continue

            if not line[0] or line[0] == 'ASN':
                continue

            asn = int(line[0][2:])
            cats = line[1:]
            for category in cats:
                if category:
                    asns.add(asn)
                    categories.add(category)

                    lines.append( [asn, category] )

        # get ASNs and names IDs
        asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns)
        category_id = self.iyp.batch_get_nodes('TAG', 'label', categories)

        # Compute links
        links = []
        for (asn, category) in lines:

            asn_qid = asn_id[asn] 
            category_qid = category_id[category]

            links.append( { 'src_id':asn_qid, 'dst_id':category_qid, 'props':[self.reference] } ) # Set AS category

        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)


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

    asdb = Crawler(ORG, URL, NAME)
    asdb.run()
    asdb.close()

    logging.info("End: %s" % sys.argv)

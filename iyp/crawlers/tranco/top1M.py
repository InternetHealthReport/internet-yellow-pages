import sys
import logging
import requests
from zipfile import ZipFile
import io
from iyp import BaseCrawler

# URL to Tranco top 1M
URL = 'https://tranco-list.eu/top-1m.csv.zip'
ORG = 'imec-DistriNet'
NAME = 'tranco.top1m'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch Tranco top 1M and push to IYP. """

        self.tranco_qid = self.iyp.get_node('RANKING', {'name': f'Tranco top 1M'}, create=True)

        sys.stderr.write('Downloading latest list...\n')
        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching Tranco csv file')

        links = []
        domains = set()
        # open zip file and read top list
        with  ZipFile(io.BytesIO(req.content)) as z:
            with z.open('top-1m.csv') as list:
                for i, row in enumerate(io.TextIOWrapper(list)):
                    row = row.rstrip()
                    rank, domain = row.split(',')

                    domains.add( domain )
                    links.append( { 'src_name':domain, 'dst_id':self.tranco_qid, 'props':[self.reference, {'rank': int(rank)}] } )

        name_id = self.iyp.batch_get_nodes('DOMAIN_NAME', 'name', domains)

        for link in links:
            link['src_id'] = name_id[link['src_name']]

        # Push all links to IYP
        self.iyp.batch_add_links('RANK', links)
        
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
    logging.info("Start: %s" % sys.argv)

    crawler = Crawler(ORG, URL, NAME)
    crawler.run()
    crawler.close()
    
    logging.info("End: %s" % sys.argv)

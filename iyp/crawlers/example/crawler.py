import sys
import logging
import requests
from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'Example Org'
URL = 'https://example.com/data.csv'

class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference

    def run(self):
        """Fetch data and push to IYP. """

        # Fetch data
        req = requests.get(self.reference['reference_url'])
        if req.status_code != 200:
            logging.error('Cannot download data {req.status_code}: {req.text}')
            sys.exit('Error while fetching data file')

        # Process line one after the other
        for i, _ in enumerate(map(self.update, req.text.splitlines())):
            sys.stderr.write(f'\rProcessed {i} lines')
        sys.stderr.write('\n')
    
    def update(self, one_line):
        """Add the entry to IYP if it's not already there and update its
        properties."""

        asn, value = one_line.split(',')

        # create node for value
        val_qid = self.iyp.get_node(
                'EXAMPLE_NODE_LABEL', 
                {'example_property':value},
                create=True
                )

        # set relationship
        statements = [[ 'EXAMPLE_RELATIONSHIP', val_qid, self.reference ]]

        # Commit to IYP
        # Get the AS's node ID (create if it is not yet registered) and commit changes
        as_qid = self.iyp.get_node('AS', {'asn': asn}, create=True) 
        self.iyp.add_links( as_qid, statements )
        
# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.WARNING, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    crawler = Crawler(ORG, URL)
    crawler.run()
    crawler.close()

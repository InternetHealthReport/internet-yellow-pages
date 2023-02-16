import flatdict
import logging
import sys
from iyp.crawlers.cloudflare.dns_top_locations import Crawler

# Organization name and URL to data
ORG = 'Cloudflare'
URL = 'https://api.cloudflare.com/client/v4/radar/dns/top/ases/'
NAME = 'cloudflare.dns_top_ases'

class Crawler(Crawler):
    
    def run(self):
        """Push data to IYP. """

        self.as_id = self.iyp.batch_get_nodes('AS', 'asn')

        super().run() 

    def update(self, param):
        """Save the domain name' top ases and corresponding properties."""

        domain, ases = param
        statements = []

        if domain == 'meta':
            return

        for entry in ases:
            asn = entry['clientASN']

            # set link
            entry['value'] = float(entry['value'])
            flat_prop = dict(flatdict.FlatDict(entry))
            statements.append([ 'QUERIED_FROM', self.as_id[asn], dict(flat_prop, **self.reference) ])

        self.iyp.add_links( self.domain_names_id[domain], statements )
        
        
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

    crawler = Crawler(ORG, URL, NAME)
    crawler.run()
    crawler.close()

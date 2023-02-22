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

    def compute_link(self, param):
        """Compute link for the domain name' top ases and corresponding properties."""

        domain, ases = param

        if domain == 'meta' or domain not in self.domain_names_id:
            return

        for entry in ases:
            asn = entry['clientASN']

            # set link
            entry['value'] = float(entry['value'])
            flat_prop = dict(flatdict.FlatDict(entry))
            self.statements.append([ self.domain_names_id[domain], self.as_id[asn], dict(flat_prop, **self.reference) ])

        
        
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

    logging.info("Ended")

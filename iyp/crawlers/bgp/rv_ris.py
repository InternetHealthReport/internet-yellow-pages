import sys
import logging
import arrow
from iyp.wiki.wikihandy import Wikihandy
import pybgpstream
import radix
from collections import defaultdict

# URL to original data
URL_RV = 'http://routeviews.org/{}/bgpdata/'
URL_RIS = 'http://data.ris.ripe.net/{}/'

class Crawler(object):
    def __init__(self):
        """
        """
   
        # Helper for wiki access
        self.wh = Wikihandy(preload=True)

        # Get the QID for Routeviews organization
        self.org_qid = self.wh.get_qid('Route Views')
        self.today = self.wh.today()


    def run(self):
        """Fetch BGP data from collectors and push to wikibase. """

        today = arrow.now().replace(hour=0, minute=0)
        start = today.shift(hours=-1)
        end = today.shift(hours=1)
        stream = pybgpstream.BGPStream(
            from_time=int(start.timestamp()), until_time=int(end.timestamp()),
            record_type="ribs",
        )

        rtree = radix.Radix()

        sys.stderr.write(f'\nReading BGP data:\n')
        for i, elem in enumerate(stream):
            # Extract the prefix and origin ASN
            msg = elem.fields
            prefix = msg['prefix']
            origin_asn_str = msg['as-path'].split(' ')[-1]
            origin_asns = []
            if '{' in origin_asn_str:
                origin_asns = origin_asn_str[1:-1].split(',')
            else:
                origin_asns = [origin_asn_str]

            # Store origin ASN in radix tree
            rnode = rtree.search_exact(prefix)
            if rnode is None:
                rnode = rtree.add(prefix)
                rnode.data['origin'] = defaultdict(set)

            for asn in origin_asns:
                rnode.data['origin'][asn].add(elem.collector)
                sys.stderr.write(f'\rProcessed {i+1} BGP messages')

        sys.stderr.write(f'\nPushing data to IYP...\n')

        # Push all prefixes data to IYP
        for i, rnode in enumerate(rtree):
            data = rnode.data['origin']
            self.update_entry(rnode.prefix, data)
            sys.stderr.write(f'\rProcessed {i+1} prefixes')


    def update_entry(self, prefix, originasn_collector):
        """Add the prefix to wikibase if it's not already there and update its properties."""

        statements = []

        # set origin AS
        for asn, collectors in originasn_collector.items():
            for collector in collectors:
                # Added properties will have this additional information
                url = URL_RV
                if 'rrc' in collector:
                    url = URL_RIS 

                self.reference = [
                        (self.wh.get_pid('source'), self.org_qid),
                        (self.wh.get_pid('reference URL'), url.format(collector)),
                        (self.wh.get_pid('point in time'), self.today)
                        ]

                as_qid = self.wh.asn2qid(asn, create=True) 
                statements.append( [self.wh.get_pid('originated by'), as_qid, self.reference]) 

        # Commit to wikibase
        # Get the prefix QID (create if prefix is not yet registered) and commit changes
        prefix_qid = self.wh.prefix2qid(prefix, create=True) 
        self.wh.upsert_statements('update from RIS/Routeviews RIBs', prefix_qid, statements )
        

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
    logging.info("Started: %s" % sys.argv)

    crawler = Crawler()
    crawler.run()

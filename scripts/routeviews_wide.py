import sys
from datetime import timedelta
import arrow
import requests
import csv
import wikihandy
import pybgpstream

# URL to original data
URL = 'http://routeviews.org/route-views.wide/bgpdata/'

class Crawler(object):
    def __init__(self):
        """
        """
   
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy(preload=True)

        # Get the QID for Routeviews organization
        self.org_qid = self.wh.get_qid('Route Views')

        # Added properties will have this additional information
        today = self.wh.today()
        self.reference = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), URL),
                (self.wh.get_pid('point in time'), today)
                ]



    def run(self):
        """Fetch BGP data from Routeviews and push to wikibase. """

        today = arrow.now().replace(hour=0, minute=0)
        start = today.shift(hours=-1)
        end = today.shift(hours=1)
        print(start.timestamp,',',end.timestamp)
        stream = pybgpstream.BGPStream(
            from_time=start.timestamp, until_time=end.timestamp,
            collectors=["route-views.wide"],
            record_type="ribs",
        )

        for i, elem in enumerate(stream):
            if elem.peer_asn == 2497:
                self.update_entry(elem.fields)
                sys.stderr.write(f'\rProcessed {i+1} prefixes')


    def update_entry(self, msg):
        """Add the prefix to wikibase if it's not already there and update its properties."""

        statements = []

        origin_as = msg['as-path'].split(' ')[-1]
        if '{' in origin_as:
            origin_as = origin_as[1:-1].split(',')
        else:
            origin_as = [origin_as]

        # set origin AS
        for asn in origin_as:
            as_qid = self.wh.asn2qid(asn, create=True) 
            statements.append( [self.wh.get_pid('announced by'), as_qid, self.reference]) 

        # Commit to wikibase
        # Get the prefix QID (create if prefix is not yet registered) and commit changes
        prefix_qid = self.wh.prefix2qid(msg['prefix'], create=True) 
        self.wh.upsert_statements('update from Routeviews RIB file', prefix_qid, statements )
        
# Main program
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run()
import sys
from datetime import timedelta
import arrow
import requests
import csv
import wikihandy
import pybgpstream

# URL to original data
URL = 'https://www.spamhaus.org/drop/asndrop.txt'

class Crawler(object):
    def __init__(self):
        """
        """
    
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy(preload=True)

        # Get the QID for Routeviews organization
        self.org_qid = self.wh.get_qid('Routeviews')

        # Added properties will have this additional information
        today = self.wh.today()
        self.reference = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), URL),
                (self.wh.get_pid('point in time'), today)
                ]



    def run(self):
        """Fetch blocklist from Spamhaus and push to wikibase. """

        today = arrow.now().replace(hour=0, minute=0)
        start = today.shift(hours=-1)
        end = today.shift(hours=1)
        print(start.timestamp,',',end.timestamp)
        stream = pybgpstream.BGPStream(
            from_time=start.timestamp, until_time=end.timestamp,
            collectors=["route-views.wide"],
            record_type="ribs",
        )

        for elem in stream:
            # record fields can be accessed directly from elem
            # e.g. elem.time
            # or via elem.record
            # e.g. elem.record.time
            print(elem)
            break
            sys.stderr.write(f'\rProcessed {i+1} ASes')


    def update_net(self, one_line):
        """Add the network to wikibase if it's not already there and update its
        properties."""

        asn, _, cc_name = one_line.partition(';')
        asn = int(asn[2:])
        cc, name = [word.strip() for word in cc_name.split('|')]

        # Properties for this AS
        statements = [ 
                [self.wh.get_pid('reported in'), self.asn_drop_qid, self.reference],
                [self.wh.get_pid('name'), name, self.reference],
                ] 

        # set countries
        if len(cc) == 2:
            cc_qid = self.wh.country2qid(cc)
            if cc_qid is not None:
                statements.append([ self.wh.get_pid('country'), cc_qid, self.reference])

        # Commit to wikibase
        # Get the AS QID (create if AS is not yet registered) and commit changes
        net_qid = self.wh.asn2qid(asn, create=True) 
        self.wh.upsert_statements('update from Spamhaus ASN DROP list', net_qid, statements )
        
# Main program
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run()

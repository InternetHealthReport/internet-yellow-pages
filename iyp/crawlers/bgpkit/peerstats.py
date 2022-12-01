import sys
from datetime import datetime, timedelta, time, timezone
import logging
import requests
from iyp import BaseCrawler
import bz2
import json

MAIN_PAGE = 'https://data.bgpkit.com/peer-stats/'
URL = 'https://data.bgpkit.com/peer-stats/{collector}/{year}/{month:02d}/peer-stats_{collector}_{year}-{month:02d}-{day:02d}_{epoch}.bz2'
ORG = 'BGPKIT'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch peer stats for each collector"""

        req = requests.get(MAIN_PAGE)
        if req.status_code != 200:
            logging.error(f'Cannot fetch peer-stats page {req.status_code}: req.text')
            sys.exit('Error while fetching main page')


        # Find all collectors
        collectors = []
        for line in req.text.splitlines():
            if line.startswith('<span class="name">') and line.endswith('/</span>'):
                collectors.append( line.partition('>')[2].partition('/')[0] )

        # Find latest date
        self.now = datetime.combine( datetime.utcnow(),time.min, timezone.utc)
        url = URL.format( collector='rrc10', year=self.now.year, 
                         month=self.now.month, day=self.now.day, 
                         epoch=int(self.now.timestamp()))

        # Check if today's data is available
        req = requests.head( url )
        if req.status_code != 200:
            self.now -= timedelta(days=1)
            logging.warning("Today's data not yet available!")

        for collector in collectors:
            url = URL.format( collector=collector, year=self.now.year, 
                            month=self.now.month, day=self.now.day, 
                            epoch=int(self.now.timestamp()))

            req = requests.get( url, stream=True )
            if req.status_code != 200:
                self.now -= timedelta(days=1)
                logging.warning(f"Data not available for {collector}")
                continue

            # keep track of collector and reference url
            stats = json.load(bz2.open(req.raw))
            self.collector_qid = self.iyp.get_node(
                    'BGP_COLLECTOR', 
                    {'name': stats['collector'], 'project': stats['project']},
                    create=True
                    )
            self.reference['reference_url'] = url

            for i, _ in enumerate(map(self.update_peer, stats['peers'].values())):
                sys.stderr.write(f'\rProcessed {i} peers')

        sys.stderr.write('\n')

    def update_peer(self, peer):

# {
#      "asn": 328474,
#      "ip": "102.67.56.1",
#      "num_connected_asns": 330,
#      "num_v4_pfxs": 919443,
#      "num_v6_pfxs": 0
#    }

        as_qid = self.iyp.get_node('AS', {'asn': peer['asn']}, create=True)

        statements = []
        statements.append( ['PEERS_WITH', self.collector_qid, dict(peer, **self.reference)] )  # Set relationship

        self.iyp.add_links(as_qid, statements)

        return as_qid

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

    asnames = Crawler(ORG, URL)
    asnames.run()
    asnames.close()


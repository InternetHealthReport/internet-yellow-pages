import sys
import logging
import requests
import json
from collections import defaultdict
import urllib3
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from iyp.wiki.wikihandy import Wikihandy

# URL to the API
URL_CONFIG = 'https://lg.de-cix.net/api/v1/config'
URL_RS = 'https://lg.de-cix.net/api/v1/routeservers'
URL_NEIGHBOR = 'https://lg.de-cix.net/api/v1/routeservers/{rs}/neighbors'
URL_ROUTE = 'https://lg.de-cix.net/api/v1/routeservers/{rs}/neighbors/{neighbor}/routes/received'
# Name of the organization providing the data
ORG = 'DE-CIX Management GmbH'

class Crawler(object):
    def __init__(self):
        """Initialize wikihandy """
    
        # Session object to fetch peeringdb data
        retries = Retry(total=10,
                backoff_factor=0.1,
                status_forcelist=[ 104, 500, 502, 503, 504 ])

        self.http_session = requests.Session()
        self.http_session.mount('https://', HTTPAdapter(max_retries=retries))

        # Helper for wiki access
        self.wh = Wikihandy()

        # Added properties will have this additional information
        self.org_qid = self.wh.get_qid(ORG)

        self.reference_config = [
            (self.wh.get_pid('source'), self.org_qid),
            (self.wh.get_pid('reference URL'), URL_RS),
            (self.wh.get_pid('point in time'), self.wh.today()),
            ]

        self.rs_config = self.fetch( URL_CONFIG )
        self.rs_asn_qid = self.wh.asn2qid(self.rs_config['asn'], create=True)


    def fetch(self, url):
        try:
            req = self.http_session.get( url )
        except urllib3.exceptions.MaxRetryError as e:
            logging.error(f"Error could not fetch: {url}")
            logging.error(e)
            return None

        if req.status_code != 200:
                return None
        return json.loads(req.text)


    def run(self):
        """Fetch data from API and push to wikibase. """

        routeservers = self.fetch(URL_RS)['routeservers']

        self.reference = [
            (self.wh.get_pid('source'), self.org_qid),
            (self.wh.get_pid('reference URL'), URL_RS),
            (self.wh.get_pid('point in time'), self.wh.today()),
            ]

        for rs in routeservers:
            # FIXME remove this: for now check only v6 not in FRA 
            if 'IPv6' not in rs['name'] or 'fra.de-cix' in rs['name']:
                continue

            sys.stderr.write(f'Processing route server {rs["name"]}\n')
            # Register/update route server 
            self.update_rs(rs)

            # route server neighbors
            self.url_neighbor = URL_NEIGHBOR.format(rs=rs['id'])
            self.reference_neighbor = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), self.url_neighbor),
                (self.wh.get_pid('point in time'), self.wh.today()),
                ]
            self.qualifier_neighbor = [ 
                (self.wh.get_pid('managed by'), self.rs_qid),
                    ]

            neighbors = self.fetch(self.url_neighbor)['neighbours']
            for neighbor in neighbors:
                # FIXME remove this: for now avoid HE
                if neighbor['asn'] == 6939:
                    continue

                sys.stderr.write(f'Processing neighbor {neighbor["id"]}\n')
                self.update_neighbor(neighbor)

                self.url_route = URL_ROUTE.format(rs=rs['id'], neighbor=neighbor['id'])
                self.reference_route = [
                    (self.wh.get_pid('source'), self.org_qid),
                    (self.wh.get_pid('reference URL'), self.url_route),
                    (self.wh.get_pid('point in time'), self.wh.today()),
                    ]

                asn_qid = self.wh.asn2qid(neighbor['asn'], create=True) 
                self.qualifier_route = [ 
                        (self.wh.get_pid('imported from'), asn_qid)
                        ]

                routes = self.fetch(self.url_route)
                if routes is None:
                    continue
                nb_pages = routes['pagination']['total_pages']
                # Imported routes
                for p in range(nb_pages):
                    # fetch all subsequent pages
                    if p != 0:
                        routes = self.fetch(self.url_route+f'?page={p}')

                    for i, route in enumerate(routes['imported']):
                        self.update_route(route,'imported')
                        sys.stderr.write(f'\rProcessing page {p+1}/{nb_pages} {i+1}/{len(routes["imported"])} routes')

                sys.stderr.write('\n')

    def update_route(self, route, status):
        """Update route data"""

        asn_qid = self.wh.asn2qid(route['bgp']['as_path'][-1], create=True) 
        # Properties
        statements = [ 
                [ self.wh.get_pid('appeared in'), self.rs_qid, 
                    self.reference_route, self.qualifier_route] ]
        statements.append( [self.wh.get_pid('announced by'), asn_qid, self.reference_route]) 
        prefix_qid = self.wh.prefix2qid(route['network'], create=True) 
        self.wh.upsert_statements('update from route server API', prefix_qid, statements)


    def update_neighbor(self, neighbor):
        """Update AS neighbor data"""

        # Properties
        statements = [ [ self.wh.get_pid('external ID'), neighbor['id'], 
            self.reference_neighbor, self.qualifier_neighbor] ]
        asn_qid = self.wh.asn2qid(neighbor['asn'], create=True) 
        self.wh.upsert_statements('update from route server API', asn_qid, statements)

    def update_rs(self, rs):
        """Update route server data or create if it's not already there"""

        # Properties
        statements = []

        # set ASN
        statements.append( [ self.wh.get_pid('autonomous system number'), str(self.rs_config['asn']) , self.reference_config])

        # set org
        statements.append( [ self.wh.get_pid('managed by'), self.org_qid, self.reference])

        # set IXP (assumes the rs['group'] is the same as peeringdb ix name)
        statements.append( [ self.wh.get_pid('part of'), self.wh.get_qid(rs['group']), self.reference])

        # set external id
        statements.append( [ self.wh.get_pid('external ID'), rs['id'], self.reference])

        # Commit to wikibase
        self.rs_qid = self.wh.get_qid( rs['name'], 
                create={  
                 'summary': 'add route server from Alice API',
                 'description': f"Route server in {rs['group']}",
                 'statements': [ [self.wh.get_pid('instance of'), self.wh.get_qid('route server')] ]
                 })
        self.wh.upsert_statements('update from route server API', self.rs_qid, statements)

        
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

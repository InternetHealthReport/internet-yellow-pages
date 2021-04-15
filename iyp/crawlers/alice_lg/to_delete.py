import sys
import logging
import requests
import json
from collections import defaultdict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from iyp.wiki.wikihandy import Wikihandy
from iyp.tools.ip2plan import ip2plan

# TODO do prefix lookups before updating wiki 
# (e.g. https://lg.de-cix.net/api/v1/lookup/prefix?q=217.115.0.0)
# so we can add all information at once.
# TODO keep track of which prefixes have been added and skip the lookup for
# already seen prefixes
# TODO keep track of prefixes per routeserver so we might not even have to do
# the neighbor query if we already saw all exported prefixes (e.g. google)

class Crawler(object):
    def __init__(self, url):
        """Initialize wikihandy and http session.
        url is the API endpoint (e.g. https://lg.de-cix.net/api/v1/)"""
    
        # URLs to the API
        self.urls = { 
            'config':  url+'/config',
            'routeservers':  url+'/routeservers',
            'routes':  url+'/routeservers/{rs}/neighbors/{neighbor}/routes/received',
            'neighbors':  url+'/routeservers/{rs}/neighbors'
            }

        # Session object to fetch peeringdb data
        retries = Retry(total=10,
                backoff_factor=0.1,
                status_forcelist=[ 104, 500, 502, 503, 504 ])

        self.http_session = requests.Session()
        self.http_session.mount('https://', HTTPAdapter(max_retries=retries))

        # Helper for wiki access
        self.wh = Wikihandy()

        self.rs_config = self.fetch( self.urls['config'] )
        self.rs_asn_qid = self.wh.asn2qid(self.rs_config['asn'], create=True)

        self.ip2plan = ip2plan(self.wh)

    def fetch(self, url):
        try:
            req = self.http_session.get( url )
        except requests.exceptions.RetryError as e:
            logging.error(f"Error could not fetch: {url}")
            logging.error(e)
            return None

        if req.status_code != 200:
                return None
        return json.loads(req.text)


    def run(self):
        """Fetch data from API and push to wikibase. """

        routeservers = self.fetch(self.urls['routeservers'])['routeservers']

        # For each routeserver
        for rs in routeservers:
            sys.stderr.write(f'Processing route server {rs["name"]}\n')

            # route server neighbors
            self.url_neighbor = self.urls['neighbors'].format(rs=rs['id'])
            neighbors = self.fetch(self.url_neighbor)['neighbours']

            # find corresponding IXP
            self.org_qid = None
            self.ix_qid = None

            # Find the peering LAN using neighbors IP addresses
            for neighbor in neighbors:
                peering_lan = self.ip2plan(neighbor['address'])
                if peering_lan is not None:
                    self.org_qid = peering_lan['org_qid']
                    self.ix_qid = peering_lan['ix_qid']
                    break

            if self.org_qid is None:
                logging.error(f'Could not find the IXP/organization corresponding to routeserver {rs}.')
                logging.error("Is the IXP's peering LAN registered in IYP or PeeringDB?")
                continue

            # Register/update route server 
            self.reference = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), self.urls['routeservers']),
                (self.wh.get_pid('point in time'), self.wh.today()),
                ]
            self.update_rs(rs)

            self.reference_neighbor = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), self.url_neighbor),
                (self.wh.get_pid('point in time'), self.wh.today()),
                ]
            self.qualifier_neighbor = [ 
                (self.wh.get_pid('managed by'), self.rs_qid),
                    ]

            for neighbor in neighbors:

                sys.stderr.write(f'Processing neighbor {neighbor["id"]}\n')
                self.update_neighbor(neighbor)

                self.url_route = self.urls['routes'].format(rs=rs['id'], neighbor=neighbor['id'])
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
        statements.append( [self.wh.get_pid('originated by'), asn_qid, self.reference_route]) 
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

        reference_config = [
            (self.wh.get_pid('source'), self.org_qid),
            (self.wh.get_pid('reference URL'), self.urls['routeservers']),
            (self.wh.get_pid('point in time'), self.wh.today()),
            ]

        # Properties
        statements = []

        # set ASN
        statements.append( [ self.wh.get_pid('autonomous system number'), str(self.rs_config['asn']) , reference_config])

        # set org
        statements.append( [ self.wh.get_pid('managed by'), self.org_qid, self.reference])

        # set IXP 
        statements.append( [ self.wh.get_pid('part of'), self.ix_qid, self.reference])

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
    URL = 'https://lg.de-cix.net/api/v1/'


    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    crawler = Crawler(URL)
    crawler.run()

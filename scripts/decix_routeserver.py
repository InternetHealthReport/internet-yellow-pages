import sys
import requests
import json
from collections import defaultdict
import wikihandy

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
    
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

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
        req = requests.get( url )
        if req.status_code != 200:
            print('Error while fetching data: ', url)
            return defaultdict(list)
        return json.loads(req.text)


    def run(self):
        """Fetch data from API and push to wikibase. """

        self.wh.login() # Login once for all threads

        routeservers = self.fetch(URL_RS)['routeservers']

        self.reference = [
            (self.wh.get_pid('source'), self.org_qid),
            (self.wh.get_pid('reference URL'), URL_RS),
            (self.wh.get_pid('point in time'), self.wh.today()),
            ]

        for rs in routeservers:
            sys.stderr.write(f'Processing route server {rs["name"]}\n')
            # Register/update route server 
            self.update_rs(rs)

            # route server neighbors
            self.url_neighbor = URL_NEIGHBOR.format(rs=rs['id'])
            self.reference_neighbor = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('managed by'), self.rs_qid),
                (self.wh.get_pid('reference URL'), self.url_neighbor),
                (self.wh.get_pid('point in time'), self.wh.today()),
                ]

            neighbors = self.fetch(self.url_neighbor)['neighbours']
            for neighbor in neighbors:
                sys.stderr.write(f'Processing neighbor {neighbor["id"]}\n')
                self.update_neighbor(neighbor)

                self.url_route = URL_ROUTE.format(rs=rs['id'], neighbor=neighbor['id'])
                self.reference_route = [
                    (self.wh.get_pid('source'), self.org_qid),
                    (self.wh.get_pid('reference URL'), self.url_route),
                    (self.wh.get_pid('point in time'), self.wh.today()),
                    ]

                routes = self.fetch(self.url_route)
                # Imported routes
                for i, route in enumerate(routes['imported']):
                    self.update_route(route,'imported')
                    sys.stderr.write(f'\rProcessing... {i+1}/{len(routes["imported"])}')

                sys.stderr.write('\n')

    def update_route(self, route, status):
        """Update route data"""

        asn_qid = self.wh.asn2qid(route['bgp']['as_path'][-1], create=True) 
        # Properties
        statements = [ [ self.wh.get_pid('appeared in'), self.rs_qid, self.reference_route] ]
        statements.append( [self.wh.get_pid('announced by'), asn_qid, self.reference_route]) 
        prefix_qid = self.wh.prefix2qid(route['network'], create=True) 
        self.wh.upsert_statements('update from route server API', prefix_qid, statements)


    def update_neighbor(self, neighbor):
        """Update AS neighbor data"""

        # Properties
        statements = [ [ self.wh.get_pid('external ID'), neighbor['id'], self.reference_neighbor] ]
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
    crawler = Crawler()
    crawler.run()

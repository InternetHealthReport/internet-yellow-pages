import sys
import logging
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from iyp.lib.wikihandy import Wikihandy
import iso3166

# URL to APNIC API
URL_API = 'http://v6data.data.labs.apnic.net/ipv6-measurement/Economies/'
MIN_POP_PERC = 0.01 # ASes with less population will be ignored

class APNICeyeball(object):
    def __init__(self):
        """Initialize wikihandy and qualifiers for pushed data"""
    
        # Helper for wiki access
        self.wh = Wikihandy()

        # Added properties will have this additional information
        today = self.wh.today()
        self.apnic_qid = self.wh.get_qid('APNIC')
        self.url = URL_API  # url will change for each country
        self.reference = [
                (self.wh.get_pid('source'), self.apnic_qid),
                (self.wh.get_pid('reference URL'), self.url),
                (self.wh.get_pid('point in time'), today)
                ]

        self.countries = iso3166.countries_by_alpha2

    def run(self):
        """Fetch data from APNIC and push to wikibase. """

        self.wh.login() # Login once for all threads
        pool = ThreadPoolExecutor()

        for cc, country in self.countries.items():

            # Get the QID of the selected country / create this country if needed
            self.countryrank_qid = self.wh.get_qid(f'APNIC eyeball estimates ({cc})',
                create={                        # Create it if it doesn't exist
                    'summary': 'add APNIC eyeball estimates for '+cc,       
                    'description': "APNIC's AS population estimates"
                                +"based on advertisement for "+country.name,
                    'statements': [[self.wh.get_pid('managed by'), self.apnic_qid],
                                [self.wh.get_pid('website'), URL_API ],
                                [self.wh.get_pid('country'), self.wh.country2qid(cc) ],
                                ]
                    })

            self.countrypercent_qid = self.wh.get_qid(f'% of Internet users in {country.name}',
                create={                        # Create it if it doesn't exist
                    'summary': 'add APNIC eyeball estimates for '+cc,       
                    'description': "APNIC's AS population estimates"
                                +"based on advertisement for "+country.name,
                    'statements': [[self.wh.get_pid('managed by'), self.apnic_qid],
                                [self.wh.get_pid('website'), URL_API ],
                                [self.wh.get_pid('country'), self.wh.country2qid(cc) ],
                                ]
                    })


            self.url = URL_API+f'{cc}/{cc}.asns.json?m={MIN_POP_PERC}'
            req = requests.get( self.url )
            if req.status_code != 200:
                sys.exit('Error while fetching data for '+cc)
            
            ranking = json.loads(req.text)
            # Make sure the ranking is sorted and add rank field
            ranking.sort(key=lambda x: x['percent'], reverse=True)
            for i, asn in enumerate(ranking):
                asn['rank']=i 


            # Push data to wiki
            for i, res in enumerate(pool.map(self.update_net, ranking)):
                sys.stderr.write(f'\rProcessing {country.name}... {i+1}/{len(ranking)}')

        pool.shutdown()

    def update_net(self, asn):
        """Add the network to wikibase if it's not already there and update its
        properties."""
        
        # Properties
        statements = []

        # set name
        if asn['autnum']:
                statements.append([self.wh.get_pid('name'), asn['autnum'], self.reference])

        # set country
        if asn['cc']:
            statements.append(
                    [ self.wh.get_pid('country'), self.wh.country2qid(asn['cc']), self.reference])

        # set rank
        statements.append(
                [ self.wh.get_pid('ranking'), 
                    { 
                    'amount': asn['rank'], 
                    'unit': self.countryrank_qid,
                    },
                    self.reference])

        # set population
        statements.append(
                [ self.wh.get_pid('population'), 
                    { 
                    'amount': asn['percent'], 
                    'unit': self.countrypercent_qid,
                    },
                    self.reference])

        # Commit to wikibase
        # Get the AS QID (create if AS is not yet registered) and commit changes
        net_qid = self.wh.asn2qid(asn['as'], create=True) 
        self.wh.upsert_statements('update from APNIC eyeball ranking', net_qid, statements )
        
# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].rpartition('/')[2][0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    apnic = APNICeyeball()
    apnic.run()

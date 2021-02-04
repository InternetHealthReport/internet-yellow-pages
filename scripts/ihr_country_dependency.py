import sys
import requests
import json
from concurrent.futures import ThreadPoolExecutor
import wikihandy
import iso3166

# URL to the API
URL_API = 'https://ihr.iijlab.net/ihr/api/hegemony/countries/?country={country}&af=4'
# Name of the organization providing the data
ORG = 'Internet Health Report'
MIN_HEGE = 0.01

class Crawler(object):
    def __init__(self):
        """Initialize wikihandy """
    
        # Helper for wiki access
        self.wh = wikihandy.Wikihandy()

        # Added properties will have this additional information
        self.org_qid = self.wh.get_qid(ORG)
        self.countries = iso3166.countries_by_alpha2

    def run(self):
        """Fetch data from API and push to wikibase. """

        self.wh.login() # Login once for all threads
        pool = ThreadPoolExecutor()

        for cc, country in self.countries.items():
            today = self.wh.today()
            self.url = URL_API.format(country=cc)
            req = requests.get( self.url )
            if req.status_code != 200:
                sys.exit('Error while fetching data for '+cc)
            data = json.loads(req.text)
            ranking = data['results']

            self.reference = [
                (self.wh.get_pid('source'), self.org_qid),
                (self.wh.get_pid('reference URL'), self.url),
                (self.wh.get_pid('point in time'), today),
                ]

            country_qid = self.wh.get_qid(country.name)
            if country_qid is not None:
                self.reference.append( (self.wh.get_pid('country'), country_qid) )

            for metric, weight in [('Total eyeball', 'eyeball'), ('Total AS', 'as')]:

                # Get the QID of the selected country / create this country if needed
                self.countryrank_qid = self.wh.get_qid(f'IHR country ranking: {metric} ({cc})',
                    create={                        # Create it if it doesn't exist
                        'summary': f'add IHR {metric} ranking for '+cc,       
                        'description': f"IHR's ranking of networks ({metric}) for "+country.name,
                        'statements': [[self.wh.get_pid('managed by'), self.org_qid]]
                        })


                # Filter out unnecessary data
                selected = [r for r in ranking 
                        if r['weightscheme']==weight and r['transitonly']==False and r['hege']>MIN_HEGE]

                # Make sure the ranking is sorted and add rank field
                selected.sort(key=lambda x: x['hege'], reverse=True)
                for i, asn in enumerate(selected):
                    asn['rank']=i 


                # Push data to wiki
                for i, res in enumerate(pool.map(self.update_entry, selected)):
                    sys.stderr.write(f'\rProcessing {country.name}... {i+1}/{len(selected)}')

                sys.stderr.write('\n')

        pool.shutdown()

    def update_entry(self, asn):
        """Add the network to wikibase if it's not already there and update its
        properties."""
        
        # Properties
        statements = []

        # set rank
        statements.append(
                [ self.wh.get_pid('ranking'), 
                    { 
                    'amount': asn['rank'], 
                    'unit': self.countryrank_qid,
                    },
                    self.reference])

        # Commit to wikibase
        # Get the AS QID (create if AS is not yet registered) and commit changes
        net_qid = self.wh.asn2qid(asn['asn'], create=True) 
        self.wh.upsert_statements('update from IHR country ranking', net_qid, statements )
        
# Main program
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run()

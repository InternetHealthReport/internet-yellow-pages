import sys
import logging
import requests
import iso3166
from datetime import datetime, time
from iyp import IYP

# URL to APNIC API
URL = 'http://v6data.data.labs.apnic.net/ipv6-measurement/Economies/'
ORG = 'APNIC'
MIN_POP_PERC = 0.01 # ASes with less population will be ignored

class Crawler(object):
    def __init__(self):
        """Initialize wikihandy and qualifiers for pushed data"""
    

        # Added properties will have this additional information
        self.url = URL  # url will change for each country
        self.reference = {
            'source': ORG,
            'reference_url': URL,
            'point_in_time': datetime.combine(datetime.utcnow(), time.min)
            }

        self.countries = iso3166.countries_by_alpha2

        # connection to IYP database
        self.iyp = IYP()

    def run(self):
        """Fetch data from APNIC and push to wikibase. """

        for cc, country in self.countries.items():

            # Get the QID of the country and corresponding ranking
            self.cc_qid = self.iyp.get_node('COUNTRY', {'country_code': cc}, create=True)
            self.ranking_qid = self.iyp.get_node('RANKING', {'name': f'APNIC eyeball estimates ({cc})'}, create=True)
            statements = [ ['COUNTRY', self.cc_qid, self.reference] ]
            self.iyp.add_links(self.ranking_qid, statements)

            self.url = URL+f'{cc}/{cc}.asns.json?m={MIN_POP_PERC}'
            req = requests.get( self.url )
            if req.status_code != 200:
                sys.exit('Error while fetching data for '+cc)
            
            ranking = req.json()
            # Make sure the ranking is sorted and add rank field
            ranking.sort(key=lambda x: x['percent'], reverse=True)
            for i, asn in enumerate(ranking):
                asn['rank']=i 

            # Push data to iyp
            for i, _ in enumerate(map(self.update_net, ranking)):
                sys.stderr.write(f'\rProcessing {country.name}... {i+1}/{len(ranking)}')

        self.iyp.close()

    def update_net(self, asn):
        """Add the network to wikibase if it's not already there and update its
        properties."""
        
        # Properties
        statements = []

        # set name
        if asn['autnum']:
            name_qid = self.iyp.get_node('NAME', {'name': asn['autnum']}, create=True) 
            statements.append(['NAME', name_qid, self.reference])

        # set country (APNIC suggest the AS has eyeball in this country)
        if asn['cc']:
            statements.append(
                    [ 'COUNTRY', self.cc_qid, self.reference])

        # set rank
        statements.append( [ 
                            'RANK',
                            self.ranking_qid,
                            dict({ 'rank': asn['rank'] }, **self.reference) 
                            ])

        # set population
        statements.append( [ 
                            'POPULATION',
                            self.cc_qid,
                            dict({ 'percent': asn['percent'] }, **self.reference),
                            ])

        # Commit to iyp
        # Get the AS QID (create if AS is not yet registered) and commit changes
        as_qid = self.iyp.get_node('AS', {'asn': str(asn['as'])}, create=True) 
        self.iyp.add_links( as_qid, statements )
        
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

    apnic = Crawler()
    apnic.run()

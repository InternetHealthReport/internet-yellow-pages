import sys
import logging
import requests
import iso3166
from iyp import BaseCrawler

# URL to APNIC API
URL = 'http://v6data.data.labs.apnic.net/ipv6-measurement/Economies/'
ORG = 'APNIC'
NAME = 'apnic.eyeball'
MIN_POP_PERC = 0.01 # ASes with less population will be ignored

class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialize IYP and list of countries"""

        self.url = URL  # url will change for each country
        self.countries = iso3166.countries_by_alpha2
        super().__init__(organization, url, name)

    def run(self):
        """Fetch data from APNIC and push to IYP. """

        processed_asn = set()

        for cc, country in self.countries.items():
            logging.info(f'processing {country}')

            # Get the QID of the country and corresponding ranking
            cc_qid = self.iyp.get_node('Country', {'country_code': cc}, create=True)
            ranking_qid = self.iyp.get_node('Ranking', {'name': f'APNIC eyeball estimates ({cc})'}, create=True)
            statements = [ ['COUNTRY', cc_qid, self.reference] ]
            self.iyp.add_links(ranking_qid, statements)

            self.url = URL+f'{cc}/{cc}.asns.json?m={MIN_POP_PERC}'
            req = requests.get( self.url )
            if req.status_code != 200:
                sys.exit('Error while fetching data for '+cc)
            
            asns = set()
            names = set()
             
            ranking = req.json()
            logging.info(f'{len(ranking)} eyeball ASes')

            # Collect all ASNs and names
            # and make sure the ranking is sorted and add rank field
            ranking.sort(key=lambda x: x['percent'], reverse=True)
            for i, asn in enumerate(ranking):
                asn['rank']=i 
                asns.add(asn['as'])
                names.add(asn['autnum'])

            # Get node IDs
            self.asn_id = self.iyp.batch_get_nodes('AS', 'asn', asns, all=False)
            self.name_id = self.iyp.batch_get_nodes('Name', 'name', names, all=False)

            # Compute links
            country_links = []
            rank_links = []
            pop_links = []
            name_links = []
            for asn in ranking:
                asn_qid = self.asn_id[asn['as']] #self.iyp.get_node('AS', {'asn': asn[2:]}, create=True)

                if asn['as'] not in processed_asn:
                    name_qid = self.name_id[asn['autnum']] #self.iyp.get_node('Name', {'name': name}, create=True)
                    name_links.append( {'src_id': asn_qid, 'dst_id': name_qid, 'props':[self.reference]} )
                    country_links.append( {'src_id': asn_qid, 'dst_id': cc_qid, 'props':[self.reference]} )

                    processed_asn.add(asn['as'])

                rank_links.append( {'src_id': asn_qid, 'dst_id': ranking_qid, 'props':[self.reference, asn]} )
                pop_links.append( {'src_id': asn_qid, 'dst_id': cc_qid, 'props':[self.reference, asn]} )

            # Push all links to IYP
            self.iyp.batch_add_links('NAME', name_links)
            self.iyp.batch_add_links('COUNTRY', country_links)
            self.iyp.batch_add_links('RANK', rank_links)
            self.iyp.batch_add_links('POPULATION', pop_links)

        
# Main program
if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.WARNING, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    apnic = Crawler(ORG, URL, NAME)
    apnic.run()
    apnic.close()

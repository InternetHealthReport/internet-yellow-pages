import sys
import logging
import flatdict
import requests
import json
from iyp import BaseCrawler

# URL to ASRank API
URL = 'https://api.asrank.caida.org/v2/restful/asns/?first=10000'
ORG = 'CAIDA'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch networks information from ASRank and push to IYP. """

        # get ASNs, names, and countries IDs
        self.asn_id = self.iyp.batch_get_nodes('AS', 'asn')
        self.country_id = self.iyp.batch_get_nodes('COUNTRY', 'country_code')
        self.asrank_qid = self.iyp.get_node('RANKING', {'name': f'CAIDA ASRank'}, create=True)

        has_next = True
        i = 0
        while has_next:
            url = URL+f'&offset={i*10000}'
            i += 1
            print(url)
            req = requests.get(url)
            if req.status_code != 200:
                # FIXME should raise an exception
                sys.exit('Error while fetching data from API')
            
            print(req.status_code)
            ranking = json.loads(req.text)['data']['asns']
            has_next = ranking['pageInfo']['hasNextPage']

            print(len(ranking['edges']))
            print(ranking['edges'][0])
            asns = set()
            names = set()
            countries = set()

            # Collect all ASNs and names
            for node in ranking['edges']:
                asn = node['node']
                names.add(asn['asnName'])
                asns.add(int(asn['asn']))
                countries.add(asn['country']['iso'])

            # Compute links
            country_links = []
            name_links = []
            rank_links = []
            asns = set()
            names = set()
            countries = set()

            for node in ranking['edges']:
                asn = node['node']

                names.add( asn['asnName'] )

                # This may be slow if countries and ASes are not already registered
                if int(asn['asn']) not in self.asn_id:
                    self.asn_id[int(asn['asn'])] = self.iyp.get_node('AS', {'asn':int(asn['asn'])}, create=True)
                if asn['country']['iso'] not in self.country_id:
                    self.country_id[asn['country']['iso']] = self.iyp.get_node('COUNTRY', {'country_code':asn['country']['iso']}, create=True)

                asn_qid = self.asn_id[int(asn['asn'])]
                country_qid = self.country_id[asn['country']['iso']]

                country_links.append( { 'src_id':asn_qid, 'dst_id':country_qid, 'props':[self.reference] } ) # Set AS name
                name_links.append( { 'src_id':asn_qid, 'dst_name':asn['asnName'], 'props':[self.reference] } ) # Set AS name
                
                ## flatten all attributes into one dictionary
                flat_asn = dict(flatdict.FlatDict(asn, delimiter='_'))

                rank_links.append( { 'src_id':asn_qid, 'dst_id':self.asrank_qid, 'props':[self.reference, flat_asn] } ) # Set AS name

            # Push nodes
            self.names_id = self.iyp.batch_get_nodes('NAME', 'name', names, all=False)

            # Add dst_id in name_links
            for link in name_links :
                link['dst_id'] = self.names_id[link['dst_name']]

            # Push all links to IYP
            self.iyp.batch_add_links('NAME', name_links)
            self.iyp.batch_add_links('COUNTRY', country_links)
            self.iyp.batch_add_links('RANK', rank_links)
        
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
    logging.info("Start: %s" % sys.argv)

    asrank = Crawler(ORG, URL)
    asrank.run()
    asrank.close()

    logging.info("End: %s" % sys.argv)

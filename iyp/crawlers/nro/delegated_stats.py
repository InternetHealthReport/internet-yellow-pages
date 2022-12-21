from collections import defaultdict
import sys
import math
import logging
import requests
from iyp import BaseCrawler

# NOTE: this script is not adding new ASNs. It only adds links for existing ASNs
# Should be run after crawlers that push many ASNs (e.g. ripe.as_names)

URL = 'https://ftp.ripe.net/pub/stats/ripencc/nro-stats/latest/nro-delegated-stats'
ORG = 'NRO'
NAME = 'nro.delegated_stats'

class Crawler(BaseCrawler):

    def run(self):
        """Fetch the delegated stat file from RIPE website and process lines one by one"""

        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching delegated file')

        asn_id = self.iyp.batch_get_nodes('AS', 'asn')

        # Read delegated-stats file. see documentation:
        # https://www.nro.net/wp-content/uploads/nro-extended-stats-readme5.txt
        self.fields_name = ['registry', 'cc', 'type', 'start', 'value', 'date', 'status', 'opaque-id']

        # Compute nodes
        opaqueids = set()
        prefixes = set()
        countries = set()

        for line in req.text.splitlines():
            # skip comments
            if line.strip().startswith('#'):
                continue

            # skip version and summary lines
            fields_value = line.split('|')
            if len(fields_value) < 8:
                continue

            # parse records
            rec = dict( zip(self.fields_name, fields_value))
            rec['value'] = int(rec['value'])

            countries.add( rec['cc'] )
            opaqueids.add( rec['opaque-id'] )

            if rec['type'] == 'ipv4' or rec['type'] == 'ipv6':
                # compute prefix length
                prefix_len = rec['value']
                if rec['type'] == 'ipv4':
                    prefix_len = int(32-math.log2(rec['value']))

                prefix = f"{rec['start']}/{prefix_len}"
                prefixes.add( prefix )

        # Create all nodes
        logging.warning('Pushing nodes to neo4j...\n')
        opaqueid_id = self.iyp.batch_get_nodes('OPAQUE_ID', 'id', opaqueids)
        prefix_id = self.iyp.batch_get_nodes('PREFIX', 'prefix', prefixes)
        country_id = self.iyp.batch_get_nodes('COUNTRY', 'country_code', countries)

        # Compute links
        country_links = []
        status_links = defaultdict(list)
        
        for line in req.text.splitlines():
            # skip comments
            if line.strip().startswith('#'):
                continue

            # skip version and summary lines
            fields_value = line.split('|')
            if len(fields_value) < 8:
                continue

            # parse records
            rec = dict( zip(self.fields_name, fields_value))
            rec['value'] = int(rec['value'])

            reference = dict(self.reference)
            reference['registry'] = rec['registry']
            country_qid = country_id[rec['cc']]
            opaqueid_qid = opaqueid_id[rec['opaque-id']]

            # ASN record
            if rec['type'] == 'asn':
                for i in range(int(rec['start']), int(rec['start'])+int(rec['value']), 1):
                    if i not in asn_id:
                        continue

                    asn_qid = asn_id[i]

                    country_links.append( { 'src_id':asn_qid, 'dst_id':country_qid, 'props':[reference] } )
                    status_links[rec['status'].upper()].append(
                        { 'src_id':asn_qid, 'dst_id':opaqueid_qid, 'props':[reference] } )

            # prefix record
            elif rec['type'] == 'ipv4' or rec['type'] == 'ipv6':

                # compute prefix length
                prefix_len = rec['value']
                if rec['type'] == 'ipv4':
                    prefix_len = int(32-math.log2(rec['value']))

                prefix = f"{rec['start']}/{prefix_len}"
                prefix_qid = prefix_id[prefix]

                country_links.append( { 'src_id':prefix_qid, 'dst_id':country_qid, 'props':[reference] } )
                status_links[rec['status'].upper()].append(
                    { 'src_id':prefix_qid, 'dst_id':opaqueid_qid, 'props':[reference] } )


        logging.warning('Pusing links to neo4j...\n')
        # Push all links to IYP
        self.iyp.batch_add_links('COUNTRY', country_links)
        for label, links in status_links.items():
            self.iyp.batch_add_links(label, links)


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

    asnames = Crawler(ORG, URL, NAME)
    asnames.run()
    asnames.close()

    logging.info("End: %s" % sys.argv)

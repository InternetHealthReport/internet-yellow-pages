import sys
import logging
from SPARQLWrapper import SPARQLWrapper, JSON
# TODO fetch PIDs with sparql, and make it a standalone script 
# (no need for fancy pywikibot setup)
from iyp.wiki.wikihandy import DEFAULT_WIKI_SPARQL, Wikihandy
import radix

class ip2asn(object):

    def __init__(self, wikihandy=None, sparql=DEFAULT_WIKI_SPARQL):
        """Fetch routing prefixes and their origin AS from iyp. 

            wikihandy: a Wikihandy instance to use. A new will be created if 
            this is set to None.
        """

        logging.info('ip2asn initialization...\n')
        if wikihandy is None:
            self.wh = Wikihandy()
        else:
            self.wh = wikihandy

        self.rtree = radix.Radix()
        self.sparql = SPARQLWrapper(sparql)

        logging.info('Fetching prefix info...\n')
        # Fetch prefixes
        QUERY = """
        #Items that have a pKa value set
        SELECT ?item ?prefix ?asn ?as_qid
        WHERE 
        {
                ?item wdt:%s wd:%s.
                ?item rdfs:label ?prefix. 
                ?item wdt:%s ?as_qid.
                ?as_qid wdt:%s ?asn.
        } 
        """ % (
                self.wh.get_pid('instance of'), 
                self.wh.get_qid(f'IP routing prefix') , 
                self.wh.get_pid(f'announced by') , 
                self.wh.get_pid(f'autonomous system number') , 
                )

        # Query wiki
        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        response = self.sparql.query().convert()
        results = response['results']
        
        # Parse results
        for res in results['bindings']:
            prefix_qid = res['item']['value'].rpartition('/')[2]
            prefix = res['prefix']['value']
            asn = res['asn']['value']
            as_qid = res['as_qid']['value'].rpartition('/')[2]

            rnode = self.rtree.add(prefix)
            rnode.data['prefix'] = prefix
            rnode.data['asn'] = asn
            rnode.data['prefix_qid'] = prefix_qid
            rnode.data['as_qid'] = as_qid
     
    def lookup(self, ip):
        """Lookup for the given ip address.
        Returns a dictionary with the corresponding prefix and ASN, as well as
        the corresponding QIDs."""
        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print("Wrong IP address: %s" % ip)
            return None

        if node is None:
            return None
        else:
            return node.data


if __name__ == "__main__":
    
    if len(sys.argv)<2:
        print(f"usage: {sys.argv[0]} IP")
        sys.exit()
    
    ip = sys.argv[1]
    ia = ip2asn()
    res = ia.lookup(ip)
    if res is None:
        print("Unknown")
    else:
        print(res)

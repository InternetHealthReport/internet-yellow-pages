import logging
import sys

import radix
from SPARQLWrapper import JSON, SPARQLWrapper

# TODO fetch PIDs with sparql, and make it a standalone script
# (no need for fancy pywikibot setup)
from iyp.wiki.wikihandy import DEFAULT_WIKI_SPARQL, Wikihandy


class ip2plan(object):

    def __init__(self, wikihandy=None, sparql=DEFAULT_WIKI_SPARQL):
        """Fetch peering lans and their corresponding IXP from iyp.

        wikihandy: a Wikihandy instance to use. A new will be created if
        this is set to None.
        """

        logging.info('ip2plan initialization...\n')
        if wikihandy is None:
            self.wh = Wikihandy()
        else:
            self.wh = wikihandy

        self.rtree = radix.Radix()
        self.sparql = SPARQLWrapper(sparql)

        logging.info('Fetching prefix info...\n')
        # Fetch prefixes
        QUERY = """
        SELECT ?item ?prefix ?ix_qid ?org_qid
        WHERE
        {
                ?item wdt:%s wd:%s.
                ?item rdfs:label ?prefix.
                ?item wdt:%s ?ix_qid.
                ?ix_qid wdt:%s ?org_qid.
        }
        """ % (
                self.wh.get_pid('instance of'),
                self.wh.get_qid('peering LAN'),
                self.wh.get_pid('managed by'),
                self.wh.get_pid('managed by'),
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
            ix_qid = res['ix_qid']['value'].rpartition('/')[2]
            org_qid = res['org_qid']['value'].rpartition('/')[2]

            rnode = self.rtree.add(prefix)
            rnode.data['prefix'] = prefix
            rnode.data['ix_qid'] = ix_qid
            rnode.data['prefix_qid'] = prefix_qid
            rnode.data['org_qid'] = org_qid

        logging.info(QUERY)
        logging.info(f'Found {len(self.rtree.nodes())} peering LANs')

    def lookup(self, ip):
        """Lookup for the given ip address.

        Returns a dictionary with the corresponding prefix and ASN, as well as the
        corresponding QIDs.
        """
        try:
            node = self.rtree.search_best(ip)
        except ValueError:
            print('Wrong IP address: %s' % ip)
            return None

        if node is None:
            return None
        else:
            return node.data


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print(f'usage: {sys.argv[0]} IP')
        sys.exit()

    ip = sys.argv[1]
    ia = ip2plan()
    res = ia.lookup(ip)
    if res is None:
        print('Unknown')
    else:
        print(res)

import bz2
import json

import requests

from iyp import BaseCrawler, RequestStatusError


class AS2RelCrawler(BaseCrawler):
    def __init__(self, organization, url, name, af):
        """Initialization: set the address family attribute (af)"""

        self.af = af
        super().__init__(organization, url, name)

    def run(self):
        """Fetch the AS relationship file from BGPKIT website and process lines one by
        one."""

        req = requests.get(self.url, stream=True)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching AS relationships')

        rels = []
        asns = set()

        # Collect all ASNs
        for rel in json.load(bz2.open(req.raw)):
            asns.add(rel['asn1'])
            asns.add(rel['asn2'])
            rels.append(rel)

        # get ASNs IDs
        self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)

        # Compute links
        links = []
        for rel in rels:
            as1_qid = self.asn_id[rel['asn1']]
            as2_qid = self.asn_id[rel['asn2']]
            rel['af'] = self.af

            links.append({'src_id': as1_qid, 'dst_id': as2_qid, 'props': [self.reference, rel]})

        # Push all links to IYP
        self.iyp.batch_add_links('PEERS_WITH', links)

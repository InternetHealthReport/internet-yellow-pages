import bz2
import json
import logging
from datetime import datetime, timezone

import requests

from iyp import BaseCrawler, RequestStatusError


class AS2RelCrawler(BaseCrawler):
    def __init__(self, organization, url, name, af):
        """Initialization: set the address family attribute (af)"""
        super().__init__(organization, url, name)
        self.af = af
        self.reference['reference_url_info'] = 'https://data.bgpkit.com/as2rel/README.txt'

    def run(self):
        """Fetch the AS relationship file from BGPKIT website and process lines one by
        one."""

        req = requests.get(self.url, stream=True)
        if req.status_code != 200:
            raise RequestStatusError(f'Error while fetching AS relationships: {req.status_code}')

        try:
            last_modified_str = req.headers['Last-Modified']
            # All HTTP dates are in UTC:
            # https://www.rfc-editor.org/rfc/rfc2616#section-3.3.1
            last_modified = datetime.strptime(last_modified_str,
                                              '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = last_modified
        except KeyError:
            logging.warning('No Last-Modified header; will not set modification time.')
        except ValueError as e:
            logging.error(f'Failed to parse Last-Modified header "{last_modified_str}": {e}')

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

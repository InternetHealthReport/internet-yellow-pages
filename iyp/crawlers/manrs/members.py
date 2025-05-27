import argparse
import logging
import sys
from datetime import datetime, time, timezone

import requests

from iyp import BaseCrawler

# URL to MANRS csv file
URL = 'https://www.manrs.org/wp-json/manrs/v1/csv/4'
ORG = 'MANRS'
NAME = 'manrs.members'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Fetch nodes for MANRS actions (create them if they are not in IYP)."""

        # connect to IYP database
        super().__init__(organization, url, name)

        self.manrs_qid = self.iyp.get_node(
            'Organization',
            {'name': 'MANRS'}
        )

        # Actions defined by MANRS
        self.actions = [
            {
                'label': 'MANRS Action 1: Filtering',
                'description': 'Prevent propagation of incorrect routing information'
            },
            {
                'label': 'MANRS Action 2: Anti-spoofing',
                'description': 'Prevent traffic with spoofed source IP addresses'
            },
            {
                'label': 'MANRS Action 3: Coordination',
                'description': 'Facilitate global operational communication and coordination'
            },
            {
                'label': 'MANRS Action 4: Global Validation',
                'description': 'Facilitate routing information on a global scale'
            }
        ]

        # Get the ID for the four items representing MANRS actions
        for action in self.actions:
            action['qid'] = self.iyp.get_node(
                'ManrsAction',
                {
                    'name': action['label'],
                    'description': action['description']
                },
                id_properties={'name'}
            )

        # Reference information for data pushed to IYP
        self.reference = {
            'reference_name': NAME,
            'reference_org': ORG,
            'reference_url_data': URL,
            'reference_time_fetch': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
        }

    def run(self):
        req = requests.get(URL)
        req.raise_for_status()

        # Keep track of unique nodes and relationships.
        asn_set = set()
        country_set = set()
        country_rel_set = set()
        implement_rel_set = set()

        # Process CSV file.
        for i, row in enumerate(req.text.splitlines()):
            # Skip the header.
            if i == 0:
                continue

            org, areas, asns, act1, act2, act3, act4 = [col.strip() for col in row.split(',')]

            # Ignore organizations without ASN.
            if not asns:
                continue

            for asn in asns.split(';'):
                asn = int(asn)
                asn_set.add(asn)
                for cc in areas.split(';'):
                    cc = cc.strip()
                    country_set.add(cc)
                    country_rel_set.add((asn, cc))
                for j, action_bool in enumerate([act1, act2, act3, act4]):
                    if action_bool == 'Yes':
                        implement_rel_set.add((asn, self.actions[j]['qid']))

        # Get/create nodes.
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asn_set, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', country_set)

        # Compute relationships.
        member_of_rels = list()
        country_rels = list()
        implement_rels = list()
        for asn in asn_set:
            member_of_rels.append({'src_id': asn_id[asn],
                                   'dst_id': self.manrs_qid,
                                   'props': [self.reference]})
        for asn, cc in country_rel_set:
            country_rels.append({'src_id': asn_id[asn],
                                 'dst_id': country_id[cc],
                                 'props': [self.reference]})
        # Translate to QIDs.
        for asn, action_qid in implement_rel_set:
            implement_rels.append({'src_id': asn_id[asn],
                                   'dst_id': action_qid,
                                   'props': [self.reference]})

        # Push relationships.
        self.iyp.batch_add_links('MEMBER_OF', member_of_rels)
        self.iyp.batch_add_links('COUNTRY', country_rels)
        self.iyp.batch_add_links('IMPLEMENT', implement_rels)

    def unit_test(self):
        return super().unit_test(['MEMBER_OF', 'IMPLEMENT', 'COUNTRY'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

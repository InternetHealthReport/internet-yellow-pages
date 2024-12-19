import argparse
import logging
import sys
from io import BytesIO

import pandas as pd
import requests

from iyp import BaseCrawler, RequestStatusError

URL = 'https://bgp.tools/asns.csv'
ORG = 'BGP.Tools'
NAME = 'bgptools.as_names'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://bgp.tools/kb/api'

        self.headers = {
            'user-agent': 'IIJ/Internet Health Report - admin@ihr.live'
        }

    @staticmethod
    def replace_link_ids(links: list, src_id: dict = dict(), dst_id=dict()):
        """Replace the src_id and dst_id values from links with their actual id."""
        for link in links:
            if src_id:
                link['src_id'] = src_id[link['src_id']]
            if dst_id:
                link['dst_id'] = dst_id[link['dst_id']]

    def run(self):
        """Fetch the AS name file from BGP.Tools website and push it to IYP."""

        req = requests.get(URL, headers=self.headers)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching AS names')

        df = pd.read_csv(BytesIO(req.content), keep_default_na=False)

        asns = set()
        names = set()
        tags = set()
        name_links = list()
        tag_links = list()

        # Normally we would use itertuples, since it is way faster. But we want to be
        # robust against format changes and since one column is called "class", which is
        # a Python keyword, the field name would be replaced by a positional value,
        # e.g., r._3 instead of r.class, which means that if the format is changed, this
        # crawler breaks again.
        # Since the data set is not too large, iterrows is fine performance-wise.
        for r in df.iterrows():
            has_link = False
            entry = r[1]
            asn = entry['asn']
            if not asn.startswith('AS'):
                logging.warning(f'asn field does not start with "AS": {entry}')
                continue
            asn = int(asn[2:])
            name = entry['name']
            if name != 'ERR_AS_NAME_NOT_FOUND':
                names.add(name)
                name_links.append({'src_id': asn, 'dst_id': name, 'props': [self.reference]})
                has_link = True
            tag = entry['class']
            if tag != 'Unknown':
                tags.add(tag)
                tag_links.append({'src_id': asn, 'dst_id': tag, 'props': [self.reference]})
                has_link = True
            if has_link:
                # Only create AS nodes if we have a relationship.
                asns.add(asn)

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names, all=False)
        tag_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', tags, all=False)

        self.replace_link_ids(name_links, asn_id, name_id)
        self.replace_link_ids(tag_links, asn_id, tag_id)

        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('CATEGORIZED', tag_links)

    def unit_test(self):
        return super().unit_test(['NAME'])


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

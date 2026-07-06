import argparse
import csv
import logging
import re
import sys
from datetime import datetime, timezone

import requests

from iyp import BaseCrawler

URL = 'https://asdb.stanford.edu/about'
ORG = 'Stanford'
NAME = 'stanford.asdb'
DATA_URL_FMT = 'https://asdb.stanford.edu/static-website/data/%Y-%m_categorized_ases.csv'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://asdb.stanford.edu/about'
        self.__get_latest_asdb_dataset_url()

    def __get_latest_asdb_dataset_url(self):
        date_regex = re.compile(r'Dataset was last updated and published on \d{1,2}/\d{1,2}/\d{4}')
        response = requests.get(URL)
        if response.status_code != 200:
            logging.error(f'Failed to access landing page {URL}: {response.status_code}')
            raise RuntimeError(f'Failed to access landing page {URL}: {response.status_code}')
        date_match = date_regex.search(response.text)
        if date_match is None:
            logging.error('Failed to find last-published data in HTML source.')
            raise RuntimeError('Failed to find last-published data in HTML source.')
        date_string = date_match.group().split()[-1]
        date = datetime.strptime(date_string, '%m/%d/%Y').replace(tzinfo=timezone.utc)
        dataset_url = date.strftime(DATA_URL_FMT)
        self.reference['reference_url_data'] = dataset_url
        self.reference['reference_time_modification'] = date

    def run(self):
        """Fetch the ASdb file and push it to IYP."""

        req = requests.get(self.reference['reference_url_data'])
        req.raise_for_status()

        lines = set()
        asns = set()
        categories = set()

        # Collect all ASNs, categories, layers, and PART_OF layer hierarchy
        part_of_lines = set()
        for line in csv.reader(req.text.splitlines(), quotechar='"', delimiter=',', skipinitialspace=True):
            if not line:
                continue

            if not line[0] or line[0] == 'ASN':
                continue

            asn = int(line[0][2:])
            cats = line[1:]
            for i, category in enumerate(cats):
                if not category:
                    continue

                # Get layer 1 entry
                if i % 2 == 0:
                    layer = 1
                    categories.add(category)
                    asns.add(asn)
                    lines.add((asn, layer, category))

                # Get layer 2 entry
                else:
                    parent_category = cats[i - 1]
                    if not parent_category:
                        continue

                    # Remove 'Other' subcategories
                    # Only store their parent category
                    if category == 'Other' or category == 'other':
                        continue

                    # Handle PART_OF layer hierarchy
                    part_of_lines.add((category, parent_category))

                    layer = 2
                    categories.add(category)
                    asns.add(asn)
                    lines.add((asn, layer, category))

        # get ASNs and names IDs
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        category_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', categories)

        # Compute PART_OF links
        part_of_links = []
        for (subcat, cat) in part_of_lines:

            subcat_qid = category_id[subcat]
            cat_qid = category_id[cat]

            part_of_links.append({'src_id': subcat_qid, 'dst_id': cat_qid,
                                  'props': [self.reference]})

        self.iyp.batch_add_links('PART_OF', part_of_links)

        # Compute links
        links = []
        for (asn, layer, category) in lines:

            asn_qid = asn_id[asn]
            category_qid = category_id[category]

            links.append({'src_id': asn_qid, 'dst_id': category_qid,
                          'props': [self.reference, {'layer': layer}]})  # Set AS category

        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED'])


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

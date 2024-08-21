import argparse
import csv
import logging
import os
import re
import sys
from datetime import datetime, timezone

import bs4
import requests
from bs4 import BeautifulSoup

from iyp import BaseCrawler, RequestStatusError


def get_latest_asdb_dataset_url(asdb_stanford_data_url: str, file_name_format: str):
    response = requests.get(asdb_stanford_data_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    latest_date_element: bs4.element.Tag = soup.find('div', class_='col-md-12').find('p')
    date_regex = re.compile(r'\d{1,2}/\d{1,2}/\d{4}')
    date_string: str = date_regex.search(latest_date_element.text).group()
    date: datetime = datetime.strptime(date_string, '%m/%d/%Y')
    dateset_file_name: str = date.strftime(file_name_format)
    asdb_stanford_data_url_formated: str = asdb_stanford_data_url.replace('#', '')
    full_url: str = f'{asdb_stanford_data_url_formated}/{dateset_file_name}'
    return full_url


URL = get_latest_asdb_dataset_url('https://asdb.stanford.edu/#data', '%Y-%m_categorized_ases.csv')
ORG = 'Stanford'
NAME = 'stanford.asdb'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://asdb.stanford.edu/'
        self.__set_modification_time_from_url()

    def __set_modification_time_from_url(self):
        fmt = 'https://asdb.stanford.edu/data/%Y-%m_categorized_ases.csv'
        try:
            date = datetime.strptime(URL, fmt).replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except ValueError as e:
            logging.warning(f'Failed to set modification time: {e}')

    def run(self):
        """Fetch the ASdb file and push it to IYP."""

        req = requests.get(URL)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching ASdb')

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

    scriptname = os.path.basename(sys.argv[0]).replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
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

import argparse
import csv
import logging
import os
import re
import sys
from datetime import datetime

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
    def run(self):
        """Fetch the ASdb file and push it to IYP."""

        req = requests.get(URL)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching ASdb')

        lines = []
        asns = set()
        categories = set()

        # Collect all ASNs and names
        for line in csv.reader(req.text.splitlines(), quotechar='"', delimiter=',', skipinitialspace=True):
            if not line:
                continue

            if not line[0] or line[0] == 'ASN':
                continue

            asn = int(line[0][2:])
            cats = line[1:]
            for category in cats:
                if category:
                    asns.add(asn)
                    categories.add(category)

                    lines.append([asn, category])

        # get ASNs and names IDs
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        category_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', categories)

        # Compute links
        links = []
        for (asn, category) in lines:

            asn_qid = asn_id[asn]
            category_qid = category_id[category]

            links.append({'src_id': asn_qid, 'dst_id': category_qid, 'props': [self.reference]})  # Set AS category

        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)


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
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

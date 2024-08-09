import argparse
import logging
import os
import sys
from urllib.error import HTTPError

import pandas as pd

from iyp import BaseCrawler, RequestStatusError

ORG = 'Citizen Lab'
URL = 'https://github.com/citizenlab/test-lists/blob/master/lists/'
NAME = 'citizenlab.urldb'


def generate_url(suffix):
    base_url = 'https://raw.githubusercontent.com/citizenlab/test-lists/master/lists/'
    joined_url = ''.join([base_url, suffix, '.csv'])
    return joined_url


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://github.com/citizenlab/test-lists'

    def run(self):
        # Fetch country codes to generate urls
        try:
            cc_df = pd.read_csv(generate_url('00-LEGEND-country_codes'), keep_default_na=False)
        except Exception as e:
            logging.error(f'Failed to fetch country codes: {e}')
            raise RequestStatusError('Error while fetching data file')

        country_codes = [e.lower() for e in cc_df['CountryCode']]

        # Iterate through country_codes, generate an url, download the csv file, extract
        # the necessary information from the csv file, and push the data to IYP.
        relationship_pairs = set()
        urls = set()
        categories = set()

        for code in country_codes:
            # Not all country codes have CSV files.
            try:
                df = pd.read_csv(generate_url(code))
            except HTTPError as e:
                # 404 is expected, everything else is not.
                if e.getcode() != 404:
                    logging.warning(f'Request for country code "{code}" failed with error: {e}')
                    raise e
                continue

            for row in df.itertuples():
                url = row.url
                category = row.category_description
                urls.add(url)
                categories.add(category)
                relationship_pairs.add((url, category))

        url_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', urls, all=False)
        category_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', categories, all=False)

        links = list()
        for (url, category) in relationship_pairs:
            url_qid = url_id[url]
            category_qid = category_id[category]
            links.append({'src_id': url_qid, 'dst_id': category_qid, 'props': [self.reference]})

        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)

    def unit_test(self):
        super().unit_test(logging, ['CATEGORIZED'])


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

import argparse
import csv
import logging
import os
import sys

import requests

from iyp import BaseCrawler, RequestStatusError

# Organization name and URL to data
ORG = 'Citizen Lab'
URL = 'https://github.com/citizenlab/test-lists/blob/master/lists/'
NAME = 'citizenlab.urldb'  # should reflect the directory and name of this file


def generate_url(suffix):
    base_url = 'https://raw.githubusercontent.com/citizenlab/test-lists/master/lists/'
    joined_url = ''.join([base_url, suffix, '.csv'])
    return joined_url


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and set up a dictionary with the org/url/today's date in self.reference

    def run(self):
        # Fetch country codes to generate urls
        req_for_country_codes = requests.get(generate_url('00-LEGEND-country_codes'))

        if req_for_country_codes.status_code != 200:
            logging.error('Cannot download data {req.status_code}: {req.text}')
            raise RequestStatusError('Error while fetching data file')

        content = req_for_country_codes.content.decode('utf-8')
        csv_data = csv.reader(content.splitlines(), delimiter=',')

        country_codes = []
        for row in csv_data:
            country_codes.append(row[0].lower())

        # Iterate through country_codes, generate an url, download the csv file,
        # extract the necessary information from the csv file,
        # and push the data to IYP.
        lines = []
        urls = set()
        categories = set()

        for code in country_codes:
            url = generate_url(code)
            req_with_respect_to_country_code = requests.get(url)
            print('Processing {}'.format(code))

            # Not necessarily every country code have a csv file.
            # Skipping those don't have one.
            if req_with_respect_to_country_code.status_code != 200:
                print('Skipping {}'.format(code))
                continue

            decoded_response = req_with_respect_to_country_code.content.decode('utf-8')
            rows = csv.reader(decoded_response.splitlines(), delimiter=',')
            for row in rows:
                url = row[0]
                category = row[2]
                urls.add(url)
                categories.add(category)
                if [url, category] in lines:
                    continue
                lines.append([url, category])

        url_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', urls)
        category_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', categories)

        links = []
        for (url, category) in lines:
            url_qid = url_id[url]
            category_qid = category_id[category]
            links.append({'src_id': url_qid, 'dst_id': category_qid, 'props': [self.reference]})

        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)
        print('Processed citizenlab/test-lists repo.')


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

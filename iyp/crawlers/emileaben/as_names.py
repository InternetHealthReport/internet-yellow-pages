import argparse
import logging
import os
import sys
import tempfile

import requests

from iyp import BaseCrawler, get_commit_datetime

# Organization name and URL to data
ORG = 'emileaben'
URL = 'https://raw.githubusercontent.com/emileaben/asnames/main/asnames.csv'
NAME = 'emileaben.as_names'  # should reflect the directory and name of this file


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://github.com/emileaben/asnames'
        self.reference['reference_time_modification'] = get_commit_datetime('emileaben/asnames', 'asnames.csv')

    def run(self):
        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()
        filename = os.path.join(tmpdir, 'as_names.txt')

        res = requests.get(URL)
        res.raise_for_status()

        with open(filename, 'w') as file:
            file.write(res.text)

        lines = []
        asns = set()
        as_names = set()

        with open(filename, 'r') as file:
            for line in file:
                line = line.strip()
                values = line.split('|')
                as_number = values[0]
                as_name = values[2]
                asns.add(int(as_number))
                as_names.add(as_name)
                lines.append(values)

            asns_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
            as_names_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', as_names, all=False)

            links = []

            for line in lines:
                asn_qid = asns_id[int(line[0])]
                as_name_qid = as_names_id[line[2]]
                links.append(
                    {'src_id': asn_qid, 'dst_id': as_name_qid, 'props': [self.reference, {'contributor': line[1]}]})

            # Push all links to IYP
            self.iyp.batch_add_links('NAME', links)

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

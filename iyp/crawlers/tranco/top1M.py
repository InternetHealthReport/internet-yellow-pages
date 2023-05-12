import argparse
import io
import logging
import os
import sys
from zipfile import ZipFile

import requests

from iyp import BaseCrawler

# URL to Tranco top 1M
URL = 'https://tranco-list.eu/top-1m.csv.zip'
ORG = 'imec-DistriNet'
NAME = 'tranco.top1m'


class Crawler(BaseCrawler):

    def run(self):
        """Fetch Tranco top 1M and push to IYP."""

        self.tranco_qid = self.iyp.get_node('Ranking', {'name': 'Tranco top 1M'}, create=True)

        sys.stderr.write('Downloading latest list...\n')
        req = requests.get(URL)
        if req.status_code != 200:
            sys.exit('Error while fetching Tranco csv file')

        links = []
        domains = set()
        # open zip file and read top list
        with ZipFile(io.BytesIO(req.content)) as z:
            with z.open('top-1m.csv') as list:
                for i, row in enumerate(io.TextIOWrapper(list)):
                    row = row.rstrip()
                    rank, domain = row.split(',')

                    domains.add(domain)
                    links.append({'src_name': domain, 'dst_id': self.tranco_qid,
                                 'props': [self.reference, {'rank': int(rank)}]})

        name_id = self.iyp.batch_get_nodes('DomainName', 'name', domains)

        for link in links:
            link['src_id'] = name_id[link['src_name']]

        # Push all links to IYP
        self.iyp.batch_add_links('RANK', links)


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

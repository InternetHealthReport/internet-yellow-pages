import argparse
import csv
import io
import logging
import sys

import requests
from bs4 import BeautifulSoup

from iyp import BaseCrawler

URL = 'https://bgp.tools/tags/'
ORG = 'bgp.tools'
NAME = 'bgptools.tags'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://bgp.tools/kb/api'
        self.tag_labels = dict()
        self.fetch_tags = list()

        self.session = requests.Session()
        self.session.headers.update({
            'user-agent': 'IIJ/Internet Health Report - admin@ihr.live'
        })

    def __get_tag_labels(self):
        """Fetch the pretty-print label corresponding to each tag by scraping the
        overview website."""
        logging.info('Fetching tag labels')
        r = self.session.get('https://bgp.tools/tags/')
        r.raise_for_status()
        soup = BeautifulSoup(r.text, features='html.parser')
        for link in soup.find_all('a'):
            href = link['href']
            if href.startswith('/tags/'):
                tag = href.removeprefix('/tags/')
                label = link.text.strip()
                self.tag_labels[tag] = label
        logging.info(f'Fetched {len(self.tag_labels)} tag labels')

    def __get_tag_counts(self):
        """Get the number of values for each tag from the summary file to only fetch
        tags that actually have values."""
        logging.info('Fetching tag counts')
        r = self.session.get('https://bgp.tools/tags.txt')
        r.raise_for_status()
        for l in r.text.splitlines():
            tag, count = l.split(',')
            if int(count) > 0:
                if tag not in self.tag_labels:
                    raise ValueError(f'No label available for tag {tag}')
                self.fetch_tags.append(tag)

    def run(self):
        """Fetch available tags and labels from the overview file and then fetch
        individual tag files, process the data and push to IYP."""
        self.__get_tag_labels()
        self.__get_tag_counts()

        asns = set()
        tags = {self.tag_labels[tag] for tag in self.fetch_tags}

        categorized_links = list()

        for tag in self.fetch_tags:
            tag_label = self.tag_labels[tag]
            logging.info(f'Processing tag {tag} / "{tag_label}"')

            tag_url = f'https://bgp.tools/tags/{tag}.csv'
            r = self.session.get(tag_url)
            r.raise_for_status()

            tag_reference = self.reference.copy()
            tag_reference['reference_url_data'] = tag_url

            for row in csv.reader(io.StringIO(r.text)):
                as_str, name = row
                if not as_str.startswith('AS'):
                    raise ValueError(f'Invalid AS string in row: {row}')
                asn = int(as_str.removeprefix('AS'))
                asns.add(asn)
                categorized_links.append({'src_id': asn, 'dst_id': tag_label, 'props': [tag_reference]})

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        tag_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', tags, all=False)

        for link in categorized_links:
            link['src_id'] = asn_id[link['src_id']]
            link['dst_id'] = tag_id[link['dst_id']]

        self.iyp.batch_add_links('CATEGORIZED', categorized_links)

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

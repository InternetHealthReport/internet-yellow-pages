import argparse
import logging
import sys

import requests

from iyp import BaseCrawler, set_modification_time_from_last_modified_header

URL = 'https://ftp.ripe.net/ripe/asnames/asn.txt'
ORG = 'RIPE NCC'
NAME = 'ripe.as_names'


class Crawler(BaseCrawler):

    def run(self):
        """Fetch the AS name file from RIPE website and process lines one by one."""

        req = requests.get(URL)
        req.raise_for_status()

        set_modification_time_from_last_modified_header(self.reference, req)

        lines = []
        asns = set()
        names = set()
        countries = set()

        # Read asn file
        for line in req.text.splitlines():
            asn, _, name_cc = line.partition(' ')
            name, _, cc = name_cc.rpartition(', ')

            if not all((asn, name, cc)) or len(cc) > 2:
                logging.warning(f'Ignoring invalid line: "{line}"')
                continue

            asn = int(asn)
            lines.append([asn, name, cc])

            asns.add(asn)
            names.add(name)
            countries.add(cc)

        # get node IDs for ASNs, names, and countries
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)

        # Compute links
        name_links = []
        country_links = []

        for asn, name, cc in lines:
            asn_qid = asn_id[asn]
            name_qid = name_id[name]
            country_qid = country_id[cc]

            name_links.append({'src_id': asn_qid, 'dst_id': name_qid,
                               'props': [self.reference]})  # Set AS name
            country_links.append({'src_id': asn_qid, 'dst_id': country_qid,
                                  'props': [self.reference]})  # Set country

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('COUNTRY', country_links)

    def unit_test(self):
        return super().unit_test(['NAME', 'COUNTRY'])


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

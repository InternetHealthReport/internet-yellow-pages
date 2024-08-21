import argparse
import logging
import os
import sys

import iso3166
import requests

from iyp import BaseCrawler, RequestStatusError

# URL to APNIC API
URL = 'http://v6data.data.labs.apnic.net/ipv6-measurement/Economies/'
ORG = 'APNIC'
NAME = 'apnic.eyeball'
MIN_POP_PERC = 0.01  # ASes with less population will be ignored


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialize IYP and list of countries."""

        self.url = URL  # url will change for each country
        self.countries = iso3166.countries_by_alpha2
        super().__init__(organization, url, name)

    def run(self):
        """Fetch data from APNIC and push to IYP."""

        processed_asn = set()

        for cc, country in self.countries.items():
            logging.info(f'processing {country}')

            # Get the QID of the country and corresponding ranking
            cc_qid = self.iyp.get_node('Country', {'country_code': cc})
            ranking_qid = self.iyp.get_node('Ranking', {'name': f'APNIC eyeball estimates ({cc})'})
            statements = [['COUNTRY', cc_qid, self.reference]]
            self.iyp.add_links(ranking_qid, statements)

            self.url = URL + f'{cc}/{cc}.asns.json?m={MIN_POP_PERC}'
            req = requests.get(self.url)
            if req.status_code != 200:
                raise RequestStatusError(f'Error while fetching data for {cc}')

            asns = set()
            names = set()

            ranking = req.json()
            logging.info(f'{len(ranking)} eyeball ASes')

            # Collect all ASNs and names
            # and make sure the ranking is sorted and add rank field
            ranking.sort(key=lambda x: x['percent'], reverse=True)
            for i, asn in enumerate(ranking):
                asn['rank'] = i + 1
                asns.add(asn['as'])
                names.add(asn['autnum'])

            # Get node IDs
            self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
            self.name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names, all=False)

            # Compute links
            country_links = []
            rank_links = []
            pop_links = []
            name_links = []
            for asn in ranking:
                asn_qid = self.asn_id[asn['as']]  # self.iyp.get_node('AS', {'asn': asn[2:]}, create=True)

                if asn['as'] not in processed_asn:
                    name_qid = self.name_id[asn['autnum']]  # self.iyp.get_node('Name', {'name': name}, create=True)
                    name_links.append({'src_id': asn_qid, 'dst_id': name_qid, 'props': [self.reference]})
                    country_links.append({'src_id': asn_qid, 'dst_id': cc_qid, 'props': [self.reference]})

                    processed_asn.add(asn['as'])

                rank_links.append({'src_id': asn_qid, 'dst_id': ranking_qid, 'props': [self.reference, asn]})
                pop_links.append({'src_id': asn_qid, 'dst_id': cc_qid, 'props': [self.reference, asn]})

            # Push all links to IYP
            self.iyp.batch_add_links('NAME', name_links)
            self.iyp.batch_add_links('COUNTRY', country_links)
            self.iyp.batch_add_links('RANK', rank_links)
            self.iyp.batch_add_links('POPULATION', pop_links)

    def unit_test(self):
        return super().unit_test(['POPULATION', 'COUNTRY', 'RANK', 'NAME'])


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

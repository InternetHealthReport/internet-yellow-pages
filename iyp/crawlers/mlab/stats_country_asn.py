import argparse
import logging
import sys

from iyp.crawlers.mlab import MLabStatsCrawler

ORG = 'M-Lab'
URL = ''
NAME = 'mlab.stats_country_asn'
aggregate = 'country_asn'


class Crawler(MLabStatsCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, aggregate)

    def __speed_test_link_generator(self, links: dict, country_id: dict):
        for (country_code, estimate_qid), properties in links.items():
            yield {
                'src_id': country_id[country_code],
                'dst_id': estimate_qid,
                'props': [self.reference, properties],
            }

    def __country_link_generator(self, links: set, estimate_id: dict, country_id: dict):
        for estimate, country_code in links:
            yield {
                'src_id': estimate_id[estimate],
                'dst_id': country_id[country_code],
                'props': [self.reference],
            }

    def __speed_test_link_generator(self, links: dict, as_id: dict, estimate_id: dict):
        for (asn, estimate), properties in links.items():
            yield {
                'src_id': as_id[asn],
                'dst_id': estimate_id[estimate],
                'props': [self.reference, properties],
            }

    def process_data(self):
        combined_df = (
            self.parquet_files['downloads']
            .set_index(['country_code', 'asn'])
            .rename(columns={'sample_count': 'download_sample_count'})
            .drop('as_name', axis='columns')
            .join(
                self.parquet_files['uploads']
                .set_index(['country_code', 'asn'])
                .rename(columns={'sample_count': 'upload_sample_count'})
                .drop('as_name', axis='columns'),
                how='outer',
            )
        )
        asns = set()
        countries = set()
        estimates = set()
        country_links = set()
        speed_test_links = dict()
        for (country_code, asn), properties in combined_df.to_dict('index').items():
            asns.add(asn)
            estimate = f'M-Lab Stats {country_code}'
            estimates.add(estimate)
            countries.add(country_code)
            country_links.add((estimate, country_code))
            properties['country_code'] = country_code
            speed_test_links[(asn, estimate)] = properties

        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop(
            'Country', 'country_code', countries, all=False
        )
        estimate_id = self.iyp.batch_get_nodes_by_single_prop(
            'Estimate', 'name', estimates, all=False
        )
        self.iyp.batch_add_links(
            'COUNTRY', self.__country_link_generator(country_links, estimate_id, country_id)
        )
        self.iyp.batch_add_links(
            'SPEED_TEST', self.__speed_test_link_generator(speed_test_links, as_id, estimate_id)
        )

    def unit_test(self):
        return super().unit_test(['SPEED_TEST', 'COUNTRY'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
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

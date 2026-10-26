import argparse
import logging
import sys

from iyp.crawlers.mlab import MLabStatsCrawler

ORG = 'M-Lab'
URL = ''
NAME = 'mlab.stats_country'
aggregate = 'country'


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

    def process_data(self):
        combined_df = (
            self.parquet_files['downloads']
            .set_index('country_code')
            .rename(columns={'sample_count': 'download_sample_count'})
            .join(
                self.parquet_files['uploads']
                .set_index('country_code')
                .rename(columns={'sample_count': 'upload_sample_count'}),
                how='outer',
            )
        )
        estimate_qid = self.iyp.get_node('Estimate', {'name': 'M-Lab Stats All Country'})
        countries = set(combined_df.index)
        speed_test_links = dict()
        for country_code, properties in combined_df.to_dict('index').items():
            properties['country_code'] = country_code
            speed_test_links[(country_code, estimate_qid)] = properties

        country_id = self.iyp.batch_get_nodes_by_single_prop(
            'Country', 'country_code', countries, all=False
        )
        self.iyp.batch_add_links(
            'SPEED_TEST', self.__speed_test_link_generator(speed_test_links, country_id)
        )

    def unit_test(self):
        return super().unit_test(['SPEED_TEST'])


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

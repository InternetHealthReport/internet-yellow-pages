import argparse
import logging
import sys
from collections import defaultdict

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.torsf'

label = 'OONI Tor Snowflake Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'torsf')
        self.categories = ['ok', 'failure']

    def process_one_line(self, one_line):
        """Process a single line from the JSONL file."""
        if super().process_one_line(one_line):
            return
        if 'success' not in one_line['test_keys']:
            self.all_results.pop()
            return
        result = 'ok' if one_line['test_keys']['success'] else 'failure'

        # Update the last entry in all_results with the new test-specific data
        self.all_results[-1] = self.all_results[-1] + (result,)

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        torsf_id = self.iyp.get_node('Tag', {'label': label}, create=True)

        censored_links = list()

        # Create one link per ASN-country pair.
        for (asn, country), result_dict in self.all_percentages.items():
            asn_id = self.node_ids['asn'][asn]
            props = dict()
            for category in self.categories:
                props[f'percentage_{category}'] = result_dict['percentages'][category]
                props[f'count_{category}'] = result_dict['category_counts'][category]
            props['total_count'] = result_dict['total_count']
            props['country_code'] = country
            censored_links.append(
                {'src_id': asn_id, 'dst_id': torsf_id, 'props': [props, self.reference]}
            )

        self.iyp.batch_add_links('CENSORED', censored_links)

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, result = entry
            target_dict[(asn, country)][result] += 1

        for (asn, country), counts in target_dict.items():
            self.all_percentages[(asn, country)] = self.make_result_dict(counts)

    def unit_test(self):
        return super().unit_test(['CENSORED'])


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

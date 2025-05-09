import argparse
import logging
import sys
from collections import defaultdict

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.facebookmessenger'

label = 'OONI Facebook Messenger Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'facebookmessenger')
        self.categories = ['unblocked', 'dns_blocking', 'tcp_blocking', 'both_blocked']

    # Process a single line from the jsonl file and store the results locally
    def process_one_line(self, one_line):
        if super().process_one_line(one_line):
            return
        result_dns = one_line['test_keys'].get('facebook_dns_blocking', None)
        result_tcp = one_line['test_keys'].get('facebook_tcp_blocking', None)
        if result_dns is None or result_tcp is None:
            self.all_results.pop()
            return

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1] + (result_dns, result_tcp)

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        facebookmessenger_id = self.iyp.get_node('Tag', {'label': label}, create=True)

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
                {'src_id': asn_id, 'dst_id': facebookmessenger_id, 'props': [props, self.reference]}
            )

        self.iyp.batch_add_links('CENSORED', censored_links)

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        for entry in self.all_results:
            asn, country, result_dns, result_tcp = entry
            if not result_dns and not result_tcp:
                target_dict[(asn, country)]['unblocked'] += 1
            elif result_dns and not result_tcp:
                target_dict[(asn, country)]['dns_blocking'] += 1
            elif not result_dns and result_tcp:
                target_dict[(asn, country)]['tcp_blocking'] += 1
            elif result_dns and result_tcp:
                target_dict[(asn, country)]['both_blocked'] += 1

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

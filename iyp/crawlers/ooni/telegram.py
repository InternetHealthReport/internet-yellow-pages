import argparse
import logging
import os
import sys
from collections import defaultdict

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.telegram'

label = 'OONI Telegram Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'telegram')
        # 'total' and 'no_total' are meta categories that indicate if any of the three
        # main categories is blocked.
        self.categories = [
            'total_blocked',
            'total_ok',
            'web_blocked',
            'web_none',
            'web_ok',
            'http_blocked',
            'http_ok',
            'tcp_blocked',
            'tcp_ok',
        ]

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""
        if super().process_one_line(one_line):
            return

        telegram_http_blocking = one_line['test_keys']['telegram_http_blocking']
        telegram_tcp_blocking = one_line['test_keys']['telegram_tcp_blocking']
        telegram_web_status = one_line['test_keys']['telegram_web_status']

        # Normalize result
        if telegram_web_status == 'blocked':
            result_web = 'web_blocked'
        elif telegram_web_status == 'ok':
            result_web = 'web_ok'
        else:
            result_web = 'web_none'

        result_http = 'http_blocked' if telegram_http_blocking else 'http_ok'
        result_tcp = 'tcp_blocked' if telegram_tcp_blocking else 'tcp_ok'

        total = 'total_ok'
        if result_web == 'web_blocked' or result_http == 'http_blocked' or result_tcp == 'tcp_blocked':
            total = 'total_blocked'

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1] + (
            total,
            result_web,
            result_http,
            result_tcp,
        )

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        telegram_id = self.iyp.get_node('Tag', {'label': label}, create=True)

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
                {'src_id': asn_id, 'dst_id': telegram_id, 'props': [props, self.reference]}
            )

        self.iyp.batch_add_links('CENSORED', censored_links)

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, total, result_web, result_http, result_tcp = entry
            target_dict[(asn, country)][total] += 1
            target_dict[(asn, country)][result_web] += 1
            target_dict[(asn, country)][result_http] += 1
            target_dict[(asn, country)][result_tcp] += 1

        for (asn, country), counts in target_dict.items():
            total_count = counts['total_ok'] + counts['total_blocked']
            self.all_percentages[(asn, country)] = self.make_result_dict(counts, total_count)

    def unit_test(self):
        return super().unit_test(['CENSORED'])


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

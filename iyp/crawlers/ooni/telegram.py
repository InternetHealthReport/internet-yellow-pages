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

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""
        super().process_one_line(one_line)

        telegram_http_blocking = one_line.get('test_keys', {}).get(
            'telegram_http_blocking', False
        )
        telegram_tcp_blocking = one_line.get('test_keys', {}).get(
            'telegram_tcp_blocking', False
        )
        telegram_web_status = (
            one_line.get('test_keys', {}).get('telegram_web_status', 'null').lower()
        )

        # Normalize result
        if telegram_web_status == 'blocked':
            result_web = 'web_blocked'
        elif telegram_web_status == 'ok':
            result_web = 'unblocked'
        else:
            result_web = 'unblocked'

        result_http = 'http_blocked' if telegram_http_blocking else 'unblocked'
        result_tcp = 'tcp_blocked' if telegram_tcp_blocking else 'unblocked'

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1][:2] + (
            result_web,
            result_http,
            result_tcp,
        )

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        telegram_id = self.iyp.batch_get_nodes_by_single_prop(
            'Tag', 'label', {label}
        ).get(label)

        censored_links = []

        # Accumulate properties for each ASN-country pair
        link_properties = defaultdict(lambda: defaultdict(lambda: 0))

        # Ensure all IDs are present and process results
        for asn, country, result_web, result_http, result_tcp in self.all_results:
            asn_id = self.node_ids['asn'].get(asn)
            country_id = self.node_ids['country'].get(country)

            if asn_id and country_id:
                props = self.reference.copy()
                if (asn, country) in self.all_percentages:
                    percentages = self.all_percentages[(asn, country)].get(
                        'percentages', {}
                    )
                    counts = self.all_percentages[(asn, country)].get(
                        'category_counts', {}
                    )
                    total_count = self.all_percentages[(asn, country)].get(
                        'total_count', 0
                    )

                    for category in [
                        'unblocked',
                        'web_blocked',
                        'http_blocked',
                        'tcp_blocked',
                    ]:
                        props[f"percentage_{category}"] = percentages.get(category, 0)
                        props[f"count_{category}"] = counts.get(category, 0)
                    props['total_count'] = total_count

                # Accumulate properties
                link_properties[(asn_id, telegram_id)] = props

        for (asn_id, telegram_id), props in link_properties.items():
            if (asn_id, telegram_id) not in self.unique_links['CENSORED']:
                self.unique_links['CENSORED'].add((asn_id, telegram_id))
                censored_links.append(
                    {'src_id': asn_id, 'dst_id': telegram_id, 'props': [props]}
                )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', censored_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Initialize counts for all categories
        categories = [
            'unblocked',
            'web_blocked',
            'http_blocked',
            'tcp_blocked',
        ]

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, result_web, result_http, result_tcp = entry
            target_dict[(asn, country)][result_web] += 1
            target_dict[(asn, country)][result_http] += 1
            target_dict[(asn, country)][result_tcp] += 1

        self.all_percentages = {}

        for (asn, country), counts in target_dict.items():
            total_count = sum(counts.values())
            for category in categories:
                counts[category] = counts.get(category, 0)

            percentages = {
                category: (
                    (counts[category] / total_count) * 100 if total_count > 0 else 0
                )
                for category in categories
            }

            result_dict = {
                'total_count': total_count,
                'category_counts': dict(counts),
                'percentages': percentages,
            }
            self.all_percentages[(asn, country)] = result_dict


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    scriptname = os.path.basename(sys.argv[0]).replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(levellevel)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + scriptname + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logging.info(f"Started: {sys.argv}")

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f"Finished: {sys.argv}")


if __name__ == '__main__':
    main()
    sys.exit(0)

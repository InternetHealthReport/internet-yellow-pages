import argparse
import logging
import os
import sys
from collections import defaultdict

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.whatsapp'

label = 'OONI WhatsApp Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'whatsapp')

    def process_one_line(self, one_line):
        """Process a single line from the JSONL file."""
        super().process_one_line(one_line)
        test_keys = one_line.get('test_keys', {})

        # Determine the status and failure for each category
        server_status = test_keys.get('registration_server_status', '').lower()
        server_failure = test_keys.get('registration_server_failure')
        endpoint_status = test_keys.get('whatsapp_endpoints_status', '').lower()
        web_status = test_keys.get('whatsapp_web_status', '').lower()
        web_failure = test_keys.get('whatsapp_web_failure')

        server_result = (
            'server_failure'
            if server_failure is not None
            else f"server_{server_status}"
        )
        endpoint_result = f"endpoint_{endpoint_status}"
        web_result = 'web_failure' if web_failure is not None else f"web_{web_status}"

        # Update the last entry in all_results with the new test-specific data
        self.all_results[-1] = self.all_results[-1][:2] + (
            server_result,
            endpoint_result,
            web_result,
        )

        if len(self.all_results[-1]) != 5:
            self.all_results.pop()

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        whatsapp_id = self.iyp.batch_get_nodes_by_single_prop(
            'Tag', 'label', {label}
        ).get(label)

        censored_links = []

        # Ensure all IDs are present and process results
        for (
            asn,
            country,
            server_result,
            endpoint_result,
            web_result,
        ) in self.all_results:
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
                        'server_failure',
                        'server_ok',
                        'server_blocked',
                        'endpoint_ok',
                        'endpoint_blocked',
                        'web_failure',
                        'web_ok',
                        'no_server_failure',
                        'no_server_ok',
                        'no_server_blocked',
                        'no_endpoint_ok',
                        'no_endpoint_blocked',
                        'no_web_failure',
                        'no_web_ok',
                    ]:
                        props[f"percentage_{category}"] = percentages.get(category, 0)
                        props[f"count_{category}"] = counts.get(category, 0)
                    props['total_count'] = total_count

                if (asn_id, whatsapp_id) not in self.unique_links['CENSORED']:
                    self.unique_links['CENSORED'].add((asn_id, whatsapp_id))
                    censored_links.append(
                        {'src_id': asn_id, 'dst_id': whatsapp_id, 'props': [props]}
                    )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', censored_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Initialize counts for all categories
        categories = [
            'server_failure',
            'server_ok',
            'server_blocked',
            'endpoint_ok',
            'endpoint_blocked',
            'web_failure',
            'web_ok',
            'no_server_failure',
            'no_server_ok',
            'no_server_blocked',
            'no_endpoint_ok',
            'no_endpoint_blocked',
            'no_web_failure',
            'no_web_ok',
        ]

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, server_result, endpoint_result, web_result = entry
            target_dict[(asn, country)][server_result] += 1
            target_dict[(asn, country)][endpoint_result] += 1
            target_dict[(asn, country)][web_result] += 1

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
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
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

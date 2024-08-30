import argparse
import logging
import os
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

    # Process a single line from the jsonl file and store the results locally
    def process_one_line(self, one_line):
        super().process_one_line(one_line)
        result_dns = one_line.get('test_keys', {}).get('facebook_dns_blocking')
        result_tcp = one_line.get('test_keys', {}).get('facebook_tcp_blocking')

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1][:2] + (result_dns, result_tcp)

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        facebookmessenger_id = self.iyp.get_node('Tag', {'label': label}, create=True)

        censored_links = []

        # Accumulate properties for each ASN-country pair
        link_properties = defaultdict(lambda: defaultdict(int))

        for asn, country, result_dns, result_tcp in self.all_results:
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
                        'dns_blocking',
                        'tcp_blocking',
                        'both_blocked',
                    ]:
                        props[f'percentage_{category}'] = percentages.get(category, 0)
                        props[f'count_{category}'] = counts.get(category, 0)
                    props['total_count'] = total_count

                # Accumulate properties
                link_properties[(asn_id, facebookmessenger_id)] = props

        # Create links only once per ASN-country pair
        for (asn_id, facebookmessenger_id), props in link_properties.items():
            if (asn_id, facebookmessenger_id) not in self.unique_links['CENSORED']:
                self.unique_links['CENSORED'].add((asn_id, facebookmessenger_id))
                censored_links.append(
                    {'src_id': asn_id, 'dst_id': facebookmessenger_id, 'props': [props]}
                )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', censored_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Initialize counts for all categories
        categories = ['unblocked', 'dns_blocking', 'tcp_blocking', 'both_blocked']

        # Populate the target_dict with counts
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

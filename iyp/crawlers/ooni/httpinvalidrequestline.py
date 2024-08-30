import argparse
import logging
import os
import sys
from collections import defaultdict

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.httpinvalidrequestline'

label = 'OONI HTTP Invalid Request Line Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'httpinvalidrequestline')

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""
        super().process_one_line(one_line)

        tampering = one_line.get('test_keys', {}).get('tampering', False)

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1][:2] + (tampering,)

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        httpinvalidrequestline_id = self.iyp.get_node('Tag', {'label': label}, create=True)

        censored_links = []

        # Accumulate properties for each ASN-country pair
        link_properties = defaultdict(lambda: defaultdict(int))

        for asn, country, tampering in self.all_results:
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
                    props['percentage_no_tampering'] = percentages.get(
                        'no_tampering', 0
                    )
                    props['count_no_tampering'] = counts.get('no_tampering', 0)
                    props['percentage_tampering'] = percentages.get('tampering', 0)
                    props['count_tampering'] = counts.get('tampering', 0)
                    props['total_count'] = total_count

                # Accumulate properties
                link_properties[(asn_id, httpinvalidrequestline_id)] = props

        for (asn_id, httpinvalidrequestline_id), props in link_properties.items():
            if (asn_id, httpinvalidrequestline_id) not in self.unique_links['CENSORED']:
                self.unique_links['CENSORED'].add((asn_id, httpinvalidrequestline_id))
                censored_links.append(
                    {
                        'src_id': asn_id,
                        'dst_id': httpinvalidrequestline_id,
                        'props': [props],
                    }
                )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', censored_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Initialize counts for all categories
        categories = ['tampering', 'no_tampering']

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, tampering = entry
            target_dict[(asn, country)]['tampering'] += 1 if tampering else 0
            target_dict[(asn, country)]['no_tampering'] += 1 if not tampering else 0

        self.all_percentages = {}

        for (asn, country), counts in target_dict.items():
            total_count = counts['tampering'] + counts['no_tampering']
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

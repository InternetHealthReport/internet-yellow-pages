import argparse
import logging
import os
import sys
from collections import defaultdict

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.httpheaderfieldmanipulation'

label = 'OONI HTTP Header Field Manipulation Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'httpheaderfieldmanipulation')

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""
        super().process_one_line(one_line)

        test_keys = one_line.get('test_keys', {}).get('tampering', {})

        total = 'total' if test_keys.get('total', False) else 'no_total'
        request_line_capitalization = (
            'request_line_capitalization'
            if test_keys.get('request_line_capitalization', False)
            else 'no_request_line_capitalization'
        )
        header_name_capitalization = (
            'header_name_capitalization'
            if test_keys.get('header_name_capitalization', False)
            else 'no_header_name_capitalization'
        )
        header_field_value = (
            'header_field_value'
            if test_keys.get('header_field_value', False)
            else 'no_header_field_value'
        )
        header_field_number = (
            'header_field_number'
            if test_keys.get('header_field_number', False)
            else 'no_header_field_number'
        )

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1][:2] + (
            total,
            request_line_capitalization,
            header_name_capitalization,
            header_field_value,
            header_field_number,
        )

        if len(self.all_results[-1]) != 7:
            self.all_results.pop()

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        httpheader_id = self.iyp.get_node('Tag', {'label': label}, create=True)

        censored_links = []

        # Accumulate properties for each ASN-country pair
        link_properties = defaultdict(lambda: defaultdict(int))

        for (
            asn,
            country,
            total,
            request_line_capitalization,
            header_name_capitalization,
            header_field_value,
            header_field_number,
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
                        'total',
                        'no_total',
                        'request_line_capitalization',
                        'no_request_line_capitalization',
                        'header_name_capitalization',
                        'no_header_name_capitalization',
                        'header_field_value',
                        'no_header_field_value',
                        'header_field_number',
                        'no_header_field_number',
                    ]:
                        props[f'percentage_{category}'] = percentages.get(category, 0)
                        props[f'count_{category}'] = counts.get(category, 0)
                    props['total_count'] = total_count

                # Accumulate properties
                link_properties[(asn_id, httpheader_id)] = props

        for (asn_id, httpheader_id), props in link_properties.items():
            if (asn_id, httpheader_id) not in self.unique_links['CENSORED']:
                self.unique_links['CENSORED'].add((asn_id, httpheader_id))
                censored_links.append(
                    {'src_id': asn_id, 'dst_id': httpheader_id, 'props': [props]}
                )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', censored_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Initialize counts for all categories
        categories = [
            'total',
            'no_total',
            'request_line_capitalization',
            'no_request_line_capitalization',
            'header_name_capitalization',
            'no_header_name_capitalization',
            'header_field_value',
            'no_header_field_value',
            'header_field_number',
            'no_header_field_number',
        ]

        # Populate the target_dict with counts
        for entry in self.all_results:
            (
                asn,
                country,
                total,
                request_line_capitalization,
                header_name_capitalization,
                header_field_value,
                header_field_number,
            ) = entry
            target_dict[(asn, country)][total] += 1
            target_dict[(asn, country)][request_line_capitalization] += 1
            target_dict[(asn, country)][header_name_capitalization] += 1
            target_dict[(asn, country)][header_field_value] += 1
            target_dict[(asn, country)][header_field_number] += 1

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

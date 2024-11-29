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
        self.categories = [
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

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""
        if super().process_one_line(one_line):
            return

        test_keys = one_line['test_keys']['tampering']

        # "total" is true if an invalid response was received from the backend server,
        # i.e., any tampering occurred, but also if there was no valid response.
        # In this case, the individual fields are all false, but total is true, which
        # can seem confusing.
        total = 'total' if test_keys['total'] else 'no_total'
        request_line_capitalization = (
            'request_line_capitalization'
            if test_keys['request_line_capitalization']
            else 'no_request_line_capitalization')
        header_name_capitalization = (
            'header_name_capitalization'
            if test_keys['header_name_capitalization']
            else 'no_header_name_capitalization'
        )
        header_field_value = (
            'header_field_value'
            if test_keys['header_field_value']
            else 'no_header_field_value'
        )
        header_field_number = (
            'header_field_number'
            if test_keys['header_field_number']
            else 'no_header_field_number'
        )

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1] + (
            total,
            request_line_capitalization,
            header_name_capitalization,
            header_field_value,
            header_field_number,
        )

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        httpheader_id = self.iyp.get_node('Tag', {'label': label}, create=True)

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
                {'src_id': asn_id, 'dst_id': httpheader_id, 'props': [props, self.reference]}
            )

        self.iyp.batch_add_links('CENSORED', censored_links)

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))

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

        for (asn, country), counts in target_dict.items():
            # This test tests multiple things with one result, i.e., the categories are
            # not disjunct so we have to use our own total count.
            total_count = counts['total'] + counts['no_total']
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

import argparse
import logging
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
        self.categories = [
            'total_ok',
            'total_blocked',
            'endpoint_ok',
            'endpoint_blocked',
            'registration_server_ok',
            'registration_server_blocked',
            'web_ok',
            'web_blocked',
        ]

    def process_one_line(self, one_line):
        """Process a single line from the JSONL file."""
        if super().process_one_line(one_line):
            return
        test_keys = one_line['test_keys']

        # Determine the status and failure for each category
        server_status = test_keys['registration_server_status']
        endpoint_status = test_keys['whatsapp_endpoints_status']
        web_status = test_keys['whatsapp_web_status']

        server_result = f'registration_server_{server_status}'
        endpoint_result = f'endpoint_{endpoint_status}'
        web_result = f'web_{web_status}'

        total = 'total_ok'
        if (
            server_result == 'registration_server_blocked'
            or endpoint_result == 'endpoint_blocked'
            or web_result == 'web_blocked'
        ):
            total = 'total_blocked'
        # Update the last entry in all_results with the new test-specific data
        self.all_results[-1] = self.all_results[-1] + (
            total,
            server_result,
            endpoint_result,
            web_result,
        )

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        whatsapp_id = self.iyp.get_node('Tag', {'label': label}, create=True)

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
                {'src_id': asn_id, 'dst_id': whatsapp_id, 'props': [props, self.reference]}
            )

        self.iyp.batch_add_links('CENSORED', censored_links)

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        for entry in self.all_results:
            asn, country, total, server_result, endpoint_result, web_result = entry
            target_dict[(asn, country)][total] += 1
            target_dict[(asn, country)][server_result] += 1
            target_dict[(asn, country)][endpoint_result] += 1
            target_dict[(asn, country)][web_result] += 1

        for (asn, country), counts in target_dict.items():
            total_count = counts['total_ok'] + counts['total_blocked']
            self.all_percentages[(asn, country)] = self.make_result_dict(counts, total_count)

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

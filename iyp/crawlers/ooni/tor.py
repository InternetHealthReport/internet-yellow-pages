import argparse
import ipaddress
import logging
import os
import sys
from collections import defaultdict

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.tor'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'tor')
        self.all_ip_tags = set()
        # Prepend "OONI Probe Tor Tag" to all tag labels
        self.all_tags = {tag: f'OONI Probe Tor Tag {tag}'
                         for tag in ['or_port_dirauth', 'dir_port', 'obfs4', 'or_port']}
        self.categories = ['ok', 'failure']

    def process_one_line(self, one_line):
        """Process a single line of the JSONL file."""
        if super().process_one_line(one_line):
            return

        test_keys = one_line['test_keys']

        # Check each target in the test_keys
        first_target = True
        for target_data in test_keys['targets'].values():
            # Technically the target_address can be domain:port, but apparently this is
            # never the case?
            ip = ipaddress.ip_address(
                target_data['target_address'].rsplit(':', 1)[0].strip('[]')
            ).compressed

            result = 'failure' if target_data['failure'] else 'ok'

            target_protocol = target_data['target_protocol']
            if target_protocol not in self.all_tags:
                continue

            self.all_ip_tags.add((ip, self.all_tags[target_protocol]))
            if first_target:
                self.all_results[-1] = self.all_results[-1] + (ip, result)
                first_target = False
            else:
                # One test contains results for multiple targets, so copy the (asn,
                # country) part and append new data.
                new_entry = self.all_results[-1][:2] + (ip, result)
                self.all_results.append(new_entry)

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        censored_links = list()
        categorized_links = list()
        ips = set()

        for ip, tag in self.all_ip_tags:
            ips.add(ip)
            categorized_links.append({'src_id': ip, 'dst_id': tag, 'props': [self.reference]})

        self.node_ids.update(
            {
                'ip': self.iyp.batch_get_nodes_by_single_prop(
                    'IP', 'ip', ips, all=False
                ),
                'tag': self.iyp.batch_get_nodes_by_single_prop(
                    'Tag', 'label', set(self.all_tags.values()), all=False
                ),
            }
        )

        # Replace IP and tag in CATEGORIZED links with node IDs.
        for link in categorized_links:
            link['src_id'] = self.node_ids['ip'][link['src_id']]
            link['dst_id'] = self.node_ids['tag'][link['dst_id']]

        for (asn, country, ip), result_dict in self.all_percentages.items():
            asn_id = self.node_ids['asn'][asn]
            ip_id = self.node_ids['ip'][ip]
            props = dict()
            for category in self.categories:
                props[f'percentage_{category}'] = result_dict['percentages'][category]
                props[f'count_{category}'] = result_dict['category_counts'][category]
            props['total_count'] = result_dict['total_count']
            props['country_code'] = country
            censored_links.append(
                {'src_id': asn_id, 'dst_id': ip_id, 'props': [props, self.reference]}
            )

        self.iyp.batch_add_links('CENSORED', censored_links)
        self.iyp.batch_add_links('CATEGORIZED', categorized_links)

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))
        for entry in self.all_results:
            asn, country, ip, result = entry
            target_dict[(asn, country, ip)][result] += 1

        for (asn, country, ip), counts in target_dict.items():
            self.all_percentages[(asn, country, ip)] = self.make_result_dict(counts)

    def unit_test(self):
        return super().unit_test(['CENSORED', 'CATEGORIZED'])


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

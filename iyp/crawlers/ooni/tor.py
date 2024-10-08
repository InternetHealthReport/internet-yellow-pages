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
        self.all_ips = set()
        self.all_tags = {'or_port_dirauth', 'dir_port', 'obfs4', 'or_port'}

    def process_one_line(self, one_line):
        """Process a single line of the JSONL file."""
        super().process_one_line(one_line)

        test_keys = one_line.get('test_keys', {})

        if not test_keys:
            self.all_results.pop()
            return

        # Check each target in the test_keys
        first_target = True
        targets = test_keys.get('targets', {})
        for _, target_data in targets.items():
            ip = ipaddress.ip_address(
                target_data.get('target_address').rsplit(':', 1)[0].strip('[]')
            )
            self.all_ips.add(ip)
            result = target_data.get('failure')
            target_protocol = target_data.get('target_protocol')
            if target_protocol not in self.all_tags:
                continue
            if first_target:
                self.all_results[-1] = self.all_results[-1][:2] + (
                    ip,
                    target_protocol,
                    result,
                )
                first_target = False
            else:
                new_entry = self.all_results[-1][:2] + (ip, target_protocol, result)
                self.all_results.append(new_entry)

            if not first_target and len(self.all_results[-1]) != 5:
                self.all_results.pop()

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        # Prepend "OONI Probe Tor Tag" to all tag labels
        prepended_tags = {f'OONI Probe Tor Tag {tag}' for tag in self.all_tags}
        self.node_ids.update(
            {
                'ip': self.iyp.batch_get_nodes_by_single_prop(
                    'IP', 'ip', [str(ip) for ip in self.all_ips]
                ),
                'tag': self.iyp.batch_get_nodes_by_single_prop(
                    'Tag', 'label', prepended_tags
                ),
            }
        )

        censored_links = []
        categorized_links = []

        link_properties = defaultdict(lambda: defaultdict(int))

        for asn, country, ip, tor_type, _ in self.all_results:
            asn_id = self.node_ids['asn'].get(asn)
            ip_id = self.node_ids['ip'].get(str(ip))
            tag_id = self.node_ids['tag'].get(f'OONI Probe Tor Tag {tor_type}')

            if asn_id and ip_id:
                props = self.reference.copy()
                if (asn, ip) in self.all_percentages:
                    percentages = self.all_percentages[(asn, ip)].get('percentages', {})
                    counts = self.all_percentages[(asn, ip)].get('category_counts', {})
                    total_count = self.all_percentages[(asn, ip)].get('total_count', 0)

                    for category in ['Failure', 'Success']:
                        props[f'percentage_{category}'] = percentages.get(category, 0)
                        props[f'count_{category}'] = counts.get(category, 0)
                    props['total_count'] = total_count
                link_properties[(asn_id, ip_id)] = props

            if (
                ip_id
                and tag_id
                and (ip_id, tag_id) not in self.unique_links['CATEGORIZED']
            ):
                self.unique_links['CATEGORIZED'].add((ip_id, tag_id))
                categorized_links.append(
                    {'src_id': ip_id, 'dst_id': tag_id, 'props': [self.reference]}
                )

        for (asn_id, ip_id), props in link_properties.items():
            if (asn_id, ip_id) not in self.unique_links['CENSORED']:
                self.unique_links['CENSORED'].add((asn_id, ip_id))
                censored_links.append(
                    {'src_id': asn_id, 'dst_id': ip_id, 'props': [props]}
                )

        self.iyp.batch_add_links('CENSORED', censored_links)
        self.iyp.batch_add_links('CATEGORIZED', categorized_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))
        categories = ['Failure', 'Success']
        for entry in self.all_results:
            asn, country, ip, tor_type, result = entry
            if result is not None:
                target_dict[(asn, ip)]['Failure'] += 1
            else:
                target_dict[(asn, ip)]['Success'] += 1

        self.all_percentages = {}

        for (asn, ip), counts in target_dict.items():
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
            self.all_percentages[(asn, ip)] = result_dict

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

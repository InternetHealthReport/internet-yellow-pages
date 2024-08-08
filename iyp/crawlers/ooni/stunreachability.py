import argparse
import ipaddress
import logging
import os
import sys
from collections import defaultdict

import tldextract

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.stunreachability'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'stunreachability')
        self.all_ips = set()
        self.all_hostnames = set()

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""
        super().process_one_line(one_line)

        stun_endpoint = one_line.get('input')
        test_keys = one_line.get('test_keys', {})
        failure = test_keys.get('failure')
        result = 'Success' if failure is None else 'Failure'

        if stun_endpoint:
            # Extract the hostname from the STUN endpoint URL if it's not an IP address
            hostname = None
            stun_url = stun_endpoint.split('//')[-1]
            stun_ip_port = stun_url.split(':')
            stun_ip = stun_ip_port[0]

            try:
                ipaddress.ip_address(stun_ip)
            except ValueError:
                hostname = tldextract.extract(stun_url).fqdn

            # Handle "queries" section to get IP addresses and map them to the hostname
            ip_addresses = []
            for query in test_keys.get('queries', []):
                if query and query.get('answers'):
                    for answer in query.get('answers', []):
                        if 'ipv4' in answer:
                            ip_addresses.append(answer['ipv4'])
                        elif 'ipv6' in answer:
                            ip_addresses.append(answer['ipv6'])

            self.all_ips.update(ip_addresses)

            # Ensure all required fields are present
            if stun_endpoint:
                # Using the last result from the base class, add our unique variables
                self.all_results[-1] = self.all_results[-1][:3] + (
                    result,
                    hostname,
                    ip_addresses,
                )

                # Append unique variables to corresponding sets
                if hostname:
                    self.all_hostnames.add(hostname)

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        stun_links = []
        resolves_to_links = []

        # Fetch all IP nodes in one batch
        if self.all_ips:
            ip_id_map = self.iyp.batch_get_nodes_by_single_prop(
                'IP', 'ip', list(self.all_ips)
            )
        else:
            ip_id_map = {}

        # Accumulate properties for each ASN-country pair
        link_properties = defaultdict(lambda: defaultdict(lambda: 0))

        # Ensure all IDs are present and process results
        for (
            asn,
            country,
            stun_endpoint,
            result,
            hostname,
            ip_addresses,
        ) in self.all_results:
            asn_id = self.node_ids['asn'].get(asn)
            url_id = self.node_ids['url'].get(stun_endpoint)
            hostname_id = self.node_ids['hostname'].get(hostname)

            if asn_id and url_id:
                props = self.reference.copy()
                if (asn, country, stun_endpoint) in self.all_percentages:
                    percentages = self.all_percentages[
                        (asn, country, stun_endpoint)
                    ].get('percentages', {})
                    counts = self.all_percentages[(asn, country, stun_endpoint)].get(
                        'category_counts', {}
                    )
                    total_count = self.all_percentages[
                        (asn, country, stun_endpoint)
                    ].get('total_count', 0)

                    for category in ['Success', 'Failure']:
                        props[f"percentage_{category}"] = percentages.get(category, 0)
                        props[f"count_{category}"] = counts.get(category, 0)
                    props['total_count'] = total_count

                # Accumulate properties
                link_properties[(asn_id, url_id)] = props

            if result == 'Success' and hostname_id:
                for ip in ip_addresses:
                    ip_id = ip_id_map.get(ip)
                    if ip_id:
                        resolves_to_links.append(
                            {
                                'src_id': hostname_id,
                                'dst_id': ip_id,
                                'props': [self.reference],
                            }
                        )

        for (asn_id, url_id), props in link_properties.items():
            if (asn_id, url_id) not in self.unique_links['CENSORED']:
                self.unique_links['CENSORED'].add((asn_id, url_id))
                stun_links.append(
                    {'src_id': asn_id, 'dst_id': url_id, 'props': [props]}
                )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', stun_links)
        self.iyp.batch_add_links('RESOLVES_TO', resolves_to_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, stun_endpoint, result, hostname, ip_addresses = entry
            target_dict[(asn, country, stun_endpoint)][result] += 1

        self.all_percentages = {}

        # Define all possible result categories to ensure they are included
        possible_results = ['Success', 'Failure']

        for (asn, country, stun_endpoint), counts in target_dict.items():
            total_count = sum(counts.values())

            # Initialize counts for all possible results to ensure they are included
            for result in possible_results:
                counts[result] = counts.get(result, 0)

            percentages = {
                category: (
                    (counts[category] / total_count) * 100 if total_count > 0 else 0
                )
                for category in possible_results
            }

            result_dict = {
                'total_count': total_count,
                'category_counts': dict(counts),
                'percentages': percentages,
            }
            self.all_percentages[(asn, country, stun_endpoint)] = result_dict


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

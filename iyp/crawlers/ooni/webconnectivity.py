import argparse
import ipaddress
import logging
import os
import sys
from collections import defaultdict
from urllib.parse import urlparse

import tldextract

from iyp.crawlers.ooni import OoniCrawler

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.webconnectivity'

label = 'OONI Web Connectivity Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'webconnectivity')
        self.all_urls = set()
        self.all_hostnames = set()

    # Process a single line from the jsonl file and store the results locally
    def process_one_line(self, one_line):
        super().process_one_line(one_line)

        ips = {'ipv4': [], 'ipv6': []}
        input_url = one_line.get('input')
        test_keys = one_line.get('test_keys', {})
        blocking = test_keys.get('blocking')
        accessible = test_keys.get('accessible')

        # Extract the IPs from the DNS replies, if they exist
        queries = test_keys.get('queries', [])
        if queries is not None:
            for query in queries:
                answers = query.get('answers')
                if answers:
                    for answer in answers:
                        ipv4 = answer.get('ipv4', [])
                        ipv6 = answer.get('ipv6', [])
                        ips['ipv4'].extend(ipv4 if isinstance(ipv4, list) else [ipv4])
                        ips['ipv6'].extend(ipv6 if isinstance(ipv6, list) else [ipv6])

        # Remove duplicates if necessary
        ips['ipv4'] = list(set(ips['ipv4']))
        ips['ipv6'] = list(set(ips['ipv6']))

        # Extract the hostname from the URL if it's not an IP address
        hostname = urlparse(input_url).hostname
        hostname = (
            tldextract.extract(input_url).fqdn
            if hostname
            and not (
                hostname.replace('.', '').isdigit() and ipaddress.ip_address(hostname)
            )
            else hostname
        )

        # Ensure all required fields are present
        if (
            self.all_results[-1][0]
            and self.all_results[-1][1]
            and input_url
            and test_keys
        ):
            # Determine the result based on the table
            # (https://github.com/ooni/spec/blob/master/nettests/ts-017-web-connectivity.md)
            if blocking is None and accessible is None:
                result = 'Failure'  # Could not assign values to the fields
            elif blocking is False and accessible is False:
                result = 'Failure'  # Expected failures (e.g., the website down)
            elif blocking is False and accessible is True:
                result = 'OK'  # Expected success (i.e., no censorship)
            elif blocking == 'dns' and accessible is False:
                result = 'Confirmed'  # DNS-based blocking
            elif blocking == 'tcp_ip' and accessible is False:
                result = 'Confirmed'  # TCP-based blocking
            elif blocking == 'http-failure' and accessible is False:
                result = 'Confirmed'  # HTTP or TLS based blocking
            elif blocking == 'http-diff' and accessible is False:
                result = 'Confirmed'  # Blockpage rather than legit page
            else:
                result = 'Anomaly'  # Default case if no other case matches

            # Using the last result from the base class, add our unique variables
            self.all_urls.add(input_url)
            self.all_hostnames.add(hostname)
            self.all_results[-1] = self.all_results[-1][:2] + (
                input_url,
                result,
                hostname,
                ips,
            )

        if len(self.all_results[-1]) != 6:
            self.all_results.pop()

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        censored_links = []
        resolves_to_links = []
        part_of_links = []

        # Collect all IP addresses first
        all_ips = []
        for asn, country, url, result, hostname, ips in self.all_results:
            if result == 'OK' and hostname and ips:
                for ip_type in ips.values():
                    all_ips.extend(
                        ipaddress.ip_address(ip).compressed for ip in ip_type
                    )

        # Fetch all IP nodes in one batch
        ip_id_map = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', all_ips, all=False)

        self.node_ids.update(
            {
                'url': self.iyp.batch_get_nodes_by_single_prop(
                    'URL', 'url', self.all_urls, all=False
                ),
                'hostname': self.iyp.batch_get_nodes_by_single_prop(
                    'HostName', 'name', self.all_hostnames, all=False
                ),
            }
        )

        # Ensure all IDs are present and process results
        for asn, country, url, result, hostname, ips in self.all_results:
            asn_id = self.node_ids['asn'].get(asn)
            url_id = self.node_ids['url'].get(url)
            hostname_id = self.node_ids['hostname'].get(hostname)

            if asn_id and url_id:
                props = self.reference.copy()
                if (asn, country, url) in self.all_percentages:
                    percentages = self.all_percentages[(asn, country, url)].get(
                        'percentages', {}
                    )
                    counts = self.all_percentages[(asn, country, url)].get(
                        'category_counts', {}
                    )
                    total_count = self.all_percentages[(asn, country, url)].get(
                        'total_count', 0
                    )

                    for category in ['OK', 'Confirmed', 'Failure', 'Anomaly']:
                        props[f'percentage_{category}'] = percentages.get(category, 0)
                        props[f'count_{category}'] = counts.get(category, 0)
                    props['total_count'] = total_count

                censored_links.append(
                    {'src_id': asn_id, 'dst_id': url_id, 'props': [props]}
                )

            if result == 'OK' and hostname and ips:
                compressed_ips = [
                    ipaddress.ip_address(ip).compressed
                    for ip_type in ips.values()
                    for ip in ip_type
                ]
                for ip in compressed_ips:
                    ip_id = ip_id_map.get(ip)
                    if (
                        hostname_id
                        and ip_id
                        and (hostname_id, ip_id) not in self.unique_links['RESOLVES_TO']
                    ):
                        self.unique_links['RESOLVES_TO'].add((hostname_id, ip_id))
                        resolves_to_links.append(
                            {
                                'src_id': hostname_id,
                                'dst_id': ip_id,
                                'props': [self.reference],
                            }
                        )
                    if url_id and ip_id:
                        if lambda ip: True if ipaddress.ip_address(ip) else False:
                            if (
                                ip_id
                                and url_id
                                and (ip_id, url_id) not in self.unique_links['PART_OF']
                            ):
                                self.unique_links['PART_OF'].add((ip_id, url_id))
                                part_of_links.append(
                                    {
                                        'src_id': ip_id,
                                        'dst_id': url_id,
                                        'props': [self.reference],
                                    }
                                )

            if hostname_id and url_id:
                # Check if the url is a valid IP address
                if not (
                    lambda url: (
                        lambda hostname: (
                            True
                            if hostname
                            and not isinstance(
                                ValueError, type(ipaddress.ip_address(hostname))
                            )
                            else False
                        )
                    )(urlparse(url).hostname)
                ):
                    if (url_id, hostname_id) not in self.unique_links['PART_OF']:
                        self.unique_links['PART_OF'].add((url_id, hostname_id))
                        part_of_links.append(
                            {
                                'src_id': url_id,
                                'dst_id': hostname_id,
                                'props': [self.reference],
                            }
                        )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', censored_links)
        self.iyp.batch_add_links('RESOLVES_TO', resolves_to_links)
        self.iyp.batch_add_links('PART_OF', part_of_links)

    def calculate_percentages(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, target, result, hostname, ips = entry
            target_dict[(asn, country, target)][result] += 1

        self.all_percentages = {}

        # Define all possible result categories to ensure they are included
        possible_results = ['OK', 'Confirmed', 'Failure', 'Anomaly']

        for (asn, country, target), counts in target_dict.items():
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
            self.all_percentages[(asn, country, target)] = result_dict


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
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

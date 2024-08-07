import argparse
import ipaddress
import json
import logging
import os
import sys
import tempfile
from collections import defaultdict
from urllib.parse import urlparse

import tldextract

from iyp import BaseCrawler

from .utils import grabber

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.webconnectivity'


class Crawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.repo = 'ooni-data-eu-fra'
        self.reference['reference_url_info'] = 'https://ooni.org/post/mining-ooni-data'
        self.unique_links = {
            'COUNTRY': set(),
            'CENSORED': set(),
            'RESOLVES_TO': set(),
            'PART_OF': set(),
        }

    def run(self):
        """Fetch data and push to IYP."""

        self.all_asns = set()
        self.all_urls = set()
        self.all_countries = set()
        self.all_results = list()
        self.all_percentages = list()
        self.all_hostnames = set()
        self.all_dns_resolvers = set()

        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()

        # Fetch data
        grabber.download_and_extract(self.repo, tmpdir, 'webconnectivity')
        logging.info('Successfully downloaded and extracted all files')
        # Now that we have downloaded the jsonl files for the test we want, we can
        # extract the data we want
        testdir = os.path.join(tmpdir, 'webconnectivity')
        for file_name in os.listdir(testdir):
            file_path = os.path.join(testdir, file_name)
            if os.path.isfile(file_path) and file_path.endswith('.jsonl'):
                with open(file_path, 'r') as file:
                    for i, line in enumerate(file):
                        data = json.loads(line)
                        self.process_one_line(data)
                        logging.info(f'\rProcessed {i+1} lines')
        logging.info('\nProcessed lines, now calculating percentages\n')
        self.calculate_percentages()
        logging.info('\nCalculated percentages, now adding entries to IYP\n')
        self.batch_add_to_iyp()
        logging.info('\nSuccessfully added all entries to IYP\n')

    # Process a single line from the jsonl file and store the results locally
    def process_one_line(self, one_line):
        """Add the entry to IYP if it's not already there and update its properties."""

        ips = {'ipv4': [], 'ipv6': []}

        probe_asn = (
            int(one_line.get('probe_asn')[2:])
            if one_line.get('probe_asn') and one_line.get('probe_asn').startswith('AS')
            else None
        )
        # Add the DNS resolver to the set, unless its not a valid IP address
        try:
            self.all_dns_resolvers.add(
                ipaddress.ip_address(one_line.get('resolver_ip'))
            )
        except ValueError:
            pass
        probe_cc = one_line.get('probe_cc')
        input_url = one_line.get('input')
        test_keys = one_line.get('test_keys', {})
        blocking = test_keys.get('blocking')
        accessible = test_keys.get('accessible')

        # Extract the IPs from the DNS replies, if they exist
        queries = test_keys.get('queries', [])
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

        # Extract the hostname from the URL if its not an IP address
        if not bool(ipaddress.ip_address(urlparse(input_url).hostname)):
            hostname = tldextract.extract(input_url).fqdn

        # Ensure all required fields are present
        if probe_asn and probe_cc and input_url and test_keys:
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

            # Append the results to the list
            self.all_asns.add(probe_asn)
            self.all_countries.add(probe_cc)
            self.all_urls.add(input_url)
            self.all_hostnames.add(hostname)
            self.all_results.append(
                (probe_asn, probe_cc, input_url, result, hostname, ips)
            )

    def batch_add_to_iyp(self):
        # First, add the nodes and store their IDs directly as returned dictionaries
        self.node_ids = {
            'asn': self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', self.all_asns),
            'country': self.iyp.batch_get_nodes_by_single_prop(
                'Country', 'country_code', self.all_countries
            ),
            'url': self.iyp.batch_get_nodes_by_single_prop('URL', 'url', self.all_urls),
            'hostname': self.iyp.batch_get_nodes_by_single_prop(
                'HostName', 'name', self.all_hostnames
            ),
            'dns_resolver': self.iyp.batch_get_nodes_by_single_prop(
                'IP', 'ip', self.all_dns_resolvers, all=False
            ),
        }

        country_links = []
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
        ip_id_map = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', all_ips)

        # Ensure all IDs are present and process results
        for asn, country, url, result, hostname, ips in self.all_results:
            asn_id = self.node_ids['asn'].get(asn)
            url_id = self.node_ids['url'].get(url)
            country_id = self.node_ids['country'].get(country)
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

            if asn_id and country_id:
                if (
                    asn_id
                    and country_id
                    and (asn_id, country_id) not in self.unique_links['COUNTRY']
                ):
                    self.unique_links['COUNTRY'].add((asn_id, country_id))
                    country_links.append(
                        {
                            'src_id': asn_id,
                            'dst_id': country_id,
                            'props': [self.reference],
                        }
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
                    if (
                        hostname_id
                        and url_id
                        and (hostname_id, url_id) not in self.unique_links['PART_OF']
                    ):
                        self.unique_links['PART_OF'].add((hostname_id, url_id))
                        part_of_links.append(
                            {
                                'src_id': hostname_id,
                                'dst_id': url_id,
                                'props': [self.reference],
                            }
                        )

        # Batch add the links (this is faster than adding them one by one)
        self.iyp.batch_add_links('CENSORED', censored_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('RESOLVES_TO', resolves_to_links)
        self.iyp.batch_add_links('PART_OF', part_of_links)

        # Batch add node labels
        self.iyp.batch_add_node_label(
            list(self.node_ids['dns_resolver'].values()), 'Resolver'
        )

    # Calculate the percentages of the results
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

import argparse
import ipaddress
import logging
import sys
from collections import defaultdict
from urllib.parse import urlparse

import tldextract

from iyp.crawlers.ooni import OoniCrawler, process_dns_queries

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.webconnectivity'

label = 'OONI Web Connectivity Test'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'webconnectivity')
        self.all_urls = set()
        self.all_hostname_ips = set()
        self.all_ip_urls = set()
        self.categories = ['ok', 'confirmed', 'failure', 'anomaly']

    # Process a single line from the jsonl file and store the results locally
    def process_one_line(self, one_line):
        if super().process_one_line(one_line):
            return

        input_url = one_line['input']
        test_keys = one_line['test_keys']
        if 'blocking' not in test_keys or 'accessible' not in test_keys:
            logging.warning('Skipping entry with missing keys')
            logging.warning(one_line)
            self.all_results.pop()
            return
        blocking = test_keys['blocking']
        accessible = test_keys['accessible']

        if not input_url.startswith('http'):
            logging.warning(f'No HTTP URL: {input_url}')

        # Extract the hostname from the URL if it's not an IP address
        try:
            hostname = urlparse(input_url).hostname
        except ValueError as e:
            logging.error(f'Failed to extract hostname from URL "{input_url}": {e}')
            self.all_results.pop()
            return
        try:
            hostname = ipaddress.ip_address(hostname).compressed
            hostname_is_ip = True
        except ValueError:
            hostname = tldextract.extract(input_url).fqdn
            hostname_is_ip = False

        host_ip_set = set()
        if not hostname_is_ip:
            # The test performs DNS queries even if the hostname is an IP, but this
            # does not make sense so we want to ignore it.
            try:
                host_ip_set = process_dns_queries(test_keys['queries'])
            except KeyError:
                logging.warning(f'No DNS resolution for URL: {input_url}')
                self.all_results.pop()
                return

        # Determine the result based on the table
        # (https://github.com/ooni/spec/blob/master/nettests/ts-017-web-connectivity.md)
        if blocking is None and accessible is None:
            result = 'failure'  # Could not assign values to the fields
        elif blocking is False and accessible is False:
            result = 'failure'  # Expected failures (e.g., the website down)
        elif blocking is False and accessible is True:
            result = 'ok'  # Expected success (i.e., no censorship)
        elif blocking == 'dns' and accessible is False:
            result = 'confirmed'  # DNS-based blocking
        elif blocking == 'tcp_ip' and accessible is False:
            result = 'confirmed'  # TCP-based blocking
        elif blocking == 'http-failure' and accessible is False:
            result = 'confirmed'  # HTTP or TLS based blocking
        elif blocking == 'http-diff' and accessible is False:
            result = 'confirmed'  # Blockpage rather than legit page
        else:
            result = 'anomaly'  # Default case if no other case matches

        # Using the last result from the base class, add our unique variables
        self.all_urls.add(input_url)
        if hostname_is_ip:
            self.all_ip_urls.add((hostname, input_url))
        else:
            self.all_hostname_ips.update(host_ip_set)
        self.all_results[-1] = self.all_results[-1] + (
            input_url,
            result,
        )

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        censored_links = list()
        resolves_to_links = list()
        part_of_links = list()
        ips = set()
        hostnames = set()

        for hostname, ip in self.all_hostname_ips:
            hostnames.add(hostname)
            ips.add(ip)
            resolves_to_links.append({'src_id': hostname, 'dst_id': ip, 'props': [self.reference]})

        for ip, url in self.all_ip_urls:
            ips.add(ip)
            part_of_links.append({'src_id': ip, 'dst_id': url, 'props': [self.reference]})

        self.node_ids.update(
            {
                'ip': self.iyp.batch_get_nodes_by_single_prop(
                    'IP', 'ip', ips, all=False
                ),
                'hostname': self.iyp.batch_get_nodes_by_single_prop(
                    'HostName', 'name', hostnames, all=False
                ),
                'url': self.iyp.batch_get_nodes_by_single_prop(
                    'URL', 'url', self.all_urls, all=False
                ),
            }
        )

        for link in resolves_to_links:
            link['src_id'] = self.node_ids['hostname'][link['src_id']]
            link['dst_id'] = self.node_ids['ip'][link['dst_id']]

        for link in part_of_links:
            link['src_id'] = self.node_ids['ip'][link['src_id']]
            link['dst_id'] = self.node_ids['url'][link['dst_id']]

        for (asn, country, url), result_dict in self.all_percentages.items():
            asn_id = self.node_ids['asn'][asn]
            url_id = self.node_ids['url'][url]
            props = dict()
            for category in self.categories:
                props[f'percentage_{category}'] = result_dict['percentages'][category]
                props[f'count_{category}'] = result_dict['category_counts'][category]
            props['total_count'] = result_dict['total_count']
            props['country_code'] = country
            censored_links.append(
                {'src_id': asn_id, 'dst_id': url_id, 'props': [props, self.reference]}
            )

        self.iyp.batch_add_links('CENSORED', censored_links)
        self.iyp.batch_add_links('RESOLVES_TO', resolves_to_links)
        self.iyp.batch_add_links('PART_OF', part_of_links)

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, target, result = entry
            target_dict[(asn, country, target)][result] += 1

        for (asn, country, target), counts in target_dict.items():
            self.all_percentages[(asn, country, target)] = self.make_result_dict(counts)

    def unit_test(self):
        return super().unit_test(['CENSORED', 'RESOLVES_TO', 'PART_OF', 'COUNTRY'])


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

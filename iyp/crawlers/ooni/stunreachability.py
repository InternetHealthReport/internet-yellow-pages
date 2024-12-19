import argparse
import ipaddress
import logging
import sys
from collections import defaultdict

import tldextract

from iyp.crawlers.ooni import OoniCrawler, process_dns_queries

ORG = 'OONI'
URL = 's3://ooni-data-eu-fra/raw/'
NAME = 'ooni.stunreachability'


class Crawler(OoniCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name, 'stunreachability')
        self.all_urls = set()
        self.all_hostname_ips = set()
        self.categories = ['ok', 'failure']

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""
        if super().process_one_line(one_line):
            return
        if not one_line['input']:
            # If no input is provided, the test fails.
            self.all_results.pop()
            return

        stun_url = one_line['input']
        failure = one_line['test_keys']['failure']
        result = 'ok' if failure is None else 'failure'

        # Extract the hostname from the STUN endpoint URL if it's not an IP address
        stun_hostname = None
        stun_endpoint = stun_url.split('//')[-1]
        stun_ip_port = stun_endpoint.split(':')
        stun_ip = stun_ip_port[0]

        try:
            stun_ip = ipaddress.ip_address(stun_ip)
        except ValueError:
            stun_hostname = tldextract.extract(stun_endpoint).fqdn

        # Handle "queries" section to get IP addresses and map them to the hostname
        if stun_hostname:
            host_ip_set = process_dns_queries(one_line['test_keys']['queries'])
            for hostname, ip in host_ip_set:
                if hostname != stun_hostname:
                    logging.warning(f'STUN hostname is "{stun_hostname}" but requested "{hostname}"')
                    continue
                self.all_hostname_ips.add((hostname, ip))
        elif one_line['test_keys']['queries']:
            logging.warning(f'STUN hostname is IP "{stun_ip}" but DNS queries were made?')
            logging.warning(one_line['test_keys']['queries'])

        self.all_urls.add(stun_url)

        # Using the last result from the base class, add our unique variables
        self.all_results[-1] = self.all_results[-1] + (
            stun_url,
            result,
        )

    def batch_add_to_iyp(self):
        super().batch_add_to_iyp()

        censored_links = list()
        resolves_to_links = list()
        hostnames = set()
        ips = set()

        for hostname, ip in self.all_hostname_ips:
            hostnames.add(hostname)
            ips.add(ip)
            resolves_to_links.append({'src_id': hostname, 'dst_id': ip, 'props': [self.reference]})

        self.node_ids.update(
            {
                'url': self.iyp.batch_get_nodes_by_single_prop(
                    'URL', 'url', self.all_urls, all=False
                ),
                'hostname': self.iyp.batch_get_nodes_by_single_prop(
                    'HostName', 'name', hostnames, all=False
                ),
                'ip': self.iyp.batch_get_nodes_by_single_prop(
                    'IP', 'ip', ips, all=False
                ),
            }
        )

        # Replace hostname and IPs in RESOLVES_TO links with node IDs.
        for link in resolves_to_links:
            link['src_id'] = self.node_ids['hostname'][link['src_id']]
            link['dst_id'] = self.node_ids['ip'][link['dst_id']]

        for (asn, country, stun_url), result_dict in self.all_percentages.items():
            asn_id = self.node_ids['asn'][asn]
            url_id = self.node_ids['url'][stun_url]
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

    def aggregate_results(self):
        target_dict = defaultdict(lambda: defaultdict(int))

        # Populate the target_dict with counts
        for entry in self.all_results:
            asn, country, stun_url, result = entry
            target_dict[(asn, country, stun_url)][result] += 1

        for (asn, country, stun_url), counts in target_dict.items():
            self.all_percentages[(asn, country, stun_url)] = self.make_result_dict(counts)

    def unit_test(self):
        return super().unit_test(['CENSORED', 'RESOLVES_TO'])


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

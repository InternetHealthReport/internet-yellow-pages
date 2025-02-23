import argparse
import logging
import os
import sys
import tempfile
from ipaddress import ip_network

import requests

from iyp import (BaseCrawler, ConnectionError, RequestStatusError,
                 get_commit_datetime)

# Organization name and URL to data
ORG = 'BGP.Tools'
URL = 'https://github.com/bgptools/anycast-prefixes'
NAME = 'bgptools.anycast_prefixes'  # should reflect the directory and name of this file


def get_dataset_url(as_prefixes_data_url: str, ip_version: int):
    anycast_prefixes_data_url_formatted: str = as_prefixes_data_url.replace('github.com', 'raw.githubusercontent.com')
    if ip_version == 4:
        anycast_prefixes_data_url_formatted += '/master/anycatch-v4-prefixes.txt'
    else:
        anycast_prefixes_data_url_formatted += '/master/anycatch-v6-prefixes.txt'
    return anycast_prefixes_data_url_formatted


def fetch_dataset(url: str):
    try:
        res = requests.get(url)
        return res
    except requests.exceptions.ConnectionError as e:
        logging.error(e)
        raise ConnectionError('Connection error while fetching data file')
    except requests.exceptions.HTTPError as e:
        logging.error(e)
        raise RequestStatusError('Error while fetching data file')


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.repo = 'bgptools/anycast-prefixes'
        self.v4_file = 'anycatch-v4-prefixes.txt'
        self.v6_file = 'anycatch-v6-prefixes.txt'
        self.reference['reference_url_info'] = 'https://bgp.tools/kb/anycatch'

    def run(self):
        ipv4_prefixes_url = get_dataset_url(URL, 4)
        ipv6_prefixes_url = get_dataset_url(URL, 6)

        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()

        # Filename to save the txt file as
        ipv4_prefixes_filename = os.path.join(tmpdir, 'anycast_ipv4_prefixes.txt')
        ipv6_prefixes_filename = os.path.join(tmpdir, 'anycast_ipv6_prefixes.txt')

        # Fetch data and push to IYP.
        # Overriding the reference_url_data according to prefixes
        self.reference['reference_url_data'] = ipv4_prefixes_url
        self.reference['reference_time_modification'] = get_commit_datetime(self.repo, self.v4_file)
        ipv4_prefixes_response = fetch_dataset(ipv4_prefixes_url)
        logging.info('IPv4 prefixes fetched successfully.')
        self.update(ipv4_prefixes_response, ipv4_prefixes_filename)

        self.reference['reference_url_data'] = ipv6_prefixes_url
        self.reference['reference_time_modification'] = get_commit_datetime(self.repo, self.v6_file)
        ipv6_prefixes_response = fetch_dataset(ipv6_prefixes_url)
        logging.info('IPv6 prefixes fetched successfully.')
        self.update(ipv6_prefixes_response, ipv6_prefixes_filename)

    def update(self, res, filename: str):
        with open(filename, 'w') as file:
            file.write(res.text)

        lines = []
        prefixes = set()

        with open(filename, 'r') as file:
            for line in file:
                try:
                    prefix = ip_network(line.strip()).compressed
                except ValueError as e:
                    logging.warning(f'Ignoring malformed prefix: "{line.strip()}": {e}')
                    continue
                prefixes.add(prefix)
                lines.append(prefix)

            prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes, all=False)
            self.iyp.batch_add_node_label(list(prefix_id.values()), 'BGPPrefix')
            tag_id = self.iyp.get_node('Tag', {'label': 'Anycast'})

            links = []
            for line in lines:
                prefix_qid = prefix_id[line]
                links.append({'src_id': prefix_qid, 'dst_id': tag_id, 'props': [self.reference]})

            # Push all links to IYP
            self.iyp.batch_add_links('CATEGORIZED', links)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
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

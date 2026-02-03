import argparse
import csv
import logging
import sys
from ipaddress import ip_network

import requests

from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'IANA'
URL = 'https://www.iana.org/numbers'
NAME = 'iana.address_space'


class Crawler(BaseCrawler):
    """Crawler for IANA IPv4 and IPv6 Address Space registries."""

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)

    def run(self):
        """Fetch and process all IANA address space data."""
        # Fetch data from all four sources
        self.reference['reference_url_info'] = 'https://www.iana.org/assignments/ipv4-address-space/'
        self.reference['reference_url_data'] = 'https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.csv'  # noqa: E501
        self._process_general(is_ipv6=False)

        self.reference['reference_url_info'] = 'https://www.iana.org/assignments/ipv6-unicast-address-assignments/'
        self.reference['reference_url_data'] = 'https://www.iana.org/assignments/ipv6-unicast-address-assignments/ipv6-unicast-address-assignments.csv'  # noqa: E501
        self._process_general(is_ipv6=True)

        # Process special-purpose addresses
        self.reference['reference_url_info'] = 'https://www.iana.org/assignments/iana-ipv4-special-registry/'
        self.reference['reference_url_data'] = 'https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry-1.csv'  # noqa: E501
        self._process_special()
        self.reference['reference_url_info'] = 'https://www.iana.org/assignments/iana-ipv6-special-registry/'
        self.reference['reference_url_data'] = 'https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry-1.csv'  # noqa: E501
        self._process_special()

    def _fetch_csv(self):
        req = requests.get(self.reference['reference_url_data'])
        req.raise_for_status()
        return csv.DictReader(req.text.splitlines())

    @staticmethod
    def _normalize_organization_name(designation):
        """Normalize organization name from Designation field."""
        # Handle "IANA - xxx" format
        if designation.startswith('IANA - '):
            return 'IANA'

        # Handle "Administered by xxx" format
        if designation.startswith('Administered by '):
            return designation.replace('Administered by ', '')

        # Handle Multicast and Future use
        if designation in ['Multicast', 'Future use']:
            return 'IANA'

        return designation

    def _process_general(self, is_ipv6: bool):
        """Process IPv4 or IPv6 general address space allocations."""
        logging.info(f'Processing {self.reference["reference_url_data"]}')

        csv_data = self._fetch_csv()

        prefixes = set()
        organizations = set()
        links_by_type = {'ALLOCATED': list(), 'RESERVED': list(), 'LEGACY': list()}

        for row in csv_data:
            prefix_str = row['Prefix']
            # Convert IPv4 prefix format (e.g., "001/8" to "1.0.0.0/8")
            if not is_ipv6:
                octet, prefix_len = prefix_str.split('/')
                prefix_str = f'{int(octet)}.0.0.0/{prefix_len}'

            normalized_prefix = ip_network(prefix_str).compressed

            designation = row['Designation']
            org_name = self._normalize_organization_name(designation)

            # Determine relationship type
            if is_ipv6:
                rel_type = row['Status']
            else:
                rel_type = row['Status [1]']
            if rel_type not in links_by_type:
                raise ValueError(f'Unexpected status: {rel_type}')

            prefixes.add(normalized_prefix)
            organizations.add(org_name)

            rel_props = {
                'Designation': designation,
                'Date': row['Date']
            }

            links_by_type[rel_type].append({
                'src_id': normalized_prefix,
                'dst_id': org_name,
                'props': [self.reference, rel_props]
            })

        prefix_id = self.iyp.batch_get_nodes_by_single_prop('IANAPrefix', 'prefix', prefixes, all=False)
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'Prefix')
        org_id = self.iyp.batch_get_nodes_by_single_prop('Organization', 'name', organizations, all=False)

        for rel_type, links in links_by_type.items():
            if not links:
                continue
            for link in links:
                link['src_id'] = prefix_id[link['src_id']]
                link['dst_id'] = org_id[link['dst_id']]
            self.iyp.batch_add_links(rel_type, links)

    @staticmethod
    def _parse_bool_property(value: str):
        """Parse a boolean property from a string in the special-purpose registry, which
        can be 'N/A' or contain footnotes."""
        if value.startswith('N/A'):
            return None
        if ' ' in value:
            value, _ = value.split()
        return value == 'True'

    def _process_special(self):
        """Process IPv4 or IPv6 special-purpose addresses."""
        logging.info(f'Processing {self.reference["reference_url_data"]}')

        csv_data = self._fetch_csv()

        prefixes = set()
        iana_qid = self.iyp.get_node('Organization', {'name': 'IANA'})
        links = list()

        for row in csv_data:
            # Cast boolean properties. Some contain footnotes, some are "N/A" so require
            # special handling.
            for prop in ['Source', 'Destination', 'Forwardable', 'Globally Reachable', 'Reserved-by-Protocol']:
                row[prop] = self._parse_bool_property(row[prop])

            ip_prefix_str = row.pop('Address Block')
            # Unique case of two entries in one cell. Handle specifically and crash
            # if this should ever change.
            if ip_prefix_str == '192.0.0.170/32, 192.0.0.171/32':
                prefixes.add('192.0.0.170/32')
                prefixes.add('192.0.0.171/32')
                links.append({
                    'src_id': '192.0.0.170/32',
                    'dst_id': iana_qid,
                    'props': [self.reference, row]
                })
                links.append({
                    'src_id': '192.0.0.171/32',
                    'dst_id': iana_qid,
                    'props': [self.reference, row]
                })
                continue

            # Some entries contain a footnote, e.g., "192.0.0.0/24 [2]"
            if ' ' in ip_prefix_str:
                ip_prefix_str, _ = ip_prefix_str.split()

            normalized_prefix = ip_network(ip_prefix_str).compressed
            prefixes.add(normalized_prefix)

            links.append({
                'src_id': normalized_prefix,
                'dst_id': iana_qid,
                'props': [self.reference, row]
            })

        prefix_id = self.iyp.batch_get_nodes_by_single_prop('IANAPrefix', 'prefix', prefixes, all=False)
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'Prefix')

        for link in links:
            link['src_id'] = prefix_id[link['src_id']]

        self.iyp.batch_add_links('RESERVED', links)

    def unit_test(self):
        return super().unit_test(['ALLOCATED', 'RESERVED', 'LEGACY'])


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

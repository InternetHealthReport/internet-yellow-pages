import argparse
import csv
import logging
import sys
from datetime import datetime, timezone
from io import StringIO
from ipaddress import AddressValueError, ip_network

import requests

from iyp import BaseCrawler, RequestStatusError

# Organization name and URL to data
ORG = 'IANA'
URL = 'https://www.iana.org/assignments/ipv4-address-space/'
NAME = 'iana.address_space'


class Crawler(BaseCrawler):
    """Crawler for IANA IPv4 and IPv6 Address Space registries."""

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://www.iana.org/assignments'

        # URLs for all four data sources
        self.ipv4_general_url = (
            'https://www.iana.org/assignments/'
            'ipv4-address-space/ipv4-address-space.csv'
        )
        self.ipv6_general_url = (
            'https://www.iana.org/assignments/'
            'ipv6-unicast-address-assignments/'
            'ipv6-unicast-address-assignments.csv'
        )
        self.ipv4_special_url = (
            'https://www.iana.org/assignments/'
            'iana-ipv4-special-registry/'
            'iana-ipv4-special-registry-1.csv'
        )
        self.ipv6_special_url = (
            'https://www.iana.org/assignments/'
            'iana-ipv6-special-registry/'
            'iana-ipv6-special-registry-1.csv'
        )

    def run(self):
        """Fetch and process all IANA address space data."""
        self.reference['reference_time_fetch'] = datetime.now(timezone.utc)

        # Fetch data from all four sources
        ipv4_general_data = self._fetch_csv(self.ipv4_general_url)
        ipv6_general_data = self._fetch_csv(self.ipv6_general_url)
        ipv4_special_data = self._fetch_csv(self.ipv4_special_url)
        ipv6_special_data = self._fetch_csv(self.ipv6_special_url)

        # Process general allocations
        self._process_general(ipv4_general_data, self.ipv4_general_url, is_ipv6=False)
        self._process_general(ipv6_general_data, self.ipv6_general_url, is_ipv6=True)

        # Process special-purpose addresses
        self._process_special(ipv4_special_data, self.ipv4_special_url, is_ipv6=False)
        self._process_special(ipv6_special_data, self.ipv6_special_url, is_ipv6=True)

    def _fetch_csv(self, url):
        req = requests.get(url, timeout=30)
        if req.status_code != 200:
            raise RequestStatusError(f'Error fetching {url}')
        return list(csv.DictReader(StringIO(req.text)))

    def _normalize_organization_name(self, designation):
        """Normalize organization name from Designation field."""
        if not designation:
            return 'IANA'

        designation = designation.strip()

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

    def _parse_bool(self, value):
        return str(value).lower().startswith('true')

    def _process_general(self, csv_data, url, is_ipv6):
        """Process IPv4 or IPv6 general address space allocations."""
        if not csv_data:
            return

        ip_version = 'IPv6' if is_ipv6 else 'IPv4'
        prefixes = set()
        organizations = set()
        links_by_type = {'ALLOCATED': [], 'RESERVED': [], 'LEGACY': []}

        for row in csv_data:
            # Get prefix - column name is same for both
            prefix_str = row.get('Prefix', '').strip()
            if not prefix_str:
                continue
            designation = row.get('Designation', '').strip()

            # Get other fields
            date = row.get('Date', '').strip()
            whois = row.get('WHOIS', '').strip()
            rdap = row.get('RDAP', '').strip()
            status = (
                row.get('Status', '') or
                row.get('Status [1]', '')
            ).strip()

            if not status:
                continue

            # Convert IPv4 prefix format if needed (e.g., "001/8" to "1.0.0.0/8")
            if not is_ipv6 and '/' in prefix_str and '.' not in prefix_str:
                try:
                    octet, prefix_len = prefix_str.split('/')
                    prefix_str = f'{int(octet)}.0.0.0/{prefix_len}'
                except ValueError:
                    logging.warning(f'Skipping invalid {ip_version} prefix format: {prefix_str}')
                    continue

            # Normalize prefix
            try:
                normalized_prefix = str(ip_network(prefix_str))
            except (ValueError, AddressValueError) as e:
                logging.warning(f'Skipping invalid {ip_version} prefix {prefix_str}: {e}')
                continue

            # Normalize organization name
            org_name = self._normalize_organization_name(designation)

            # Record nodes
            prefixes.add(normalized_prefix)
            organizations.add(org_name)

            # Determine relationship type
            rel_type = status.upper()
            if rel_type not in links_by_type:
                links_by_type[rel_type] = []

            # Create relationship properties
            ref_copy = self.reference.copy()
            ref_copy['reference_url_data'] = url

            rel_props = {
                'designation': designation,
                'date': date,
                'whois': whois,
                'rdap': rdap,
                'status': status
            }
            rel_props.update(ref_copy)

            links_by_type[rel_type].append({
                'src_id': normalized_prefix,
                'dst_id': org_name,
                'props': [rel_props]
            })

        # Get/create nodes
        prefix_qid = self.iyp.batch_get_nodes_by_single_prop('IANAPrefix', 'prefix', prefixes, all=False)
        org_qid = self.iyp.batch_get_nodes_by_single_prop('Organization', 'name', organizations, all=False)

        # Replace IDs with QIDs and push links
        for rel_type, links in links_by_type.items():
            if not links:
                continue
            for link in links:
                link['src_id'] = prefix_qid[link['src_id']]
                link['dst_id'] = org_qid[link['dst_id']]
            self.iyp.batch_add_links(rel_type, links)

        logging.info(f'Processed {len(prefixes)} {ip_version} general prefixes')

    def _process_special(self, csv_data, url, is_ipv6):
        """Process IPv4 or IPv6 special-purpose addresses."""
        if not csv_data:
            return

        ip_version = 'IPv6' if is_ipv6 else 'IPv4'
        prefixes = set()
        organizations = {'IANA'}
        links = []

        for row in csv_data:
            # Get address block
            prefix_str = row.get('Address Block', '').strip()
            if not prefix_str:
                continue

            # Get other fields
            name = row.get('Name', '').strip()
            allocation = row.get('Allocation Date', '').strip()
            termination = row.get('Termination Date', '').strip()
            source = self._parse_bool(row.get('Source', ''))
            destination = self._parse_bool(row.get('Destination', ''))
            forwardable = self._parse_bool(row.get('Forwardable', ''))
            globally_reachable = self._parse_bool(row.get('Globally Reachable', ''))
            reserved_by_protocol = self._parse_bool(row.get('Reserved-by-Protocol', ''))

            # Normalize prefix
            try:
                normalized_prefix = str(ip_network(prefix_str))
            except (ValueError, AddressValueError) as e:
                logging.warning(f'Skipping invalid {ip_version} special prefix {prefix_str}: {e}')
                continue

            prefixes.add(normalized_prefix)

            # Create relationship properties
            ref_copy = self.reference.copy()
            ref_copy['reference_url_data'] = url

            rel_props = {
                'designation': name,
                'allocation_date': allocation,
                'termination_date': termination,
                'source': source,
                'destination': destination,
                'forwardable': forwardable,
                'globally_reachable': globally_reachable,
                'reserved_by_protocol': reserved_by_protocol,
                **ref_copy
            }

            links.append({
                'src_id': normalized_prefix,
                'dst_id': 'IANA',
                'props': [rel_props]
            })

        # Get/create nodes
        prefix_qid = self.iyp.batch_get_nodes_by_single_prop(
            'IANAPrefix', 'prefix', prefixes, all=False
        )
        org_qid = self.iyp.batch_get_nodes_by_single_prop(
            'Organization', 'name', organizations, all=False
        )

        # Replace IDs with QIDs and push links
        for link in links:
            link['src_id'] = prefix_qid[link['src_id']]
            link['dst_id'] = org_qid[link['dst_id']]

        self.iyp.batch_add_links('RESERVED', links)
        logging.info(f'Processed {len(prefixes)} {ip_version} special-purpose prefixes')

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

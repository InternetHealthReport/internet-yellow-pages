import argparse
import logging
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from ipaddress import AddressValueError, IPv4Network, IPv6Network

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
        self.ipv4_general_url = 'https://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.xml'
        self.ipv6_general_url = (
            'https://www.iana.org/assignments/'
            'ipv6-unicast-address-assignments/'
            'ipv6-unicast-address-assignments.xml'
        )
        self.ipv4_special_url = (
            'https://www.iana.org/assignments/'
            'iana-ipv4-special-registry/'
            'iana-ipv4-special-registry.xml'
        )
        self.ipv6_special_url = (
            'https://www.iana.org/assignments/'
            'iana-ipv6-special-registry/'
            'iana-ipv6-special-registry.xml'
        )

    def run(self):
        """Fetch and process all IANA address space data."""
        self.reference['reference_time_fetch'] = datetime.now(timezone.utc)

        # Fetch data from all four sources
        ipv4_general_data = self._fetch_xml(self.ipv4_general_url)
        ipv6_general_data = self._fetch_xml(self.ipv6_general_url)
        ipv4_special_data = self._fetch_xml(self.ipv4_special_url)
        ipv6_special_data = self._fetch_xml(self.ipv6_special_url)

        # Process general allocations
        self._process_ipv4_general(ipv4_general_data)
        self._process_ipv6_general(ipv6_general_data)

        # Process special-purpose addresses
        self._process_ipv4_special(ipv4_special_data)
        self._process_ipv6_special(ipv6_special_data)

    def _fetch_xml(self, url):
        """Fetch XML data from IANA."""
        req = requests.get(url, timeout=30)
        if req.status_code != 200:
            logging.error(f'Cannot download data from {url}: {req.status_code}')
            raise RequestStatusError(f'Error fetching data from {url}')
        return req.text

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

    def _process_ipv4_general(self, xml_data):
        """Process IPv4 general address space allocations."""
        if not xml_data:
            return

        try:
            root = ET.fromstring(xml_data)
            ns = {'iana': 'http://www.iana.org/assignments'}

            if root.attrib.get('id') != 'ipv4-address-space':
                logging.error(f'Unexpected registry id: {root.attrib.get("id")}')
                raise RequestStatusError('Invalid XML structure')

            prefixes = set()
            organizations = set()
            links_by_type = {'ALLOCATED': [], 'RESERVED': [], 'LEGACY': []}

            for record in root.findall('iana:record', ns):
                prefix_elem = record.find('iana:prefix', ns)
                designation_elem = record.find('iana:designation', ns)
                date_elem = record.find('iana:date', ns)
                whois_elem = record.find('iana:whois', ns)
                status_elem = record.find('iana:status', ns)

                if prefix_elem is None or status_elem is None:
                    continue

                prefix_str = prefix_elem.text.strip()
                designation = (
                    designation_elem.text.strip()
                    if designation_elem is not None and designation_elem.text
                    else ''
                )
                date = date_elem.text.strip() if date_elem is not None and date_elem.text else ''
                whois = whois_elem.text.strip() if whois_elem is not None and whois_elem.text else ''
                status = status_elem.text.strip()

                # Convert prefix format (e.g., "001/8" to "1.0.0.0/8")
                try:
                    octet, prefix_len = prefix_str.split('/')
                    ipv4_prefix = f'{int(octet)}.0.0.0/{prefix_len}'
                    normalized_prefix = str(IPv4Network(ipv4_prefix))
                except (ValueError, AddressValueError):
                    logging.warning(f'Skipping invalid IPv4 prefix: {prefix_str}')
                    continue

                # Normalize organization name
                org_name = self._normalize_organization_name(designation)

                # Record nodes
                prefixes.add(normalized_prefix)
                organizations.add(org_name)

                # Determine relationship type
                rel_type = status.upper()

                # Create relationship properties
                ref_copy = self.reference.copy()
                ref_copy['reference_url_data'] = self.ipv4_general_url

                rel_props = {
                    'designation': designation,
                    'date': date,
                    'whois': whois,
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

            logging.info(f'Processed {len(prefixes)} IPv4 general prefixes')

        except ET.ParseError as e:
            logging.error(f'Error parsing IPv4 general XML: {e}')
            raise RequestStatusError('XML parsing error')

    def _process_ipv6_general(self, xml_data):
        """Process IPv6 general unicast address allocations."""
        if not xml_data:
            return

        try:
            root = ET.fromstring(xml_data)
            ns = {'iana': 'http://www.iana.org/assignments'}

            if root.attrib.get('id') != 'ipv6-unicast-address-assignments':
                logging.error(f'Unexpected registry id: {root.attrib.get("id")}')
                raise RequestStatusError('Invalid XML structure')

            prefixes = set()
            organizations = set()
            links_by_type = {'ALLOCATED': [], 'RESERVED': [], 'LEGACY': []}

            for record in root.findall('iana:record', ns):
                prefix_elem = record.find('iana:prefix', ns)
                description_elem = record.find('iana:description', ns)
                whois_elem = record.find('iana:whois', ns)
                status_elem = record.find('iana:status', ns)

                if prefix_elem is None or status_elem is None:
                    continue

                prefix_str = prefix_elem.text.strip() if prefix_elem.text else ''
                description = description_elem.text.strip() if description_elem is not None else ''
                date = record.attrib.get('date', '')
                whois = whois_elem.text.strip() if whois_elem is not None and whois_elem.text else ''
                status = status_elem.text.strip()

                # Validate IPv6 prefix
                try:
                    network = IPv6Network(prefix_str)
                    normalized_prefix = str(network)
                except (ValueError, AddressValueError) as e:
                    logging.warning(f'Skipping invalid IPv6 prefix {prefix_str}: {e}')
                    continue

                # Normalize organization name
                org_name = self._normalize_organization_name(description)

                # Record nodes
                prefixes.add(normalized_prefix)
                organizations.add(org_name)

                # Determine relationship type
                rel_type = status.upper()

                # Create relationship properties
                ref_copy = self.reference.copy()
                ref_copy['reference_url_data'] = self.ipv6_general_url

                rel_props = {
                    'designation': description,
                    'date': date,
                    'whois': whois,
                    'status': status
                }
                rel_props.update(ref_copy)

                if rel_type not in links_by_type:
                    links_by_type[rel_type] = []

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

            logging.info(f'Processed {len(prefixes)} IPv6 general prefixes')

        except ET.ParseError as e:
            logging.error(f'Error parsing IPv6 general XML: {e}')
            raise RequestStatusError('XML parsing error')

    def _process_ipv4_special(self, xml_data):
        """Process IPv4 special-purpose addresses."""
        if not xml_data:
            return

        try:
            root = ET.fromstring(xml_data)
            ns = {'iana': 'http://www.iana.org/assignments'}

            # This registry IS nested (unlike IPv4/IPv6 general)
            registry = root.find(
                'iana:registry[@id="iana-ipv4-special-registry-1"]',
                ns
            )
            if registry is None:
                logging.error('Could not find IPv4 special-purpose registry')
                raise RequestStatusError('Invalid XML structure')

            prefixes = set()
            organizations = {'IANA'}
            links = []

            for record in registry.findall('iana:record', ns):
                address_elem = record.find('iana:address', ns)
                name_elem = record.find('iana:name', ns)
                allocation_elem = record.find('iana:allocation', ns)
                reserved_elem = record.find('iana:reserved', ns)
                spec_elem = record.find('iana:spec', ns)

                if address_elem is None or not address_elem.text:
                    continue

                prefix_str = address_elem.text.strip()
                name = name_elem.text.strip() if name_elem is not None and name_elem.text else ''
                allocation = (
                    allocation_elem.text.strip()
                    if allocation_elem is not None and allocation_elem.text
                    else ''
                )
                reserved = (
                    reserved_elem.text.strip().lower() == 'true'
                    if reserved_elem is not None and reserved_elem.text
                    else False
                )

                # Extract RFCs from <spec>
                rfcs = []
                if spec_elem is not None:
                    for xref in spec_elem.findall('iana:xref', ns):
                        if xref.attrib.get('type') == 'rfc':
                            rfcs.append(xref.attrib.get('data'))

                try:
                    normalized_prefix = str(IPv4Network(prefix_str))
                except (ValueError, AddressValueError) as e:
                    logging.warning(f'Skipping invalid IPv4 special prefix {prefix_str}: {e}')
                    continue

                prefixes.add(normalized_prefix)

                ref_copy = self.reference.copy()
                ref_copy['reference_url_data'] = self.ipv4_special_url

                rel_props = {
                    'designation': name,
                    'allocation_date': allocation,
                    'reserved': reserved,
                    'rfcs': rfcs,
                    **ref_copy
                }

                links.append({
                    'src_id': normalized_prefix,
                    'dst_id': 'IANA',
                    'props': [rel_props]
                })

            prefix_qid = self.iyp.batch_get_nodes_by_single_prop(
                'IANAPrefix', 'prefix', prefixes, all=False
            )
            org_qid = self.iyp.batch_get_nodes_by_single_prop(
                'Organization', 'name', organizations, all=False
            )

            for link in links:
                link['src_id'] = prefix_qid[link['src_id']]
                link['dst_id'] = org_qid[link['dst_id']]

            self.iyp.batch_add_links('RESERVED', links)
            logging.info(f'Processed {len(prefixes)} IPv4 special-purpose prefixes')

        except ET.ParseError as e:
            logging.error(f'Error parsing IPv4 special XML: {e}')
            raise RequestStatusError('XML parsing error')

    def _process_ipv6_special(self, xml_data):
        """Process IPv6 special-purpose addresses."""
        if not xml_data:
            return

        try:
            root = ET.fromstring(xml_data)
            ns = {'iana': 'http://www.iana.org/assignments'}

            # Nested registry is required here
            registry = root.find(
                'iana:registry[@id="iana-ipv6-special-registry-1"]',
                ns
            )
            if registry is None:
                logging.error('Could not find IPv6 special-purpose registry')
                raise RequestStatusError('Invalid XML structure')

            prefixes = set()
            organizations = {'IANA'}
            links = []

            for record in registry.findall('iana:record', ns):
                address_elem = record.find('iana:address', ns)
                name_elem = record.find('iana:name', ns)
                allocation_elem = record.find('iana:allocation', ns)
                reserved_elem = record.find('iana:reserved', ns)
                spec_elem = record.find('iana:spec', ns)

                if address_elem is None or not address_elem.text:
                    continue

                prefix_str = address_elem.text.strip()
                name = name_elem.text.strip() if name_elem is not None and name_elem.text else ''
                allocation = (
                    allocation_elem.text.strip()
                    if allocation_elem is not None and allocation_elem.text
                    else ''
                )
                reserved = (
                    reserved_elem.text.strip().lower() == 'true'
                    if reserved_elem is not None and reserved_elem.text
                    else False
                )

                # Extract RFCs from <spec>
                rfcs = []
                if spec_elem is not None:
                    for xref in spec_elem.findall('iana:xref', ns):
                        if xref.attrib.get('type') == 'rfc':
                            rfcs.append(xref.attrib.get('data'))

                # Validate IPv6 prefix
                try:
                    normalized_prefix = str(IPv6Network(prefix_str))
                except (ValueError, AddressValueError) as e:
                    logging.warning(f'Skipping invalid IPv6 special prefix {prefix_str}: {e}')
                    continue

                prefixes.add(normalized_prefix)

                ref_copy = self.reference.copy()
                ref_copy['reference_url_data'] = self.ipv6_special_url

                rel_props = {
                    'designation': name,
                    'allocation_date': allocation,
                    'reserved': reserved,
                    'rfcs': rfcs,
                    **ref_copy
                }

                links.append({
                    'src_id': normalized_prefix,
                    'dst_id': 'IANA',
                    'props': [rel_props]
                })

            prefix_qid = self.iyp.batch_get_nodes_by_single_prop(
                'IANAPrefix', 'prefix', prefixes, all=False
            )
            org_qid = self.iyp.batch_get_nodes_by_single_prop(
                'Organization', 'name', organizations, all=False
            )

            for link in links:
                link['src_id'] = prefix_qid[link['src_id']]
                link['dst_id'] = org_qid[link['dst_id']]

            self.iyp.batch_add_links('RESERVED', links)
            logging.info(f'Processed {len(prefixes)} IPv6 special-purpose prefixes')

        except ET.ParseError as e:
            logging.error(f'Error parsing IPv6 special XML: {e}')
            raise RequestStatusError('XML parsing error')

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

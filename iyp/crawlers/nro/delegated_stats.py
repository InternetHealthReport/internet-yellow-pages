import argparse
import logging
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from ipaddress import IPv4Address, IPv4Network

import requests

from iyp import BaseCrawler, RequestStatusError

# NOTE: this script is not adding new ASNs. It only adds links for existing ASNs
# Should be run after crawlers that push many ASNs (e.g. ripe.as_names)

URL = 'https://ftp.ripe.net/pub/stats/ripencc/nro-stats/latest/nro-delegated-stats'
ORG = 'NRO'
NAME = 'nro.delegated_stats'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://www.nro.net/wp-content/uploads/nro-extended-stats-readme5.txt'

    @staticmethod
    def ffs(x):
        """Returns the index, counting from 0, of the least significant set bit in
        `x`."""
        return (x & -x).bit_length() - 1

    @staticmethod
    def decompose_prefix(ip, hosts):
        # First address of this range
        start = IPv4Address(ip)
        # Last address of this range
        stop = start + hosts - 1
        remaining = int.from_bytes(stop.packed, byteorder='big') - int.from_bytes(start.packed, byteorder='big') + 1
        next_address = start
        while remaining > 0:
            # Get the largest possible prefix length by checking the last bit set.
            next_address_packed = int.from_bytes(next_address.packed, byteorder='big')
            first_bit = Crawler.ffs(next_address_packed)
            # Get the number of host bits required to cover the remaining addresses. If
            # remaining is not a power of 2, round down to not cover too much.
            required_host_bits = int(math.log2(remaining))
            # Due to the last bit set in next_address, we can not choose more host bits,
            # only less.
            host_bits = min(first_bit, required_host_bits)
            next_prefix = IPv4Network(f'{next_address}/{32 - host_bits}')
            remaining -= next_prefix.num_addresses
            next_address = next_prefix.broadcast_address + 1
            yield str(next_prefix)

    def run(self):
        """Fetch the delegated stat file from RIPE website and process lines one by
        one."""

        req = requests.get(URL)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching delegated file')

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn')
        asn_in_iyp = sorted(asn_id.keys())

        # Read delegated-stats file. see documentation:
        # https://www.nro.net/wp-content/uploads/nro-extended-stats-readme5.txt
        self.fields_name = ['registry', 'cc', 'type', 'start', 'value', 'date', 'status', 'opaque-id']

        opaqueids = set()
        prefixes = set()
        countries = set()
        asn_country_links = list()
        prefix_country_links = list()
        asn_status_links = defaultdict(list)
        prefix_status_links = defaultdict(list)

        logging.info('Parsing file...')
        for line in req.text.splitlines():
            # Skip comments.
            if line.strip().startswith('#'):
                continue

            fields_value = line.split('|')
            # Get modification time from version line.
            if len(fields_value) == 7 and fields_value[0].isdigit():
                try:
                    date = datetime.strptime(fields_value[5], '%Y%m%d').replace(tzinfo=timezone.utc)
                    self.reference['reference_time_modification'] = date
                except ValueError as e:
                    logging.warning(f'Failed to set modification time: {e}')
            # Skip summary lines.
            if len(fields_value) < 8:
                continue

            # Parse record lines.
            rec = dict(zip(self.fields_name, fields_value))
            rec['value'] = int(rec['value'])
            rec['status'] = rec['status'].upper()

            additional_props = {'registry': rec['registry']}

            if rec['type'] == 'asn':
                start_asn = int(rec['start'])
                asns_to_link = list()
                if rec['value'] == 1 and start_asn in asn_id:
                    # Fast path.
                    asns_to_link = [start_asn]
                elif rec['value'] > 1:
                    as_range_start = start_asn
                    as_range_end = start_asn + rec['value'] - 1
                    # Get overlap between existing ASes and the AS range specified in
                    # the record.
                    asns_to_link = [a for a in asn_in_iyp if as_range_start <= a <= as_range_end]
                if asns_to_link:
                    # Only add if ASN is already present.
                    countries.add(rec['cc'])
                    opaqueids.add(rec['opaque-id'])
                    for i in asns_to_link:
                        asn_qid = asn_id[i]
                        asn_country_links.append({'src_id': asn_qid,
                                                  'dst_id': rec['cc'],
                                                  'props': [self.reference, additional_props]})
                        asn_status_links[rec['status']].append({'src_id': asn_qid,
                                                                'dst_id': rec['opaque-id'],
                                                                'props': [self.reference, additional_props]})
            elif rec['type'] == 'ipv4' or rec['type'] == 'ipv6':
                countries.add(rec['cc'])
                opaqueids.add(rec['opaque-id'])
                # Compute the prefix length.
                # 'value' is a CIDR prefix length for IPv6, but a count of hosts for
                # IPv4.
                start = rec['start']
                prefix_len = rec['value']
                if rec['type'] == 'ipv4':
                    # Some IPv4 prefixes are not CIDR aligned.
                    # Either their size is not a power of two and/or their start address
                    # is not aligned with their size.
                    needs_decomposition = False
                    prefix_len = 32 - math.log2(rec['value'])
                    if not prefix_len.is_integer():
                        needs_decomposition = True
                    else:
                        # Size is CIDR aligned
                        prefix = f'{start}/{int(prefix_len)}'
                        try:
                            IPv4Network(prefix)
                        except ValueError:
                            # Start address is not aligned.
                            needs_decomposition = True
                    if needs_decomposition:
                        # Decompose into CIDR prefixes.
                        record_prefixes = [prefix for prefix in self.decompose_prefix(start, rec['value'])]
                    else:
                        # Valid prefix, no decomposition required.
                        record_prefixes = [prefix]
                else:
                    # IPv6 prefixes are always CIDR aligned.
                    prefix = f'{start}/{prefix_len}'
                    record_prefixes = [prefix]
                # Add prefix(es) to IYP set.
                prefixes.update(record_prefixes)
                for prefix in record_prefixes:
                    # Create links for prefix(es)
                    prefix_country_links.append({'src_id': prefix,
                                                 'dst_id': rec['cc'],
                                                 'props': [self.reference, additional_props]})
                    prefix_status_links[rec['status']].append({'src_id': prefix,
                                                               'dst_id': rec['opaque-id'],
                                                               'props': [self.reference, additional_props]})

        # Create all nodes
        opaqueid_id = self.iyp.batch_get_nodes_by_single_prop('OpaqueID', 'id', opaqueids, all=False)
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries, all=False)

        # Replace with QIDs
        for link in asn_country_links:
            link['dst_id'] = country_id[link['dst_id']]
        for link in prefix_country_links:
            link['src_id'] = prefix_id[link['src_id']]
            link['dst_id'] = country_id[link['dst_id']]
        for links in asn_status_links.values():
            for link in links:
                link['dst_id'] = opaqueid_id[link['dst_id']]
        for links in prefix_status_links.values():
            for link in links:
                link['src_id'] = prefix_id[link['src_id']]
                link['dst_id'] = opaqueid_id[link['dst_id']]

        # Push all links to IYP
        self.iyp.batch_add_links('COUNTRY', asn_country_links)
        self.iyp.batch_add_links('COUNTRY', prefix_country_links)
        for label, links in asn_status_links.items():
            self.iyp.batch_add_links(label, links)
        for label, links in prefix_status_links.items():
            self.iyp.batch_add_links(label, links)

    def unit_test(self):
        return super().unit_test(['AVAILABLE', 'ASSIGNED', 'RESERVED', 'COUNTRY'])


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

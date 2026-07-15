import argparse
import bz2
import ipaddress
import logging
import os
import sys
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from iyp import BaseCrawler

URL = 'https://publicdata.caida.org/datasets/topology/ark/ipv4/itdk/'
ORG = 'CAIDA'
NAME = 'caida.itdk'


def map_links(links: list, src_map: dict = dict(), dst_map: dict = dict()):
    for link in links:
        if src_map:
            link['src'] = src_map[link['src']]
        if dst_map:
            link['dst'] = dst_map[link['dst']]


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.node_ids = list()
        self.link_ids = list()
        self.ips = list()
        self.asns = set()
        self.hostnames = set()
        self.assigned_links = list()
        self.node_part_of_links = list()
        self.ip_part_of_links = list()
        self.managed_by_links = list()
        self.resolves_to_links = list()
        self.placeholder_networks = {
            4: ipaddress.IPv4Network('224.0.0.0/3'),
            6: ipaddress.IPv6Network('ff00::/8'),
        }

    def __get_newest_folder(self) -> str:
        res = requests.get(URL)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, features='html.parser')
        newest_date = None
        newest_link = str()
        for link in soup.find_all('a'):
            href = link['href']
            try:
                date = datetime.strptime(href, '%Y-%m/')
            except ValueError:
                continue
            if newest_date is None or date > newest_date:
                newest_date = date
                newest_link = os.path.join(URL, href)
        self.reference['reference_time_modification'] = newest_date.replace(tzinfo=timezone.utc)
        self.reference['reference_url_info'] = newest_link
        self.reference['reference_url_data'] = newest_link
        return newest_link

    def fetch(self):
        tmp_dir = self.create_tmp_dir()
        newest_link = self.__get_newest_folder()
        logging.info(f'Getting data from {newest_link}')
        res = requests.get(newest_link)
        soup = BeautifulSoup(res.text, features='html.parser')
        res.raise_for_status()
        for link in soup.find_all('a'):
            href = link['href']
            if (not href.endswith('.bz2')
                or href.endswith('.ifaces.bz2')
                    or href.endswith('.addrs.bz2')):
                continue
            file_link = os.path.join(newest_link, href)
            logging.info(f'Fetching {file_link}')
            res = requests.get(file_link)
            res.raise_for_status()
            with open(os.path.join(tmp_dir, href), 'wb') as f:
                f.write(res.content)

    def parse_nodes_file(self, file: str, ip_version: int):
        logging.info(f'Parsing IPv{ip_version} nodes file: {file}')
        if ip_version == 4:
            ip_address_class = ipaddress.IPv4Address
        else:
            ip_address_class = ipaddress.IPv6Address
        with bz2.open(file, 'rt') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                line_split = line.split()
                node_id = line_split[1].rstrip(':')
                interface_ips = line_split[2:]
                parsed_ips = list()
                for ip in interface_ips:
                    try:
                        ip_parsed = ip_address_class(ip)
                    except ValueError as e:
                        logging.warning(f'Skipping invalid IP: {e}')
                        continue
                    if ip_parsed in self.placeholder_networks[ip_version]:
                        continue
                    parsed_ips.append(ip_parsed.compressed)
                node_id = f'v{ip_version}-{node_id.lower()}'
                self.node_ids.append(node_id)
                self.ips.extend(parsed_ips)
                for ip in parsed_ips:
                    self.assigned_links.append({'src': ip, 'dst': node_id, 'props': [self.reference]})

    def parse_links_file(self, file: str, ip_version: int):
        logging.info(f'Parsing IPv{ip_version} links file: {file}')
        if ip_version == 4:
            ip_address_class = ipaddress.IPv4Address
        else:
            ip_address_class = ipaddress.IPv6Address
        with bz2.open(file, 'rt') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                line_split = line.split()
                link_id = line_split[1].rstrip(':')
                link_entries = line_split[2:]
                link_node_ids = list()
                parsed_ips = list()
                for link_entry in link_entries:
                    if ':' not in link_entry:
                        # Only node id.
                        link_node_ids.append(link_entry)
                        continue
                    node_id, ip = link_entry.split(':', maxsplit=1)
                    link_node_ids.append(node_id)
                    try:
                        ip_parsed = ip_address_class(ip)
                    except ValueError as e:
                        logging.warning(f'Skipping invalid IP: {e}')
                        continue
                    if ip_parsed in self.placeholder_networks[ip_version]:
                        continue
                    parsed_ips.append(ip_parsed.compressed)
                link_id = f'v{ip_version}-{link_id.lower()}'
                self.link_ids.append(link_id)
                for ip in parsed_ips:
                    self.ip_part_of_links.append({'src': ip, 'dst': link_id, 'props': [self.reference]})
                for node_id in link_node_ids:
                    node_id = f'v{ip_version}-{node_id.lower()}'
                    self.node_part_of_links.append({'src': node_id, 'dst': link_id, 'props': [self.reference]})

    def parse_nodes_as_file(self, file: str, ip_version: int):
        logging.info(f'Parsing IPv{ip_version} nodes-AS file: {file}')
        with bz2.open(file, 'rt') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                line_split = line.split()
                node_id = line_split[1]
                asn = int(line_split[2])
                heuristic = line_split[3]
                node_id = f'v{ip_version}-{node_id.lower()}'
                self.asns.add(asn)
                self.managed_by_links.append({'src': node_id,
                                              'dst': asn,
                                              'props': [self.reference,
                                                        {'heuristic': heuristic}]})

    def parse_dns_names_file(self, file: str):
        logging.info(f'Parsing DNS names file: {file}')
        with bz2.open(file, 'rt') as f:
            for line in f:
                line_split = line.strip().split('\t')
                if len(line_split) < 3:
                    continue
                ip = line_split[1]
                hostname = line_split[2]
                if hostname in [
                    'FAIL.NON-AUTHORITATIVE.in-addr.arpa',
                    'FAIL.SERVER-FAILURE.in-addr.arpa',
                    'error.arpa',
                ]:
                    continue
                self.hostnames.add(hostname)
                try:
                    ip_parsed = ipaddress.ip_address(ip)
                except ValueError as e:
                    logging.warning(f'Skipping invalid IP: {e}')
                    continue
                self.resolves_to_links.append({'src': hostname, 'dst': ip_parsed.compressed, 'props': [self.reference]})

    def run(self):
        self.fetch()
        tmp_dir = self.get_tmp_dir()
        for entry in os.scandir(tmp_dir):
            file_path = os.path.join(tmp_dir, entry.name)
            if entry.name.endswith('dns-names.txt.bz2'):
                self.parse_dns_names_file(file_path)
            elif entry.name.endswith('.nodes.bz2'):
                if entry.name.startswith('midar'):
                    self.parse_nodes_file(file_path, 4)
                elif entry.name.startswith('speedtrap'):
                    self.parse_nodes_file(file_path, 6)
                else:
                    logging.error(f'Invalid nodes file name: {file_path}')
            elif entry.name.endswith('.nodes.as.bz2'):
                if entry.name.startswith('midar'):
                    self.parse_nodes_as_file(file_path, 4)
                elif entry.name.startswith('speedtrap'):
                    self.parse_nodes_as_file(file_path, 6)
                else:
                    logging.error(f'Invalid nodes-as file name: {file_path}')
            elif entry.name.endswith('.links.bz2'):
                if entry.name.startswith('midar'):
                    self.parse_links_file(file_path, 4)
                elif entry.name.startswith('speedtrap'):
                    self.parse_links_file(file_path, 6)
                else:
                    logging.error(f'Invalid links file name: {file_path}')

        router_id = self.iyp.batch_get_nodes_by_single_prop('Router',
                                                            'id',
                                                            set(self.node_ids),
                                                            all=False,
                                                            batch_size=10000)
        router_link_id = self.iyp.batch_get_nodes_by_single_prop('RouterLink',
                                                                 'id',
                                                                 set(self.link_ids),
                                                                 all=False,
                                                                 batch_size=10000)
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', self.asns, all=True)
        ip_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', set(self.ips), all=False, batch_size=10000)
        hostname_id = self.iyp.batch_get_nodes_by_single_prop('HostName',
                                                              'name',
                                                              self.hostnames,
                                                              all=False,
                                                              batch_size=10000)

        map_links(self.assigned_links, ip_id, router_id)
        map_links(self.node_part_of_links, router_id, router_link_id)
        map_links(self.ip_part_of_links, ip_id, router_link_id)
        map_links(self.managed_by_links, router_id, asn_id)
        for link in self.resolves_to_links:
            # There seem to be DNS lookups for IPs that are not in any link or the nodes
            # file, i.e., we do not know to which node they belong. Would be dangling,
            # so ignore.
            if link['dst'] not in ip_id:
                continue
            link['src'] = hostname_id[link['src']]
            link['dst'] = ip_id[link['dst']]
        # map_links(self.resolves_to_links, hostname_id, ip_id)

        self.iyp.batch_add_links('ASSIGNED', self.assigned_links)
        self.iyp.batch_add_links('PART_OF', self.node_part_of_links)
        self.iyp.batch_add_links('PART_OF', self.ip_part_of_links)
        self.iyp.batch_add_links('MANAGED_BY', self.managed_by_links)
        self.iyp.batch_add_links('RESOLVES_TO', self.resolves_to_links)

    def unit_test(self):
        return super().unit_test(['ASSIGNED', 'PART_OF', 'MANAGED_BY', 'RESOLVES_TO'])


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

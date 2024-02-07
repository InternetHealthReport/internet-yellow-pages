import argparse
import ipaddress
import json
import logging
import os
import sys

import flatdict
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from iyp import (BaseCrawler, JSONDecodeError, MissingKeyError,
                 RequestStatusError)

ORG = 'RIPE NCC'

URL = 'https://atlas.ripe.net/api/v2/measurements'
NAME = 'ripe.atlas_measurements'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        self.__initialize_session()
        super().__init__(organization, url, name)

    def __initialize_session(self) -> None:
        self.session = Session()
        retry = Retry(
            backoff_factor=0.1,
            status_forcelist=(429, 500, 502, 503, 504),
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    @staticmethod
    def __process_response(response: requests.Response):
        if response.status_code != requests.codes.ok:
            raise RequestStatusError(f'Request to {response.url} failed with status: {response.status_code}')
        try:
            data = response.json()
        except json.decoder.JSONDecodeError as e:
            raise JSONDecodeError(f'Decoding JSON reply from {response.url} failed with exception: {e}')
        if 'next' not in data or 'results' not in data:
            raise MissingKeyError('"next" or "results" key missing from response data.')

        next_url = data['next']
        if not next_url:
            logging.info('Reached end of list')
            next_url = str()
        return next_url, data['results']

    def __execute_query(self, url: str):
        logging.info(f'Querying {url}')
        r = self.session.get(url)
        return self.__process_response(r)

    @staticmethod
    def __transform_data(data):
        for item in data:
            # Convert empty lists to None as flatdict library converts it automatically
            # to FlatterDict object which would cause a problem when
            # loading data in neo4j.
            for key in item:
                if isinstance(item[key], list) and len(item[key]) == 0:
                    item[key] = None

            # Flatten the target information since the original data has multiple fields
            # prefixed with 'target_'. Given that we flatten the dict
            # on the '_' delimiter, this action would potentially
            # cause a TypeError from flatdict if it isn't handled properly.
            target_info = {
                'hostname': item.pop('target', None),
                'asn': item.pop('target_asn', None),
                'ip': item.pop('target_ip', None),
                'prefix': item.pop('target_prefix', None),
                'resolved_ips': item.pop('resolved_ips', None)
            }
            item['target'] = target_info

            # Flatten the group information, They are the same as target
            # information as they are prefixed with 'group_'.
            group_info = {
                'value': item.pop('group', None),
                'id': item.pop('group_id', None)
            }
            item['group'] = group_info

    @staticmethod
    def __is_valid_ip(ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    @staticmethod
    def __get_all_resolved_ips(probe_measurement):
        # Ensure 'resolved_ips' takes precedence over 'target_ip.
        resolved_ips = probe_measurement['target']['resolved_ips'] or probe_measurement['target']['ip'] or []
        resolved_ips = [resolved_ips] if not isinstance(resolved_ips, list) else resolved_ips
        valid_resolved_ips = [ip for ip in resolved_ips if ip is not None and ip != '']
        return valid_resolved_ips

    @staticmethod
    def __add_if_not_none(v, s: set):
        if v is not None and v != '' and v not in s:
            s.add(v)

    @staticmethod
    def __add_if_not_none_values(lst, s: set):
        if not isinstance(lst, list):
            return
        for item in lst:
            Crawler.__add_if_not_none(item, s)

    def __get_abandoned_probe_ids(self) -> set:
        # status_id
        #   0: Never Connected
        #   3: Abandoned
        query = """MATCH (n:AtlasProbe)
                   WHERE n.status_id IN [0, 3]
                   RETURN n.id AS prb_id"""
        return set(e['prb_id'] for e in self.iyp.tx.run(query))

    def run(self):
        params = {'format': 'json',
                  'is_public': True,
                  'status': 2,
                  'optional_fields': 'current_probes',
                  'page_size': 500}
        r = self.session.get(URL, params=params)
        next_url, data = self.__process_response(r)
        while next_url:
            next_url, next_data = self.__execute_query(next_url)
            data += next_data
            logging.info(f'Added {len(next_data)} measurements. Total: {len(data)}')
        print(f'Fetched {len(data)} measurements', file=sys.stderr)

        # Transform the data to be compatible with the flatdict format.
        self.__transform_data(data)

        # Compute nodes
        probe_measurement_ids = set()
        probe_ids = set()
        ips = set()
        ases = set()
        hostnames = set()

        valid_probe_measurements = list()

        # To reduce the number of PART_OF relationships, we do not consider probes that
        # were never connected or are abandoned.
        abandoned_prb_ids = self.__get_abandoned_probe_ids()
        logging.info(f'Fetched {len(abandoned_prb_ids)} abandoned probe IDs.')

        for probe_measurement in data:
            probe_measurement_id = probe_measurement['id']
            if not probe_measurement_id:
                logging.error(f'Probe Measurement without ID. Should never happen: {probe_measurement}.')
                continue
            if probe_measurement_id in probe_measurement_ids:
                logging.warning(f'Duplicate probe measurement ID: {probe_measurement_id}.')
                continue

            resolved_ips = self.__get_all_resolved_ips(probe_measurement)
            for i in range(len(resolved_ips)):
                probe_af = int(probe_measurement['af'])
                resolved_ips[i] = ipaddress.ip_address(resolved_ips[i]).compressed if probe_af == 6 else resolved_ips[i]

            hostname = probe_measurement['target']['hostname']
            if hostname == '' or self.__is_valid_ip(hostname):
                hostname = None
                probe_measurement['target']['hostname'] = None

            asn = probe_measurement['target']['asn']
            probe_ids_participated = probe_measurement['current_probes']

            self.__add_if_not_none(probe_measurement_id, probe_measurement_ids)
            self.__add_if_not_none(hostname, hostnames)
            self.__add_if_not_none(asn, ases)
            self.__add_if_not_none_values(resolved_ips, ips)
            self.__add_if_not_none_values(probe_ids_participated, probe_ids)

            valid_probe_measurements.append(probe_measurement)

        # push nodes
        logging.info('Fetching/pushing nodes')
        probe_measurement_ids = dict()

        attrs_flattened = []
        for probe_measurement in valid_probe_measurements:
            probe_measurement_copy = probe_measurement.copy()
            del probe_measurement_copy['current_probes']
            probe_measurement_flattened = dict(flatdict.FlatterDict(probe_measurement_copy, delimiter='_'))
            attrs_flattened.append(probe_measurement_flattened)

        logging.info(f'{len(attrs_flattened)} measurements')
        probe_measurement_ids = self.iyp.batch_get_nodes('AtlasMeasurement', attrs_flattened, ['id'], create=True)
        logging.info(f'{len(probe_ids)} probes')
        probe_ids = self.iyp.batch_get_nodes_by_single_prop('AtlasProbe', 'id', probe_ids, all=False, create=True)
        logging.info(f'{len(ips)} IPs')
        ip_ids = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', ips, all=False, create=True)
        logging.info(f'{len(hostnames)} hostnames')
        hostname_ids = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', hostnames, all=False, create=True)
        logging.info(f'{len(ases)} ASNs')
        asn_ids = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', ases, all=False, create=True)

        # compute links
        target_links = list()
        part_of_links = list()

        logging.info('Computing links')
        for probe_measurement in valid_probe_measurements:
            probe_measurement_qid = probe_measurement_ids[probe_measurement['id']]
            probe_measurement_reference = self.reference.copy()
            probe_measurement_reference['reference_url'] = probe_measurement_reference['reference_url'] + \
                f'/{probe_measurement["id"]}'

            probe_measurement_asn = probe_measurement['target']['asn']
            if probe_measurement_asn:
                asn_qid = asn_ids[probe_measurement_asn]
                target_links.append({'src_id': probe_measurement_qid, 'dst_id': asn_qid,
                                    'props': [probe_measurement_reference]})

            probe_measurement_hostname = probe_measurement['target']['hostname']
            if probe_measurement_hostname:
                hostname_qid = hostname_ids[probe_measurement_hostname]
                target_links.append({'src_id': probe_measurement_qid, 'dst_id': hostname_qid,
                                    'props': [probe_measurement_reference]})

            probe_measurement_ips = self.__get_all_resolved_ips(probe_measurement)
            for probe_measurement_ip in probe_measurement_ips:
                ip_qid = ip_ids[probe_measurement_ip]
                target_links.append({'src_id': probe_measurement_qid, 'dst_id': ip_qid,
                                    'props': [probe_measurement_reference]})

            probe_ids_participated = probe_measurement['current_probes']
            if probe_ids_participated:
                for probe_id in probe_ids_participated:
                    if probe_id in abandoned_prb_ids:
                        continue
                    probe_qid = probe_ids[probe_id]
                    part_of_links.append({'src_id': probe_qid, 'dst_id': probe_measurement_qid,
                                          'props': [probe_measurement_reference]})

        # Push all links to IYP
        logging.info('Fetching/pushing relationships')
        logging.info(f'{len(target_links)} TARGET')
        self.iyp.batch_add_links('TARGET', target_links)
        logging.info(f'{len(part_of_links)} PART_OF')
        self.iyp.batch_add_links('PART_OF', part_of_links)
        logging.info('Done.')


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
    print('Fetching RIPE Atlas probe measurements', file=sys.stderr)

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

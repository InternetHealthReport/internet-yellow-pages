import argparse
import ipaddress
import json
import logging
import sys

import flatdict
import iso3166
import requests
from neo4j.spatial import WGS84Point
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from iyp import BaseCrawler, MissingKeyError, RequestStatusError

ORG = 'RIPE NCC'

URL = 'https://atlas.ripe.net/api/v2/probes'
NAME = 'ripe.atlas_probes'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        self.__initialize_session()
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://atlas.ripe.net/docs/apis/rest-api-manual/probes/'
        # Atlas API is real-time, i.e., we can use the same timestamp.
        self.reference['reference_time_modification'] = self.reference['reference_time_fetch']

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
            raise RequestStatusError(f'Decoding JSON reply from {response.url} failed with exception: {e}')
        if 'next' not in data or 'results' not in data:
            raise MissingKeyError('"next" or "results" key missing from response data.')

        next_url = data['next']
        if not next_url:
            logging.debug('Reached end of list')
            next_url = str()
        return next_url, data['results']

    def __execute_query(self, url: str):
        logging.debug(f'Querying {url}')
        r = self.session.get(url)
        return self.__process_response(r)

    @staticmethod
    def __add_if_not_none(v, s: set):
        if v and v not in s:
            s.add(v)

    def run(self):
        params = {'format': 'json',
                  'is_public': True,
                  'page_size': 500}
        r = self.session.get(URL, params=params)
        next_url, data = self.__process_response(r)
        while next_url:
            next_url, next_data = self.__execute_query(next_url)
            data += next_data
            logging.debug(f'Added {len(next_data)} probes. Total: {len(data)}')
        logging.info(f'Fetched {len(data)} probes.')

        # Compute nodes
        probe_ids = set()
        ips = set()
        ases = set()
        countries = set()
        points = set()

        valid_probes = list()

        for probe in data:
            probe_id = probe['id']
            if not probe_id:
                logging.error(f'Probe without ID. Should never happen: {probe}')
                continue
            if probe_id in probe_ids:
                logging.warning(f'Duplicate probe ID: {probe_id}. Probably caused by changing probe connectivity while '
                                'fetching.')
                continue

            ipv4 = probe['address_v4']
            # Ensure proper IP formatting.
            ipv6 = probe['address_v6']
            if ipv6:
                ipv6 = ipaddress.ip_address(ipv6).compressed
                probe['address_v6'] = ipv6
            asv4 = probe['asn_v4']
            asv6 = probe['asn_v6']

            probe_ids.add(probe_id)
            valid_probes.append(probe)
            self.__add_if_not_none(ipv4, ips)
            self.__add_if_not_none(asv4, ases)
            self.__add_if_not_none(ipv6, ips)
            self.__add_if_not_none(asv6, ases)

            country_code = probe['country_code']
            if country_code:
                if country_code in iso3166.countries_by_alpha2:
                    countries.add(country_code)
                else:
                    logging.warning(f'Skipping creation of COUNTRY relationship of probe {probe["id"]} due to non-ISO '
                                    f'country code: {country_code}')
            else:
                # Our country_code property formatter does not like None objects, so
                # remove the property instead.
                probe.pop('country_code')

            if probe['geometry']:
                geo_coordinates = probe['geometry']['coordinates']
                if geo_coordinates:
                    long, lat = geo_coordinates
                    # There is at least one probe with invalid coordinates.
                    if long < -180 or long > 180 or lat < -90 or long > 90:
                        probe['geometry'] = None
                    else:
                        points.add(WGS84Point((long, lat)))

            # An empty tag list causes a FlatterDict instance to remain in the object,
            # which neo4j does not like.
            if not probe['tags']:
                probe.pop('tags')

        # push nodes
        probe_id = dict()
        # Each probe is a JSON object with nested fields, so we need to flatten it.
        flattened_probes = [dict(flatdict.FlatterDict(probe, delimiter='_')) for probe in valid_probes]
        probe_id = self.iyp.batch_get_nodes('AtlasProbe', flattened_probes, ['id'])
        ip_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', ips, all=False)
        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', ases, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)
        point_id = self.iyp.batch_get_nodes_by_single_prop('Point', 'position', points)

        # compute links
        assigned_links = list()
        located_in_as_links = list()
        located_in_point_links = list()
        country_links = list()

        for probe in valid_probes:
            probe_qid = probe_id[probe['id']]

            ipv4 = probe['address_v4']
            if ipv4:
                ip_qid = ip_id[ipv4]
                assigned_links.append({'src_id': ip_qid, 'dst_id': probe_qid, 'props': [self.reference]})

            ipv6 = probe['address_v6']
            if ipv6:
                ip_qid = ip_id[ipv6]
                assigned_links.append({'src_id': ip_qid, 'dst_id': probe_qid, 'props': [self.reference]})

            asv4 = probe['asn_v4']
            if asv4:
                as_qid = as_id[asv4]
                located_in_as_links.append(
                    {'src_id': probe_qid, 'dst_id': as_qid, 'props': [self.reference, {'af': 4}]})

            asv6 = probe['asn_v6']
            if asv6:
                as_qid = as_id[asv6]
                located_in_as_links.append(
                    {'src_id': probe_qid, 'dst_id': as_qid, 'props': [self.reference, {'af': 6}]})

            if ('country_code' in probe
                and (country_code := probe['country_code'])
                    and country_code in iso3166.countries_by_alpha2):
                country_qid = country_id[country_code]
                country_links.append({'src_id': probe_qid, 'dst_id': country_qid,
                                     'props': [self.reference]})
            if probe['geometry']:
                geo_coordinates = probe['geometry']['coordinates']
                if geo_coordinates:
                    position = WGS84Point((geo_coordinates[0], geo_coordinates[1]))
                    point_qid = point_id[position]
                    located_in_point_links.append(
                        {'src_id': probe_qid, 'dst_id': point_qid, 'props': [self.reference]})

        # Push all links to IYP
        self.iyp.batch_add_links('ASSIGNED', assigned_links)
        self.iyp.batch_add_links('LOCATED_IN', located_in_as_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('LOCATED_IN', located_in_point_links)

    def unit_test(self):
        return super().unit_test(['ASSIGNED', 'LOCATED_IN', 'COUNTRY'])


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
    print('Fetching RIPE Atlas probes')

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

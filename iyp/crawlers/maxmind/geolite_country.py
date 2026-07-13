import argparse
import hashlib
import json
import logging
import os
import sys
from io import BytesIO
from ipaddress import ip_network
from zipfile import ZipFile

import pandas as pd
import requests

from iyp import BaseCrawler, set_modification_time_from_last_modified_header

ORG = 'MaxMind'
URL = 'https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip'
NAME = 'maxmind.geolite_country'

SHA256_URL = 'https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip.sha256'

MAXMIND_ACCOUNT_ID = ''
MAXMIND_LICENSE_KEY = ''
if os.path.exists('config.json'):
    MAXMIND_ACCOUNT_ID = json.load(open('config.json', 'r'))['maxmind']['account_id']
    MAXMIND_LICENSE_KEY = json.load(open('config.json', 'r'))['maxmind']['license_key']


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/'

    def run(self):
        """Fetch data and push to IYP."""
        session = requests.Session()
        session.auth = (MAXMIND_ACCOUNT_ID, MAXMIND_LICENSE_KEY)

        logging.info('Fetching files...')

        hash_res = session.get(SHA256_URL)
        if hash_res.status_code != 200:
            logging.error('Failed to fetch SHA256 hash.')
            hash_res.raise_for_status()
        sha256_hash, expected_filename = hash_res.text.split()

        db_res = session.get(URL)
        if db_res.status_code != 200:
            logging.error('Failed to fetch database.')
            db_res.raise_for_status()

        session.close()

        sha256 = hashlib.file_digest(BytesIO(db_res.content), 'sha256').hexdigest()
        filename = db_res.headers['Content-Disposition'].removeprefix('attachment; filename=')
        if sha256 != sha256_hash or filename != expected_filename:
            logging.error('Validation of SHA256 hash or filename failed:')
            logging.error(f'    SHA256: {sha256}')
            logging.error(f'  Expected: {sha256_hash}')
            logging.error(f'  Filename: {filename}')
            logging.error(f'  Expected: {expected_filename}')
            raise ValueError('Validation of SHA256 hash or filename failed.')

        set_modification_time_from_last_modified_header(self.reference, db_res)
        logging.info(f'Downloaded file:  {filename} Last-Modified: {self.reference["reference_time_modification"]}')

        logging.info('Parsing data...')

        with ZipFile(BytesIO(db_res.content)) as zf:
            path_prefix = filename.removesuffix('.zip')
            try:
                v4_file = zf.getinfo(f'{path_prefix}/GeoLite2-Country-Blocks-IPv4.csv')
                v6_file = zf.getinfo(f'{path_prefix}/GeoLite2-Country-Blocks-IPv6.csv')
                geoname_file = zf.getinfo(f'{path_prefix}/GeoLite2-Country-Locations-en.csv')
            except KeyError as e:
                logging.error(f'Failed to extract data from ZIP file: {e}')
                raise ValueError(f'Failed to extract data from ZIP file: {e}')

            with zf.open(geoname_file) as f:
                geoname_df = pd.read_csv(f, keep_default_na=False, na_values=[''])
            geoname_df.pop('locale_code')
            # Data contains Asia and Europe as locations with only a continent
            # code, which we do not model.
            geoname_df = geoname_df[geoname_df['country_iso_code'].notna()]

            with zf.open(v4_file) as f:
                ipv4_df = pd.read_csv(f, usecols=(0, 1))
            with zf.open(v6_file) as f:
                ipv6_df = pd.read_csv(f, usecols=(0, 1))
            ip_df = pd.concat([ipv4_df, ipv6_df])
            # Some blocks only have an entry for the registered country, which
            # we already cover with the delegated stats file.
            ip_df = ip_df[ip_df['geoname_id'].notna()]

        ip_merged_df = ip_df.merge(geoname_df)
        ip_merged_df.pop('geoname_id')

        countries = set(ip_merged_df['country_iso_code'].unique())
        prefixes = set()
        links = list()

        for r in ip_merged_df.itertuples(index=False):
            prefix = ip_network(r.network).compressed
            prefixes.add(prefix)
            links.append(
                {
                    'src_id': prefix,
                    'dst_id': r.country_iso_code,
                    'props': [
                        self.reference,
                        {
                            'continent_code': r.continent_code,
                            'continent_name': r.continent_name,
                            'country_iso_code': r.country_iso_code,
                            'country_name': r.country_name,
                            'is_in_european_union': r.is_in_european_union,
                        }
                    ]
                }
            )

        logging.info('Pushing data...')

        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries, all=False)
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('GeoPrefix', 'prefix', prefixes, all=False)
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'Prefix')

        for link in links:
            link['src_id'] = prefix_id[link['src_id']]
            link['dst_id'] = country_id[link['dst_id']]

        self.iyp.batch_add_links('COUNTRY', links)

    def unit_test(self):
        return super().unit_test(['COUNTRY'])


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

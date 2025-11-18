import argparse
import logging
import os
import sys
import tempfile
from ipaddress import ip_network
import pandas as pd
from neo4j.spatial import WGS84Point


import requests

from iyp import BaseCrawler, get_commit_datetime

# based on crawler/bgptools/anycast_prefixes.py

# Organization name and URL to data
ORG = 'ut-dacs'
URL = 'https://github.com/ut-dacs/anycast-census'
NAME = 'ut-dacs.laces'

def get_dataset_url(as_prefixes_data_url: str, ip_version: int):
    anycast_prefixes_data_url_formatted: str = as_prefixes_data_url.replace('github.com', 'raw.githubusercontent.com')
    if ip_version == 4:
        anycast_prefixes_data_url_formatted += '/master/IPv4-latest.parquet'
    else:
        anycast_prefixes_data_url_formatted += '/master/IPv6-latest.parquet'
    return anycast_prefixes_data_url_formatted


def fetch_dataset(url: str):
    res = requests.get(url)
    res.raise_for_status()
    return res


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.repo = 'ut-dacs/anycast-census'
        self.v4_file = 'IPv4-latest.parquet'
        self.v6_file = 'IPv6-latest.parquet'
        self.reference['reference_url_info'] = 'manycast.net'

    def run(self):
        ipv4_prefixes_url = get_dataset_url(URL, 4)
        ipv6_prefixes_url = get_dataset_url(URL, 6)

        # Create a temporary directory
        tmpdir = tempfile.mkdtemp()

        # Filenames
        ipv4_prefixes_filename = os.path.join(tmpdir, 'anycast_ipv4_prefixes.parquet')
        ipv6_prefixes_filename = os.path.join(tmpdir, 'anycast_ipv6_prefixes.parquet')

        # Fetch data and push to IYP.
        # Overriding the reference_url_data according to prefixes
        self.reference['reference_url_data'] = ipv4_prefixes_url
        self.reference['reference_time_modification'] = get_commit_datetime(self.repo, self.v4_file)
        ipv4_prefixes_response = fetch_dataset(ipv4_prefixes_url)
        logging.info('IPv4 prefixes fetched successfully.')
        self.update(ipv4_prefixes_response, ipv4_prefixes_filename)
        print('finished v4')

        self.reference['reference_url_data'] = ipv6_prefixes_url
        self.reference['reference_time_modification'] = get_commit_datetime(self.repo, self.v6_file)
        ipv6_prefixes_response = fetch_dataset(ipv6_prefixes_url)
        logging.info('IPv6 prefixes fetched successfully.')
        self.update(ipv6_prefixes_response, ipv6_prefixes_filename)
        print('finished v6')

    def update(self, res, filename: str):
        """
        res: requests.Response object
        filename: str - path to save the fetched data
        """

        # write fetched data to file in binary mode
        with open(filename, 'wb') as file:
            file.write(res.content)

        # read .parquet using pandas
        laces_df = pd.read_parquet(filename, engine='pyarrow')

        # filter df on GCD_ICMP where LACeS has high confidence of anycast and locations
        if 'v4' in filename:
            laces_df = laces_df[laces_df['GCD_ICMPv4'] > 1]
        else:
            laces_df = laces_df[laces_df['GCD_ICMPv6'] > 1]

        anycast_prefixes = set() # unique prefixes set
        points = set() # unique points

        # iterate over rows creating anycast prefixes and points
        for index, row in laces_df.iterrows():
            try:
                prefix = ip_network(row['prefix']).compressed
                locations = row['locations']

                # create point for each location
                for location in locations:
                    lat = location['lat']
                    lon = location['lon']

                    points.add(WGS84Point((lon, lat)))

                # add prefix to anycast prefixes set
                anycast_prefixes.add(prefix)

            except ValueError as e:
                logging.warning(f'Ignoring malformed prefix: "{row["backing_prefix"]}": {e}')
                continue

        # get all IYP prefix IDs for our anycast prefixes and points
        anycast_pfx_ids = self.iyp.batch_get_nodes_by_single_prop('AnycastPrefix', 'prefix', anycast_prefixes, all=False)
        point_id = self.iyp.batch_get_nodes_by_single_prop('Point', 'position', points)

        # Add AnycastPrefix labels to IYP
        self.iyp.batch_add_node_label(list(anycast_pfx_ids.values()), 'AnycastPrefix')

        located_in_point_links = []

        for prefix in anycast_prefixes:
            prefix_qid = anycast_pfx_ids[prefix]
            locations = laces_df[laces_df['prefix'] == prefix]['locations'].iloc[0]

            # get point_id for each location and create LOCATED_IN links
            for location in locations:
                lat = location['lat']
                lon = location['lon']
                city = location['city']

                position = WGS84Point((lon, lat))
                point_qid = point_id[position]

                located_in_point_links.append({
                    'src_id': prefix_qid,
                    'dst_id': point_qid,
                    'props': [
                        self.reference,
                        {'city': city}
                    ],
                })


        # Push all links to IYP
        self.iyp.batch_add_links('LOCATED_IN', located_in_point_links)

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

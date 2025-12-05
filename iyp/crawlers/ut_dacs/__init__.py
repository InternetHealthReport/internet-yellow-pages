import logging
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network

import pandas as pd
from neo4j.spatial import WGS84Point

from iyp import BaseCrawler, DataNotAvailableError, get_commit_datetime

DATA_URL_FMT = 'https://raw.githubusercontent.com/ut-dacs/anycast-census/refs/heads/main/%Y/%m/%d/IPv{ip_version}.parquet'  # noqa: E501


class LacesCrawler(BaseCrawler):
    def __init__(self, organization: str, url: str, name: str, ip_version: int):
        super().__init__(organization, url, name)
        self.ip_version = ip_version
        self.repo = 'ut-dacs/anycast-census'
        self.latest_file = f'IPv{ip_version}-latest.parquet'
        self.reference['reference_url_info'] = 'https://manycast.net/'

    def run(self):
        # Overriding the reference_url_data according to prefixes
        modification_time = get_commit_datetime(self.repo, self.latest_file)
        if modification_time < datetime.now(tz=timezone.utc) - timedelta(days=7):
            raise DataNotAvailableError(f'No recent IPv{self.ip_version} data available.')
        prefixes_url = modification_time.strftime(DATA_URL_FMT).format(ip_version=self.ip_version)
        self.reference['reference_time_modification'] = modification_time
        self.reference['reference_url_data'] = prefixes_url

        logging.info(f'Fetching {prefixes_url}')
        # PyArrow is required, otherwise the locations field breaks.
        laces_df = pd.read_parquet(prefixes_url, engine='pyarrow')

        # Filter on GCD_ICMP where LACeS has high confidence of anycast and locations.
        laces_df = laces_df[laces_df[f'GCD_ICMPv{self.ip_version}'] > 1]

        bgp_prefixes = set()
        geo_prefixes = set()
        points = set()
        countries = set()
        categorized_links = list()
        located_in_links = list()
        country_links = list()

        anycast_tag_qid = self.iyp.get_node('Tag', {'label': 'Anycast'})

        # Iterate over rows creating anycast prefixes and points.
        for idx, row in laces_df.iterrows():
            ref_data = row.to_dict()
            try:
                prefix = ip_network(ref_data.pop('prefix')).compressed
            except ValueError as e:
                logging.warning(f'Ignoring malformed prefix: "{row.prefix}": {e}')
                continue
            try:
                bgp_prefix = ip_network(ref_data.pop('backing_prefix')).compressed
            except ValueError as e:
                logging.warning(f'Ignoring malformed backing prefix: "{row.backing_prefix}": {e}')
                continue
            if bgp_prefix not in bgp_prefixes:
                bgp_prefixes.add(bgp_prefix)
                categorized_links.append({
                    'src_id': bgp_prefix,
                    'dst_id': anycast_tag_qid,
                    'props': [
                        self.reference
                    ]
                })

            locations = ref_data.pop('locations')
            # Create a point and LOCATED_IN link for each location.
            if locations.any():
                geo_prefixes.add(prefix)
            for location in locations:
                lat = location.pop('lat')
                lon = location.pop('lon')
                point = WGS84Point((lon, lat))
                points.add(point)

                # Include location metadata in link properties, exclude None values.
                ref_data.update({k: v for k, v in location.items() if v})

                located_in_links.append({
                    'src_id': prefix,
                    'dst_id': point,
                    'props': [
                        self.reference,
                        dict(ref_data),
                    ],
                })

                # Some entries only have WGS84 coordinates, but no country mapping.
                if location['country_code']:
                    country = location['country_code']
                    countries.add(country)
                    country_links.append({
                        'src_id': prefix,
                        'dst_id': country,
                        'props': [
                            self.reference,
                            dict(ref_data),
                        ],
                    })

        # Get all IYP prefix IDs for our anycast prefixes and points.
        bgp_prefix_id = self.iyp.batch_get_nodes_by_single_prop(
            'BGPPrefix',
            'prefix',
            bgp_prefixes,
            all=False
        )
        geo_prefix_id = self.iyp.batch_get_nodes_by_single_prop(
            'GeoPrefix',
            'prefix',
            geo_prefixes,
            all=False
        )
        point_id = self.iyp.batch_get_nodes_by_single_prop('Point', 'position', points, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries, all=False)

        # Add Prefix labels to AnycastPrefix and GeoPrefix nodes.
        self.iyp.batch_add_node_label(list(bgp_prefix_id.values()), 'Prefix')
        self.iyp.batch_add_node_label(list(geo_prefix_id.values()), 'Prefix')

        # Replace links values with node ids.
        for link in categorized_links:
            link['src_id'] = bgp_prefix_id[link['src_id']]
        for link in country_links:
            link['src_id'] = geo_prefix_id[link['src_id']]
            link['dst_id'] = country_id[link['dst_id']]
        for link in located_in_links:
            link['src_id'] = geo_prefix_id[link['src_id']]
            link['dst_id'] = point_id[link['dst_id']]

        self.iyp.batch_add_links('CATEGORIZED', categorized_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('LOCATED_IN', located_in_links)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED', 'COUNTRY', 'LOCATED_IN'])

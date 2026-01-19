import argparse
import logging
import sys
from datetime import datetime, timezone
from ipaddress import IPv4Network, IPv6Network

import iso3166
import requests
from bs4 import BeautifulSoup

from iyp import BaseCrawler

URL = 'https://ip-ranges.amazonaws.com/ip-ranges.json'
ORG = 'Amazon'
NAME = 'amazon.aws_ip_ranges'

# AWS documentation page with region-to-country mapping
AWS_REGIONS_DOC_URL = 'https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html'

# Manual mapping for country names that do not match iso3166 exactly.
# These are edge cases where AWS uses different naming conventions.
COUNTRY_NAME_OVERRIDES = {
    'South Korea': 'KR',
    'Taiwan': 'TW',
    'United Kingdom': 'GB',
}

# Regions not documented on the main documentation page.
ADDITIONAL_REGIONS = {
    'cn-north-1': 'CN',  # https://docs.amazonaws.cn/en_us/aws/latest/userguide/endpoints-Beijing.html
    'cn-northwest-1': 'CN',  # https://docs.amazonaws.cn/en_us/aws/latest/userguide/endpoints-Ningxia.html
    'eusc-de-east-1': 'DE',  # https://aws.amazon.com/blogs/aws/opening-the-aws-european-sovereign-cloud/
    # https://docs.aws.amazon.com/govcloud-us/latest/UserGuide/using-govcloud.html
    'us-gov-east-1': 'US',
    'us-gov-west-1': 'US',
}


def fetch_region_to_country_mapping():
    """Fetch AWS region to country code mapping by scraping AWS documentation.

    Returns a dict mapping region codes (e.g., 'us-east-1') to ISO country codes (e.g.,
    'US').
    """
    logging.info(f'Fetching AWS region mapping from {AWS_REGIONS_DOC_URL}')

    resp = requests.get(AWS_REGIONS_DOC_URL)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Find the regions table (there's only one table on this page)
    tables = soup.find_all('table')
    if not tables:
        raise ValueError('No table found on AWS regions documentation page')

    table = tables[0]

    # Skip header row, process data rows
    rows = table.find_all('tr')[1:]

    region_to_country = dict()

    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 4:
            continue

        region_code = cols[0].get_text(strip=True)
        country_name = cols[3].get_text(strip=True)  # Geography column

        # Check manual overrides first
        if country_name in COUNTRY_NAME_OVERRIDES:
            country_code = COUNTRY_NAME_OVERRIDES[country_name]
        else:
            # Try iso3166 lookup (case-insensitive)
            country = iso3166.countries_by_name.get(country_name.upper())
            if country:
                country_code = country.alpha2
            else:
                logging.warning(f'Unknown country name for region {region_code}: {country_name}')
                continue

        region_to_country[region_code] = country_code

    logging.info(f'Fetched {len(region_to_country)} region-to-country mappings')

    # Only add regions if they have not been added to the main documentation page.
    for region_code, country_code in ADDITIONAL_REGIONS.items():
        if region_code not in region_to_country:
            region_to_country[region_code] = country_code
            logging.info(f'Manually added region: {region_code} -> {country_code}')

    return region_to_country


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://docs.aws.amazon.com/vpc/latest/userguide/aws-ip-ranges.html'

    def run(self):
        """Fetch AWS IP ranges and push to IYP."""

        # Fetch region-to-country mapping from AWS documentation
        region_to_country = fetch_region_to_country_mapping()

        # Fetch AWS IP ranges JSON
        logging.info(f'Fetching AWS IP ranges from {URL}')
        resp = requests.get(URL)
        resp.raise_for_status()
        data = resp.json()

        # Parse createDate for reference_time_modification
        # Format: 'YYYY-MM-DD-HH-mm-ss'
        if 'createDate' in data:
            try:
                dt = datetime.strptime(data['createDate'], '%Y-%m-%d-%H-%M-%S')
                self.reference['reference_time_modification'] = dt.replace(tzinfo=timezone.utc)
            except ValueError:
                logging.warning(f"Could not parse createDate: {data['createDate']}")

        # Parse prefixes
        items = list()
        unknown_regions = set()
        for item in data['prefixes']:
            region = item['region']
            if region not in region_to_country:
                unknown_regions.add(region)
            country_code = region_to_country.get(region)
            items.append({
                'prefix': IPv4Network(item['ip_prefix']).compressed,
                'region': item['region'],
                'service': item['service'],
                'country_code': country_code,
            })
        for item in data['ipv6_prefixes']:
            region = item['region']
            if region not in region_to_country:
                unknown_regions.add(region)
            country_code = region_to_country.get(region)
            items.append({
                'prefix': IPv6Network(item['ipv6_prefix']).compressed,
                'region': item['region'],
                'service': item['service'],
                'country_code': country_code,
            })

        if unknown_regions:
            logging.warning(f'Regions without country mapping: {unknown_regions}')

        logging.info(f'Processing {len(items)} prefixes')

        # Collect unique values
        prefixes = set()
        services = set()
        countries = set()

        for item in items:
            prefixes.add(item['prefix'])
            services.add(item['service'])
            country_code = item['country_code']
            if country_code:
                countries.add(country_code)

        # Create/get nodes
        prefix_id = self.iyp.batch_get_nodes_by_single_prop(
            'GeoPrefix', 'prefix', prefixes, all=False
        )
        # Add Prefix label to all GeoPrefix nodes
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'Prefix')

        tag_id = self.iyp.batch_get_nodes_by_single_prop(
            'Tag', 'label', services, all=False
        )

        country_id = self.iyp.batch_get_nodes_by_single_prop(
            'Country', 'country_code', countries, all=False
        )

        # Prepare relationships
        categorized_links = list()
        country_links = list()
        # If a prefix hosts multiple services, we would add multiple country
        # relationships.
        prefix_country_pairs = set()

        for item in items:
            prefix = item['prefix']
            prefix_qid = prefix_id[prefix]
            # CATEGORIZED -> Tag (service)
            categorized_links.append({
                'src_id': prefix_qid,
                'dst_id': tag_id[item['service']],
                'props': [self.reference, {'region': item['region']}]
            })

            # COUNTRY -> Country
            country_code = item['country_code']
            prefix_country_pair = (prefix, country_code)
            if country_code and prefix_country_pair not in prefix_country_pairs:
                country_links.append({
                    'src_id': prefix_qid,
                    'dst_id': country_id[country_code],
                    'props': [self.reference, {'region': item['region']}]
                })
                prefix_country_pairs.add(prefix_country_pair)

        # Create relationships
        self.iyp.batch_add_links('CATEGORIZED', categorized_links)
        self.iyp.batch_add_links('COUNTRY', country_links)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED', 'COUNTRY'])


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

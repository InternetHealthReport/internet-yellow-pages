import argparse
import logging
import sys
from datetime import datetime
import requests
from iyp import BaseCrawler

ORG = 'Amazon Web Services'
URL = 'https://ip-ranges.amazonaws.com/ip-ranges.json'
NAME = 'amazon.aws_ip_ranges'

REGION_TO_COUNTRY = {
    'us-east-1': 'US', 'us-east-2': 'US', 'us-west-1': 'US', 'us-west-2': 'US',
    'af-south-1': 'ZA',
    'ap-east-1': 'HK', 'ap-south-1': 'IN', 'ap-south-2': 'IN',
    'ap-northeast-1': 'JP', 'ap-northeast-2': 'KR', 'ap-northeast-3': 'JP',
    'ap-southeast-1': 'SG', 'ap-southeast-2': 'AU',
    'ap-southeast-3': 'ID', 'ap-southeast-4': 'AU',
    'ca-central-1': 'CA',
    'eu-central-1': 'DE', 'eu-central-2': 'CH',
    'eu-west-1': 'IE', 'eu-west-2': 'GB', 'eu-west-3': 'FR',
    'eu-south-1': 'IT', 'eu-south-2': 'ES',
    'eu-north-1': 'SE',
    'me-south-1': 'BH', 'me-central-1': 'AE',
    'sa-east-1': 'BR',
    'us-gov-east-1': 'US', 'us-gov-west-1': 'US'
}


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = (
            'https://docs.aws.amazon.com/vpc/latest/userguide/aws-ip-ranges.html'
        )

    def run(self):
        logging.info(f"Fetching AWS IP ranges from {URL}")
        try:
            resp = requests.get(URL)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.error(f"Failed to fetch AWS IP ranges: {e}")
            return

        # Parse createDate for reference_time_modification
        # Format: "YYYY-MM-DD-HH-mm-ss" -> "2026-01-10-08-28-04"
        if 'createDate' in data:
            try:
                dt = datetime.strptime(data['createDate'], "%Y-%m-%d-%H-%M-%S")
                # Ensure UTC if needed, but assuming simple datetime object
                self.reference['reference_time_modification'] = dt
            except ValueError:
                logging.warning(f"Could not parse createDate: {data['createDate']}")

        # Create Organization
        org_id = self.iyp.get_node('Organization', {'name': ORG}, ['name'])

        prefixes_v4 = []
        prefixes_v6 = []

        # AWS JSON has 'prefixes' (IPv4) and 'ipv6_prefixes' (IPv6)
        items = []
        for item in data.get('prefixes', []):
            items.append((item['ip_prefix'], item['region'], item['service'], 4))
        for item in data.get('ipv6_prefixes', []):
            items.append((item['ipv6_prefix'], item['region'], item['service'], 6))

        for prefix, region, service, version in items:
            props = {
                'prefix': prefix,
                'af': version
            }
            if version == 4:
                prefixes_v4.append(props)
            else:
                prefixes_v6.append(props)

        # 1. Batch Create Nodes
        logging.info(
            f"Creating {len(prefixes_v4)} IPv4 and {len(prefixes_v6)} IPv6 nodes..."
        )
        prefix_v4_ids = self.iyp.batch_get_nodes(
            'Prefix', prefixes_v4, ['prefix'], create=True
        )
        prefix_v6_ids = self.iyp.batch_get_nodes(
            'Prefix', prefixes_v6, ['prefix'], create=True
        )

        all_prefix_ids = {**prefix_v4_ids, **prefix_v6_ids}

        # 2. Prepare Links
        managed_by_links = []
        located_in_links = []
        categorized_links = []

        tag_cache = {}
        country_cache = {}

        for prefix, region, service, version in items:
            if prefix not in all_prefix_ids:
                continue

            p_node_id = all_prefix_ids[prefix]

            # MANAGED_BY Organization
            managed_by_links.append({
                'src_id': p_node_id,
                'dst_id': org_id,
                'props': [self.reference]
            })

            # LOCATED_IN Country
            country_code = REGION_TO_COUNTRY.get(region)
            if country_code:
                if country_code not in country_cache:
                    c_id = self.iyp.get_node(
                        'Country', {'country_code': country_code}, ['country_code']
                    )
                    country_cache[country_code] = c_id

                if country_cache[country_code]:
                    located_in_links.append({
                        'src_id': p_node_id,
                        'dst_id': country_cache[country_code],
                        'props': [self.reference]
                    })

            # CATEGORIZED (Service Tag)
            if service:
                if service not in tag_cache:
                    t_id = self.iyp.get_node('Tag', {'label': service}, ['label'])
                    tag_cache[service] = t_id

                categorized_links.append({
                    'src_id': p_node_id,
                    'dst_id': tag_cache[service],
                    'props': [self.reference]
                })

        # 3. Batch Create Links
        if managed_by_links:
            self.iyp.batch_add_links('MANAGED_BY', managed_by_links)
        if located_in_links:
            self.iyp.batch_add_links('LOCATED_IN', located_in_links)
        if categorized_links:
            self.iyp.batch_add_links('CATEGORIZED', categorized_links)

        logging.info(f"Refreshed AWS IP Ranges: {len(items)} prefixes processed.")

    def unit_test(self):
        return super().unit_test(['MANAGED_BY', 'CATEGORIZED', 'LOCATED_IN'])


def main():
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

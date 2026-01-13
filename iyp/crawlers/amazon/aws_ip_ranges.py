import argparse
import logging
import sys
from datetime import datetime
import requests
from iyp import BaseCrawler

URL = 'https://ip-ranges.amazonaws.com/ip-ranges.json'
NAME = 'amazon.aws_ip_ranges'


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
        categorized_links = []
        tag_cache = {}

        for prefix, region, service, version in items:
            if prefix not in all_prefix_ids:
                continue

            p_node_id = all_prefix_ids[prefix]

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
        if categorized_links:
            self.iyp.batch_add_links('CATEGORIZED', categorized_links)

        logging.info(f"Refreshed AWS IP Ranges: {len(items)} prefixes processed.")

    def unit_test(self):
        return super().unit_test(['CATEGORIZED'])


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

    crawler = Crawler(URL, URL, NAME)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

import requests

from iyp import BaseCrawler, RequestStatusError

# Organization name and URL to data
ORG = 'Example Org'
URL = 'https://example.com/data.csv'
NAME = 'example.crawler'


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        # If available, add an info URL providing a human-readable description of the
        # data.
        self.reference['reference_url_info'] = 'https://example.com/about.html'
        # If available, provide a timestamp indicating when the dataset was updated the
        # last time. Should be a timezone-aware datetime object.
        # This should be ABSENT if unknown.
        self.reference['reference_time_modification'] = datetime.now(tz=timezone.utc)

    def run(self):
        """Fetch data and push to IYP."""

        # Fetch data
        req = requests.get(self.reference['reference_url_data'])
        if req.status_code != 200:
            # Use logging module for log messages. Be sparse on info logging.
            logging.error(f'Cannot download data {req.status_code}: {req.text}')
            # Crawlers should raise exceptions and never call sys.exit().
            raise RequestStatusError('Error while fetching data file.')

        # Keep track of *unique* nodes. We do not want to create multiple nodes with the
        # same properties.
        nodes = set()
        # Keep track of links. Depending on the dataset it might be required to manually
        # track uniqueness as well.
        links = list()

        # Process line one after the other
        for line in enumerate(req.text.splitlines()):
            node1, node2, value = line.split(',')
            # Record nodes
            nodes.add(node1)
            nodes.add(node2)
            # Add relationship. We need to replace the source and destination IDs with
            # the appropriate QID once we fetched/created the nodes.
            links.append({
                'src_id': node1,
                'dst_id': node2,
                # List of properties to add to the relationship. Should be a list of
                # dicts that will be merged.
                'props': [
                    self.reference,  # Always include the reference data.
                    {'property_value': value}  # Optionally add your own properties.
                ]
            })

        # Get/create nodes. In most cases nodes are identified by a single property
        # (called "id" in this example) and have no additional properties.
        # This function gets/creates nodes for all values in the "nodes" set.
        # For more complex scenarios check the batch_get_nodes function.
        node_id = self.iyp.batch_get_nodes_by_single_prop('EXAMPLE_NODE_LABEL', 'id', nodes, all=False)
        # Replace node IDs with actual QIDs
        for link in links:
            link['src_id'] = node_id[link['src_id']]
            link['dst_id'] = node_id[link['dst_id']]

        self.iyp.batch_add_links('EXAMPLE_RELATIONSHIP_LABEL', links)

    def unit_test(self):
        # Unit test checks for existence of relationships created by this crawler.
        return super().unit_test(['EXAMPLE_RELATIONSHIP_LABEL'])


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

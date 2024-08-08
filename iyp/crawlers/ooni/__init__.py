import ipaddress
import json
import logging
import os

from iyp import BaseCrawler

from .utils import grabber


# OONI Crawler base class
class OoniCrawler(BaseCrawler):

    def __init__(self, organization, url, name, dataset):
        """OoniCrawler initialization requires the dataset name."""
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://ooni.org/post/mining-ooni-data'
        self.repo = 'ooni-data-eu-fra'
        self.dataset = dataset
        self.all_asns = set()
        self.all_countries = set()
        self.all_results = list()
        self.all_percentages = list()
        self.all_dns_resolvers = set()
        self.unique_links = {
            'COUNTRY': set(),
            'CENSORED': set(),
            'RESOLVES_TO': set(),
            'PART_OF': set(),
            'CATEGORIZED': set(),
        }

    def run(self):
        """Fetch data and push to IYP."""

        # Create a temporary directory
        tmpdir = self.create_tmp_dir()

        # Fetch data
        grabber.download_and_extract(self.repo, tmpdir, self.dataset)
        logging.info('Successfully downloaded and extracted all files')
        # Now that we have downloaded the jsonl files for the test we want, we can
        # extract the data we want
        testdir = os.path.join(
            tmpdir,
            self.dataset,
        )
        for file_name in os.listdir(testdir):
            file_path = os.path.join(
                testdir,
                file_name,
            )
            if os.path.isfile(file_path) and file_path.endswith('.jsonl'):
                with open(file_path, 'r') as file:
                    for i, line in enumerate(file):
                        data = json.loads(line)
                        self.process_one_line(data)
                        logging.info(f"\rProcessed {i + 1} lines")
        logging.info('\n Processed lines, now calculating percentages\n')
        self.calculate_percentages()
        logging.info('\n Calculated percentages, now adding entries to IYP\n')
        self.batch_add_to_iyp()
        logging.info('\n Successfully added all entries to IYP\n')

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally."""

        probe_asn = (
            int(one_line.get('probe_asn')[2:])
            if one_line.get('probe_asn') and one_line.get('probe_asn').startswith('AS')
            else None
        )
        # Add the DNS resolver to the set, unless its not a valid IP address
        try:
            self.all_dns_resolvers.add(
                ipaddress.ip_address(one_line.get('resolver_ip'))
            )
        except ValueError:
            pass
        probe_cc = one_line.get('probe_cc')

        # Append the results to the list
        self.all_asns.add(probe_asn)
        self.all_countries.add(probe_cc)
        self.all_results.append((probe_asn, probe_cc, None, None))

    def batch_add_to_iyp(self):
        """Add the results to the IYP."""
        country_links = []

        # First, add the nodes and store their IDs directly as returned dictionaries
        self.node_ids = {
            'asn': self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', self.all_asns),
            'country': self.iyp.batch_get_nodes_by_single_prop(
                'Country', 'country_code', self.all_countries
            ),
            'dns_resolver': self.iyp.batch_get_nodes_by_single_prop(
                'IP', 'ip', self.all_dns_resolvers, all=False
            ),
        }
        # to avoid duplication of country links, we only add them from
        # the webconnectivity dataset
        if self.dataset in ['webconnectivity']:
            for entry in self.all_results:
                asn, country = entry[:2]
                asn_id = self.node_ids['asn'].get(asn)
                country_id = self.node_ids['country'].get(country)

                # Check if the COUNTRY link is unique
                if (asn_id, country_id) not in self.unique_links['COUNTRY']:
                    self.unique_links['COUNTRY'].add((asn_id, country_id))
                    country_links.append(
                        {
                            'src_id': asn_id,
                            'dst_id': country_id,
                            'props': [self.reference],
                        }
                    )
            self.iyp.batch_add_links('COUNTRY', country_links)

        # Batch add node labels
        self.iyp.batch_add_node_label(
            list(self.node_ids['dns_resolver'].values()), 'Resolver'
        )

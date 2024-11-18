import ipaddress
import json
import logging
import os

from iyp import BaseCrawler
from iyp.crawlers.ooni.utils import grabber


# OONI Crawler base class
class OoniCrawler(BaseCrawler):

    def __init__(self, organization, url, name, dataset):
        """OoniCrawler initialization requires the dataset name."""
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://ooni.org/post/mining-ooni-data'
        self.repo = 'ooni-data-eu-fra'
        self.dataset = dataset
        self.categories = list()
        self.all_asns = set()
        self.all_countries = set()
        self.all_results = list()
        self.all_percentages = dict()
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
        logging.info('Successfully downloaded and extracted all files.')
        # Now that we have downloaded the jsonl files for the test we want, we can
        # extract the data we want
        logging.info('Processing files...')
        for entry in os.scandir(tmpdir):
            if not entry.is_file() or not entry.name.endswith('.jsonl'):
                continue
            file_path = os.path.join(tmpdir, entry.name)
            with open(file_path, 'r') as file:
                for line in file:
                    data = json.loads(line)
                    self.process_one_line(data)
        logging.info('Calculating percentages...')
        self.aggregate_results()
        logging.info('Adding entries to IYP...')
        self.batch_add_to_iyp()
        logging.info('Done.')

    def process_one_line(self, one_line):
        """Process a single line from the jsonl file and store the results locally.

        Return True if an error occurred and no result was added, i.e., the extended
        class should not continue to process this line.
        """

        # No test result. Can happen sometimes.
        if not one_line.get('test_keys'):
            return True

        # Extract the ASN, throw an exception if malformed
        try:
            probe_asn = int(one_line['probe_asn'].removeprefix('AS'))
        except ValueError as e:
            logging.error(f'Invalid probe ASN: {one_line["probe_asn"]}')
            raise e

        # Add the DNS resolver to the set, unless its not a valid IP address
        try:
            resolver_ip = ipaddress.ip_address(one_line.get('resolver_ip'))
            if resolver_ip.is_global:
                self.all_dns_resolvers.add(resolver_ip.compressed)
        except ValueError:
            pass

        probe_cc = one_line.get('probe_cc')

        if probe_asn == 0:
            # Ignore result if probe ASN is hidden.
            return True

        self.all_asns.add(probe_asn)
        if probe_cc != 'ZZ':
            # Do not create country nodes for ZZ country.
            self.all_countries.add(probe_cc)

        # Append the results to the list.
        self.all_results.append((probe_asn, probe_cc))
        """The base function adds a skeleton to the all_results list, which includes the
        probe_asn and the probe_cc.

        Each extended crawler then modifies this entry by calling self.all_results[-1]
        to access the last result and add its specific variables.
        Attention: if you are discarding a result in the extended class, you need to
        make sure you specifically pop() the entry created here, in the base class, or
        you WILL end up with misformed entries that only contain the probe_asn and
        probe_cc, and mess up your data.
        """
        return False

    def batch_add_to_iyp(self):
        """Add the results to the IYP."""
        country_links = list()

        # First, add the nodes and store their IDs directly as returned dictionaries
        self.node_ids = {
            'asn': self.iyp.batch_get_nodes_by_single_prop(
                'AS', 'asn', self.all_asns, all=False
            ),
            'country': self.iyp.batch_get_nodes_by_single_prop(
                'Country', 'country_code', self.all_countries
            ),
            'dns_resolver': self.iyp.batch_get_nodes_by_single_prop(
                'IP', 'ip', self.all_dns_resolvers, all=False
            ),
        }
        # To avoid duplication of country links, we only add them from
        # the webconnectivity dataset
        if self.dataset == 'webconnectivity':
            for entry in self.all_results:
                asn, country = entry[:2]
                if country == 'ZZ':
                    continue
                asn_id = self.node_ids['asn'][asn]
                country_id = self.node_ids['country'][country]

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

    def aggregate_results(self):
        """Populate the self.all_percentages dict by aggregating results and calculating
        percentages."""
        raise NotImplementedError()

    def make_result_dict(self, counts: dict, total_count: int = None):
        """Create a result dict containing the counts, total count, and percentages.

        Ensure that entries for all categories defined in self.categories exist. If not
        specified, total_count is the sum of all counts.
        """
        if total_count is None:
            total_count = sum(counts.values())

        for category in self.categories:
            # Ensure entry for each category exists.
            counts[category] = counts.get(category, 0)

        percentages = {
            category: (
                (counts[category] / total_count) * 100 if total_count > 0 else 0
            )
            for category in self.categories
        }

        return {
            'total_count': total_count,
            'category_counts': dict(counts),
            'percentages': percentages,
        }


def process_dns_queries(queries_list: list):
    host_ip_set = set()
    if not queries_list:
        return host_ip_set
    for query in queries_list:
        if query['query_type'] not in {'A', 'AAAA'} or query['failure']:
            continue
        hostname = query['hostname']
        for answer in query['answers']:
            try:
                if answer['answer_type'] == 'A':
                    ip = ipaddress.ip_address(answer['ipv4'])
                elif answer['answer_type'] == 'AAAA':
                    ip = ipaddress.ip_address(answer['ipv6'])
                else:
                    # CNAME etc.
                    continue
            except ValueError:
                # In rare cases the answer IP is scrubbed and thus invalid.
                continue
            if not ip.is_global:
                continue
            host_ip_set.add((hostname, ip.compressed))
    return host_ip_set

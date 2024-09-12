import argparse
import logging
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import requests
from github import Github

from iyp import BaseCrawler, ConnectionError, RequestStatusError


def get_latest_dataset_url(github_repo: str, data_dir: str, file_extension: str):
    """Return url to the first file with the given extension found in the latest
    (alphabetically ordered) folder found in data_dir of the given github_repo.

    Returns an empty string if no such file is found.
    """

    gh = Github()
    repo = gh.get_repo(github_repo)
    all_data_dir = sorted([d.path for d in repo.get_contents(data_dir)])
    latest_files = repo.get_contents(all_data_dir[-1])
    for file in latest_files:
        if file.path.endswith(file_extension):
            logging.info(file.download_url)
            return file.download_url

    return ''


# Organization name and URL to data
ORG = 'Internet Intelligence Lab'
URL = get_latest_dataset_url('InetIntel/Dataset-AS-to-Organization-Mapping', '/data', '.json')
NAME = 'inetintel.as_org'  # should reflect the directory and name of this file


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and set up a dictionary with the org/url/today's date in self.reference
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://github.com/InetIntel/Dataset-AS-to-Organization-Mapping'
        self.__get_modification_time_from_url()

    def __get_modification_time_from_url(self):
        expected_suffix = '.json'
        try:
            if not URL.endswith(expected_suffix):
                raise ValueError(f'Expected "{expected_suffix}" file for data URL')
            _, date_str = URL[:-len(expected_suffix)].rsplit('.', maxsplit=1)
            date = datetime.strptime(date_str, '%Y-%m').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except ValueError as e:
            logging.warning(f'Failed to set modification time: {e}')

    def run(self):
        """Fetch data and push to IYP."""

        # Create a temporary directory
        self.tmpdir = tempfile.mkdtemp()

        # Filename to save the JSON file as
        self.filename = os.path.join(self.tmpdir, 'inetintel_as_org.json')

        # Fetch data
        try:
            req = requests.get(URL)
        except requests.exceptions.ConnectionError as e:
            logging.error(e)
            raise ConnectionError('Connection error while fetching data file')
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            raise RequestStatusError('Error while fetching data file')

        with open(self.filename, 'w') as file:
            file.write(req.text)

        logging.info('Dataset crawled and saved in a temporary file.')

        # The dataset is very large. Pandas has the ability to read JSON, and, in
        # theory, it could do it in a more memory-efficient way.
        df = pd.read_json(self.filename, orient='index')
        logging.info('Dataset has {} rows.'.format(len(df)))

        # Optimized code
        batch_size = 10000
        if len(df) < batch_size:
            batch_size = len(df)

        count_rows_global = 0
        count_relationships_global = 0
        connections = defaultdict(set)  # Remember the relationship between the "AS" and its Sibling.
        connections_org = defaultdict(set)  # Remember the relationship between the organizations
        org_id = self.iyp.batch_get_node_extid('PeeringdbOrgID')

        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i:i + batch_size]
            batch_lines = []
            batch_asns = set()
            pdb_orgs = set()
            batch_urls = set()
            count_rows = 0
            count_relationships = 0

            for index, row in df_batch.iterrows():
                asn = int(index)
                batch_asns.add(asn)
                sibling_asns = set()

                for sibling_asn in row['Sibling ASNs']:
                    sibling_asns.add(int(sibling_asn))
                    batch_asns.add(int(sibling_asn))

                pdb_orgs = set([int(org['org_id']) for org in row['Reference Orgs'] if org['source'] == 'PDB'])

                url = row['Website']
                if len(url) > 1:
                    batch_urls.add(url)

                batch_lines.append([asn, url, sibling_asns, pdb_orgs])
                count_rows += 1

            asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', batch_asns, all=False)
            url_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', batch_urls, all=False)

            asn_to_url_links = []
            asn_to_sibling_asn_links = []
            org_to_sibling_org_links = []

            for (asn, url, siblings, pdb_orgs) in batch_lines:
                asn_qid = asn_id[asn]

                if len(url) > 1:
                    url_qid = url_id[url]
                    asn_to_url_links.append({'src_id': asn_qid, 'dst_id': url_qid, 'props': [self.reference]})

                for org0 in pdb_orgs:
                    for org1 in pdb_orgs:
                        if org0 in org_id and org1 in org_id:
                            if org0 != org1 and org1 not in connections_org[org0]:
                                org0_qid = org_id[org0]
                                org1_qid = org_id[org1]
                                org_to_sibling_org_links.append(
                                    {'src_id': org0_qid, 'dst_id': org1_qid, 'props': [self.reference]})

                                connections_org[org0].add(org1)
                                connections_org[org1].add(org0)

                for sibling in siblings:
                    sibling_qid = asn_id[sibling]
                    if asn_qid != sibling_qid:
                        # A check whether asn and sibling are connected already.
                        if asn in connections and sibling in connections[asn]:
                            continue
                        elif sibling in connections and asn in connections[sibling]:
                            continue
                        else:
                            connections[asn].add(sibling)
                            asn_to_sibling_asn_links.append(
                                {'src_id': asn_qid, 'dst_id': sibling_qid, 'props': [self.reference]})
                            count_relationships += 1

            # Push all links to IYP
            if len(asn_to_url_links) > 0:
                self.iyp.batch_add_links('WEBSITE', asn_to_url_links)

            if len(asn_to_sibling_asn_links) > 0:
                self.iyp.batch_add_links('SIBLING_OF', asn_to_sibling_asn_links)

            if len(org_to_sibling_org_links) > 0:
                self.iyp.batch_add_links('SIBLING_OF', org_to_sibling_org_links)

            count_rows_global += count_rows
            count_relationships_global += count_relationships
            logging.info('processed: {} rows and {} relationships'
                         .format(count_rows_global, count_relationships_global))

    def close(self):
        super().close()

        os.remove(self.filename)
        os.rmdir(self.tmpdir)

    def unit_test(self):
        return super().unit_test(['SIBLING_OF'])


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

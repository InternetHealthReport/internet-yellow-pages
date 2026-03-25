import argparse
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import combinations

import requests
from github import Github

from iyp import BaseCrawler, DataNotAvailableError

ORG = 'Internet Intelligence Lab'
URL = 'https://github.com/InetIntel/Dataset-AS-to-Organization-Mapping'
NAME = 'inetintel.as_org'

FORMAT_SPECIFIER = 'v1.2.ff003'


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
        if file.path.endswith(file_extension) and FORMAT_SPECIFIER in file.path:
            return file.download_url
    return str()


class Crawler(BaseCrawler):

    def link_generator(self, elems, src_map: dict, dst_map: dict):
        for src, dst in elems:
            yield {
                'src_id': src_map[src],
                'dst_id': dst_map[dst],
                'props': [self.reference]
            }

    def run(self):
        """Fetch data and push to IYP."""
        dataset_url = get_latest_dataset_url('InetIntel/Dataset-AS-to-Organization-Mapping', '/data', '.json')
        if not dataset_url:
            logging.error('Failed to find valid file in repository.')
            raise DataNotAvailableError('Failed to find valid file in repository.')
        self.reference['reference_url_data'] = dataset_url

        logging.info(f'Fetching {dataset_url}')
        r = requests.get(dataset_url)
        r.raise_for_status()
        data = r.json()

        # Reference data is included as metadata.
        self.reference['reference_url_info'] = data['metadata']['documentation_url']
        data_creation_time = datetime.strptime(data['metadata']['snapshot_month'], '%Y-%m').replace(tzinfo=timezone.utc)
        # Dataset is produced monthly.
        if data_creation_time < datetime.now(tz=timezone.utc) - timedelta(days=60):
            logging.error(f'Failed to find recent dataset. Latest available: {data_creation_time}')
            raise DataNotAvailableError(f'Failed to find recent dataset. Latest available: {data_creation_time}')

        # We only create SIBLING_OF relationships between existing PeeringDB
        # organizations. Since this dataset is only produced monthly, it might contain
        # organizations that were already deleted so use the peeringdb.org crawler as
        # reference and create no organizations here.
        iyp_org_id = self.iyp.batch_get_nodes_by_single_prop('Organization', 'name', all=True, create=False)

        asns = set()
        urls = set()
        org_siblings = defaultdict(set)
        as_siblings = defaultdict(set)
        website_links = list()
        org_sibling_of_links = set()
        asn_sibling_of_links = set()

        for asn, as_data in data['as2org'].items():
            asn = int(asn)
            # This is just a fictional identifier for the dataset.
            org_id = as_data['OrgID']

            as_siblings[org_id].add(asn)

            pdb_org = as_data['PDB.Org']
            if pdb_org in iyp_org_id:
                org_siblings[org_id].add(pdb_org)
            website = as_data['Website']
            if website:
                asns.add(asn)
                urls.add(website)
                website_links.append((asn, website))

        for sibling_set in as_siblings.values():
            if len(sibling_set) <= 1:
                continue
            for asn0, asn1 in combinations(sibling_set, 2):
                asns.add(asn0)
                asns.add(asn1)
                asn_sibling_of_links.add((asn0, asn1))

        for org_set in org_siblings.values():
            if len(org_set) <= 1:
                continue
            for org0, org1 in combinations(org_set, 2):
                org_sibling_of_links.add((iyp_org_id[org0], iyp_org_id[org1]))

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        url_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', urls, all=False)
        # Translate ASNs/URLs to QIDs on the fly.
        self.iyp.batch_add_links('WEBSITE', self.link_generator(website_links, asn_id, url_id))
        logging.info('AS siblings')
        self.iyp.batch_add_links('SIBLING_OF', self.link_generator(asn_sibling_of_links, asn_id, asn_id))
        # Organizations are already QIDs.
        logging.info('Organization siblings')
        self.iyp.batch_add_links('SIBLING_OF', super().link_generator(org_sibling_of_links))

    def unit_test(self):
        return super().unit_test(['SIBLING_OF', 'WEBSITE'])


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

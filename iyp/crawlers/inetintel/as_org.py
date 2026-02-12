import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from itertools import combinations

import requests
from github import Github

from iyp import BaseCrawler, DataNotAvailableError


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
            return file.download_url
    return str()


ORG = 'Internet Intelligence Lab'
URL = 'https://github.com/InetIntel/Dataset-AS-to-Organization-Mapping'
NAME = 'inetintel.as_org'


class Crawler(BaseCrawler):

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
        org_id = self.iyp.batch_get_nodes_by_single_prop('Organization', 'name', all=True, create=False)

        asns = set()
        urls = set()
        website_links = list()
        org_sibling_of_links = set()
        asn_sibling_of_links = set()

        for asn, as_data in data['data'].items():
            asn = int(asn)
            asns.add(asn)

            # Process sibling ASes.
            for sibling_asn in as_data['Sibling ASNs']:
                sibling_asn = int(sibling_asn)
                asns.add(sibling_asn)
                if ((asn, sibling_asn) in asn_sibling_of_links
                        or (sibling_asn, asn) in asn_sibling_of_links
                        # There is at least one instance of this...
                        or asn == sibling_asn):
                    continue
                asn_sibling_of_links.add((asn, sibling_asn))

            # Process PeeringDB sibling organizations.
            pdb_orgs = [org.removeprefix('PDB: ')
                        for org in as_data['Reference Orgs']
                        if org.startswith('PDB: ')]
            for org0, org1 in combinations(pdb_orgs, 2):
                if (org0 not in org_id
                        or org1 not in org_id):
                    continue
                org0_qid = org_id[org0]
                org1_qid = org_id[org1]
                if ((org0_qid, org1_qid) in org_sibling_of_links
                        or (org1_qid, org0_qid) in org_sibling_of_links):
                    continue
                org_sibling_of_links.add((org0_qid, org1_qid))

            # Add website if available.
            url = as_data['Website']
            if url:
                urls.add(url)
                website_links.append((asn, url))

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        url_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', urls, all=False)
        # Translate ASNs/URLs to QIDs on the fly.
        self.iyp.batch_add_links('WEBSITE',
                                 [
                                     {
                                         'src_id': asn_id[asn],
                                         'dst_id': url_id[url],
                                         'props': [self.reference]
                                     }
                                     for asn, url in website_links
                                 ]
                                 )
        logging.info('AS siblings')
        self.iyp.batch_add_links('SIBLING_OF',
                                 [
                                     {
                                         'src_id': asn_id[asn0],
                                         'dst_id': asn_id[asn1],
                                         'props': [self.reference]
                                     }
                                     for asn0, asn1 in asn_sibling_of_links
                                 ])
        # Organizations are already QIDs.
        logging.info('Organization siblings')
        self.iyp.batch_add_links('SIBLING_OF',
                                 [
                                     {
                                         'src_id': org0,
                                         'dst_id': org1,
                                         'props': [self.reference]
                                     }
                                     for org0, org1 in org_sibling_of_links
                                 ])

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

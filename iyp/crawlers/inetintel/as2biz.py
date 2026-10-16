import argparse
import logging
import sys
from datetime import datetime, timezone

import requests
from github import Github

from iyp import BaseCrawler, DataNotAvailableError

ORG = 'Internet Intelligence Lab'
URL = 'https://github.com/InetIntel/Dataset-AS2Biz'
NAME = 'inetintel.as2biz'

GITHUB_REPO = 'InetIntel/Dataset-AS2Biz'
DATA_DIR = 'data'
# Subcategory used by the taxonomy when no finer classification applies. Like the
# stanford.asdb crawler, we do not create a Tag for it and only keep the sector.
OTHER_SUBCATEGORY = 'Other'


def get_latest_dataset_url(github_repo: str, data_dir: str):
    """Return the download URL of the as2biz JSON file found in the latest
    (alphabetically ordered, i.e., YYYY-MM) folder of data_dir in the given
    repository.

    Returns an empty string if no such file is found.
    """
    repo = Github().get_repo(github_repo)
    all_data_dir = sorted(d.path for d in repo.get_contents(data_dir) if d.type == 'dir')
    if not all_data_dir:
        return str()
    for file in repo.get_contents(all_data_dir[-1]):
        if file.name.startswith('as2biz.') and file.name.endswith('.json'):
            return file.download_url
    return str()


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://github.com/InetIntel/Dataset-AS2Biz#readme'

    def run(self):
        """Fetch the latest AS2Biz snapshot and push it to IYP."""
        dataset_url = get_latest_dataset_url(GITHUB_REPO, DATA_DIR)
        if not dataset_url:
            logging.error('Failed to find valid file in repository.')
            raise DataNotAvailableError('Failed to find valid file in repository.')
        self.reference['reference_url_data'] = dataset_url

        logging.info(f'Fetching {dataset_url}')
        req = requests.get(dataset_url)
        req.raise_for_status()
        data = req.json()

        snapshot_month = data['metadata']['snapshot_month']
        self.reference['reference_time_modification'] = datetime.strptime(
            snapshot_month, '%Y-%m'
        ).replace(tzinfo=timezone.utc)
        logging.info(f'Using snapshot {snapshot_month}')

        asns = set()
        categories = set()
        # (asn, layer, category, source). Multiple labels of one AS can belong to the
        # same sector, so layer-1 entries would be duplicated otherwise. Entries with
        # different sources are kept as distinct relationships.
        classification_results = set()
        # (subcategory, sector) hierarchy between Tags.
        tags_relationships = set()

        for asn, labels in data['data'].items():
            asn = int(asn)
            for label, source in labels.items():
                sector, sep, subcategory = label.partition(' - ')
                if not sep or not sector or not subcategory:
                    logging.warning(f'Ignoring unexpected label format for AS{asn}: "{label}"')
                    continue

                asns.add(asn)
                # Layer 1: sector
                categories.add(sector)
                classification_results.add((asn, 1, sector, source))

                # Layer 2: subcategory. Only store the sector for "Other".
                if subcategory == OTHER_SUBCATEGORY:
                    continue
                categories.add(subcategory)
                classification_results.add((asn, 2, subcategory, source))
                tags_relationships.add((subcategory, sector))

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        category_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', categories, all=False)

        part_of_links = [
            {
                'src_id': category_id[subcategory],
                'dst_id': category_id[sector],
                'props': [self.reference],
            }
            for subcategory, sector in tags_relationships
        ]
        self.iyp.batch_add_links('PART_OF', part_of_links)

        categorized_links = [
            {
                'src_id': asn_id[asn],
                'dst_id': category_id[category],
                'props': [self.reference, {'layer': layer, 'source': source}],
            }
            for asn, layer, category, source in classification_results
        ]
        self.iyp.batch_add_links('CATEGORIZED', categorized_links)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED', 'PART_OF'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
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

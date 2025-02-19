import argparse
import gzip
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone

import arrow
import requests

from iyp import BaseCrawler, DataNotAvailableError

# URL to AS2Org API
URL = 'https://publicdata.caida.org/datasets/as-organizations/'
ORG = 'CAIDA'
NAME = 'caida.as2org'


# (:AS)-[:MANAGED_BY]->(:Organization) // Most relevant
# (:Organization)-[:COUNTRY]->(:Country)
# (:Organization)-[:NAME]->(:Name)

class Crawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://publicdata.caida.org/datasets/as-organizations/README.txt'

    def __set_modification_time_from_metadata_line(self, date_str):
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.warning(f'Failed to get modification date from metadata line: {date_str.strip()}')
            logging.warning(e)
            logging.warning('Using date from filename.')

    def run(self):
        date = arrow.now()
        for _ in range(6):
            full_url = URL + f'{date.year}{date.month:02d}01.as-org2info.txt.gz'
            req = requests.head(full_url)

            # Found the latest file
            if req.status_code == 200:
                url = full_url
                break

            date = date.shift(months=-1)

        else:
            # for loop was not 'broken', no file available
            raise DataNotAvailableError('No recent CAIDA as2org file available')
        date = date.datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        self.reference['reference_time_modification'] = date
        self.reference['reference_url_data'] = url

        logging.info(f'Fetching data from: {url}')
        req = requests.get(self.url)
        req.raise_for_status()

        logging.info('Processing data...')

        data = gzip.decompress(req.content).decode()
        lines = data.split('\n')

        lines = [line for line in lines if line.strip()]

        orgs_mode = True
        asn_orgid = dict()
        name_country_orgids = defaultdict(lambda: defaultdict(set))
        name_orgids = defaultdict(set)
        orgid_name = dict()
        countries = set()
        for line in lines:
            if line == '# format:org_id|changed|org_name|country|source':
                orgs_mode = True
            elif line == '# format:aut|changed|aut_name|org_id|opaque_id|source':
                orgs_mode = False
            elif 'program start time' in line:
                date = line.split('program start time:')[1]
                date = date.strip()
                self.__set_modification_time_from_metadata_line(date)

            if line.startswith('#'):
                continue

            fields = line.split('|')

            # extract org information with format:
            # org_id|changed|org_name|country|source
            # NB changed and source fields not used
            if orgs_mode:
                org_id = fields[0]
                if org_id.startswith('@del'):
                    # There are some placeholder organizations with no name and IDs
                    # starting with @del, which probably indicate some old relationship
                    # that no longer exists. Does not make sense to model them, since
                    # they all map to the same Organization node with an empty name.
                    continue
                org_name = fields[2]
                country = fields[3]
                # Index by name, since this the identifier of the Organization node.
                # Keep track of which org ID is the source for a country. Some orgs have
                # multiple IDs mapping them to different countries,
                name_country_orgids[org_name][country].add(org_id)
                countries.add(country)
                # Some orgs (with the same name) map to multiple IDs.
                name_orgids[org_name].add(org_id)
                orgid_name[org_id] = org_name

            # extract org to as mapping with format:
            # aut|changed|aut_name|org_id|opaque_id|source
            # NB changed, aut_name, opaque_id, and source fields not used
            else:
                asn = int(fields[0])
                org_id = fields[3]
                if org_id.startswith('@del'):
                    continue
                asn_orgid[asn] = org_id

        names = set(name_orgids.keys())
        org_ids = set(orgid_name.keys())
        ases = set(asn_orgid.keys())
        caida_org_id = self.iyp.batch_get_nodes_by_single_prop('CaidaOrgID', 'id', org_ids)
        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', ases)
        organization_id = self.iyp.batch_get_nodes_by_single_prop('Organization', 'name', names)
        name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)

        managed_links = list()
        for asn, org_id in asn_orgid.items():
            org_qid = organization_id[orgid_name[org_id]]
            asn_qid = as_id[asn]
            managed_links.append({'src_id': asn_qid, 'dst_id': org_qid,
                                 'props': [self.reference, {'org_id': org_id}]})

        name_links = list()
        country_links = list()
        external_id_links = list()

        for name in name_orgids:
            org_ids = name_orgids[name]
            org_qid = organization_id[name]
            name_qid = name_id[name]

            name_links.append({'src_id': org_qid, 'dst_id': name_qid,
                               'props': [self.reference, {'org_ids': list(org_ids)}]})

            for org_id in org_ids:
                caida_org_id_qid = caida_org_id[org_id]
                external_id_links.append({'src_id': org_qid, 'dst_id': caida_org_id_qid, 'props': [self.reference]})

            for country, org_ids in name_country_orgids[name].items():
                country_qid = country_id[country]
                country_links.append({'src_id': org_qid, 'dst_id': country_qid,
                                      'props': [self.reference, {'org_ids': list(org_ids)}]})

        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('EXTERNAL_ID', external_id_links)
        self.iyp.batch_add_links('MANAGED_BY', managed_links)
        self.iyp.batch_add_links('NAME', name_links)

    def unit_test(self):
        return super().unit_test(['COUNTRY', 'EXTERNAL_ID', 'MANAGED_BY', 'NAME'])


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

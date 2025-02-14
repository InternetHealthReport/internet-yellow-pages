import argparse
import gzip
import json
import logging
import sys
from datetime import datetime, timezone

import arrow
import requests

from iyp import BaseCrawler, RequestStatusError

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

        date = arrow.now()
        for _ in range(6):
            full_url = url + f'{date.year}{date.month:02d}01.as-org2info.txt.gz'
            req = requests.head(full_url)

            # Found the latest file
            if req.status_code == 200:
                url = full_url
                break

            date = date.shift(months=-1)

        else:
            # for loop was not 'broken', no file available
            raise Exception('No recent CAIDA as2org file available')
        date = date.datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        logging.info('going to use this URL: ' + url)
        super().__init__(organization, url, name)
        self.reference['reference_time_modification'] = date

    def __set_modification_time_from_metadata_line(self, date_str):
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.warning(f'Failed to get modification date from metadata line: {date_str.strip()}')
            logging.warning(e)
            logging.warning('Using date from filename.')

    def run(self):
        req = requests.get(self.url)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching CAIDA as2org file')

        data = gzip.decompress(req.content).decode()
        lines = data.split('\n')

        lines = [line for line in lines if line.strip()]

        orgs_mode = True
        org_as = {}
        org_countries = {}
        org_names = {}
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
                orgid = fields[0]
                org_name = fields[2]
                country = fields[3]
                org_countries[orgid] = country
                org_names[orgid] = org_name

            # extract org to as mapping with format:
            # aut|changed|aut_name|org_id|opaque_id|source
            # NB changed, aut_name, opaque_id, and source fields not used
            else:
                asn = int(fields[0])
                orgid = fields[3]
                org_as[asn] = orgid

        names = set(org_names.values())
        countries = set(org_countries.values())
        ases = org_as.keys()
        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', ases)
        organization_id = self.iyp.batch_get_nodes_by_single_prop('Organization', 'name', names)
        name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)
        managed_links = []

        for asn in org_as:
            org_qid = organization_id.get(org_names[org_as[asn]])
            asn_qid = as_id.get(asn)
            managed_links.append({'src_id': asn_qid, 'dst_id': org_qid,
                                 'props': [self.reference]})

        self.iyp.batch_add_links('MANAGED_BY', managed_links)

        named_links = []
        country_links = []

        for org in org_names:
            name = org_names[org]
            country = org_countries[org]
            org_qid = organization_id.get(name)
            name_qid = name_id.get(name)
            country_qid = country_id.get(country)

            named_links.append({'src_id': org_qid, 'dst_id': name_qid, 'props': [self.reference]})
            country_links.append({'src_id': org_qid, 'dst_id': country_qid, 'props': [self.reference]})

        self.iyp.batch_add_links('NAME', named_links)
        self.iyp.batch_add_links('COUNTRY', country_links)

    def unit_test(self):
        return super().unit_test(['NAME', 'COUNTRY', 'MANAGED_BY'])


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

import argparse
import json
import logging
import os
import sys
from datetime import timedelta

import flatdict
import iso3166
import requests_cache
from neo4j.spatial import WGS84Point

from iyp import BaseCrawler
from iyp.crawlers.peeringdb.ix import (handle_social_media,
                                       set_reference_time_from_metadata)

ORG = 'PeeringDB'

# URL to peeringdb API for organizations
URL = 'https://peeringdb.com/api/org'
NAME = 'peeringdb.org'

# Label used for the class/item representing the organization IDs
ORGID_LABEL = 'PeeringdbOrgID'

API_KEY = ''
CACHE_DIR = ''
CACHE_DURATION = requests_cache.DO_NOT_CACHE
if os.path.exists('config.json'):
    with open('config.json', 'r') as f:
        config = json.load(f)
    API_KEY = config['peeringdb']['apikey']
    CACHE_DIR = config['cache']['directory']
    CACHE_DURATION = timedelta(days=config['cache']['duration_in_days'])
    del config  # Do not leave as a global variable.


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialisation for pushing peeringDB organizations to IYP."""

        self.headers = {'Authorization': 'Api-Key ' + API_KEY}
        self.requests = requests_cache.CachedSession(os.path.join(CACHE_DIR, ORG), expire_after=CACHE_DURATION)

        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://www.peeringdb.com/apidocs/#tag/api/operation/list%20org'

    def run(self):
        """Fetch organizations information from PeeringDB and push to IYP."""

        req = self.requests.get(URL, headers=self.headers)
        req.raise_for_status()

        result = req.json()
        set_reference_time_from_metadata(self.reference, result)
        organizations = result['data']

        # compute nodes
        orgs = set()
        names = set()
        websites = set()
        countries = set()
        points = set()
        orgids = set()

        for org in organizations:
            orgs.add(org['name'].strip())
            names.add(org['name'].strip())
            orgids.add(org['id'])

            if org['website']:
                websites.add(org['website'].strip())

            if org['country'] in iso3166.countries_by_alpha2:
                countries.add(org['country'])

            if org['latitude'] and org['longitude']:
                position = WGS84Point((org['longitude'], org['latitude']))
                points.add(position)

            handle_social_media(org, websites)

        # push nodes
        self.org_id = self.iyp.batch_get_nodes_by_single_prop('Organization', 'name', orgs)
        self.name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names)
        self.website_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', websites, all=False)
        self.country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries, all=False)
        self.point_id = self.iyp.batch_get_nodes_by_single_prop('Point', 'position', points)
        self.orgid_id = self.iyp.batch_get_nodes_by_single_prop(ORGID_LABEL, 'id', orgids)

        # compute links
        name_links = []
        website_links = []
        country_links = []
        point_links = []
        orgid_links = []

        for org in organizations:

            flat_org = {}
            try:
                flat_org = dict(flatdict.FlatDict(org))
            except Exception as e:
                logging.error(f'Cannot flatten dictionary {org}\n{e}')

            orgid_qid = self.orgid_id[org['id']]
            org_qid = self.org_id[org['name'].strip()]
            orgid_links.append({'src_id': org_qid, 'dst_id': orgid_qid, 'props': [self.reference, flat_org]})

            name_qid = self.name_id[org['name'].strip()]
            name_links.append({'src_id': org_qid, 'dst_id': name_qid, 'props': [self.reference]})

            if 'website' in org and org['website'] in self.website_id:
                website_qid = self.website_id[org['website'].strip()]
                website_links.append({'src_id': org_qid, 'dst_id': website_qid, 'props': [self.reference]})

            if 'country' in org and org['country'] in self.country_id:
                country_qid = self.country_id[org['country']]
                country_links.append({'src_id': org_qid, 'dst_id': country_qid, 'props': [self.reference]})

            if org['latitude'] and org['longitude']:
                position = WGS84Point((org['longitude'], org['latitude']))
                point_qid = self.point_id[position]
                point_links.append({'src_id': org_qid, 'dst_id': point_qid, 'props': [self.reference]})

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('LOCATED_IN', point_links)
        self.iyp.batch_add_links('EXTERNAL_ID', orgid_links)

    def unit_test(self):
        return super().unit_test(['NAME', 'WEBSITE', 'COUNTRY', 'EXTERNAL_ID', 'LOCATED_IN'])


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

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

# NOTES This script should be executed after peeringdb.org

ORG = 'PeeringDB'

# URL to peeringdb API for facilities
URL = 'https://peeringdb.com/api/fac'
NAME = 'peeringdb.fac'

# Label used for the nodes representing the organization and facility IDs
ORGID_LABEL = 'PeeringdbOrgID'
FACID_LABEL = 'PeeringdbFacID'

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
        """Initialisation for pushing peeringDB facilities to IYP."""

        self.headers = {'Authorization': 'Api-Key ' + API_KEY}
        self.requests = requests_cache.CachedSession(os.path.join(CACHE_DIR, ORG), expire_after=CACHE_DURATION)

        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://www.peeringdb.com/apidocs/#tag/api/operation/list%20fac'

    def run(self):
        """Fetch facilities information from PeeringDB and push to IYP."""

        logging.info('Fetching PeeringDB data...')
        req = self.requests.get(URL, headers=self.headers)
        req.raise_for_status()

        result = req.json()
        set_reference_time_from_metadata(self.reference, result)
        facilities = result['data']

        # compute nodes
        facs = set()
        names = set()
        websites = set()
        countries = set()
        points = set()
        facids = set()

        for fac in facilities:
            facs.add(fac['name'].strip())
            names.add(fac['name'].strip())
            facids.add(fac['id'])

            if fac['website']:
                websites.add(fac['website'].strip())

            if fac['country'] in iso3166.countries_by_alpha2:
                countries.add(fac['country'])

            if fac['latitude'] and fac['longitude']:
                position = WGS84Point((fac['longitude'], fac['latitude']))
                points.add(position)

            handle_social_media(fac, websites)

        # push nodes
        self.fac_id = self.iyp.batch_get_nodes_by_single_prop('Facility', 'name', facs)
        self.name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names)
        self.website_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', websites)
        self.country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)
        self.point_id = self.iyp.batch_get_nodes_by_single_prop('Point', 'position', points)
        self.facid_id = self.iyp.batch_get_nodes_by_single_prop(FACID_LABEL, 'id', facids)

        # get organization nodes
        self.org_id = self.iyp.batch_get_node_extid(ORGID_LABEL)

        # compute links
        name_links = []
        website_links = []
        country_links = []
        point_links = []
        facid_links = []
        org_links = []

        for fac in facilities:

            flat_fac = {}
            try:
                flat_fac = dict(flatdict.FlatDict(fac))
            except Exception as e:
                logging.error(f'Cannot flatten dictionary {fac}\n{e}')

            facid_qid = self.facid_id[fac['id']]
            fac_qid = self.fac_id[fac['name'].strip()]
            facid_links.append({'src_id': fac_qid, 'dst_id': facid_qid, 'props': [self.reference, flat_fac]})

            name_qid = self.name_id[fac['name'].strip()]
            name_links.append({'src_id': fac_qid, 'dst_id': name_qid, 'props': [self.reference]})

            if 'website' in fac and fac['website'] in self.website_id:
                website_qid = self.website_id[fac['website'].strip()]
                website_links.append({'src_id': fac_qid, 'dst_id': website_qid, 'props': [self.reference]})

            if 'country' in fac and fac['country'] in self.country_id:
                country_qid = self.country_id[fac['country']]
                country_links.append({'src_id': fac_qid, 'dst_id': country_qid, 'props': [self.reference]})

            if fac['latitude'] and fac['longitude']:
                position = WGS84Point((fac['longitude'], fac['latitude']))
                point_qid = self.point_id[position]
                point_links.append({'src_id': fac_qid, 'dst_id': point_qid, 'props': [self.reference]})

            org_qid = self.org_id[fac['org_id']]
            org_links.append({'src_id': fac_qid, 'dst_id': org_qid, 'props': [self.reference]})

        # Push all links to IYP
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('LOCATED_IN', point_links)
        self.iyp.batch_add_links('EXTERNAL_ID', facid_links)
        self.iyp.batch_add_links('MANAGED_BY', org_links)

    def unit_test(self):
        return super().unit_test(['NAME', 'WEBSITE', 'COUNTRY', 'EXTERNAL_ID', 'MANAGED_BY', 'LOCATED_IN'])


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

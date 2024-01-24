import argparse
import json
import logging
import os
import sys
from datetime import datetime, time, timedelta, timezone

import flatdict
import requests_cache

from iyp import BaseCrawler

# NOTES This script should be executed after peeringdb.org
# TODO add the type PEERING_LAN? may break the unique constraint

ORG = 'PeeringDB'
URL = ''
NAME = 'peeringdb.ix'

# URL to peeringdb API for exchange points
URL_PDB_IXS = 'https://peeringdb.com/api/ix?depth=2'
# API endpoint for LAN prefixes
URL_PDB_LANS = 'https://peeringdb.com/api/ixlan?depth=2'
# API endpoint for network/facility links
URL_PDB_NETFAC = 'https://peeringdb.com/api/netfac'

# Label used for nodes representing the exchange point IDs
IXID_LABEL = 'PeeringdbIXID'
# Label used for nodes representing the organization IDs
ORGID_LABEL = 'PeeringdbOrgID'
# Label used for the nodes representing the network IDs
NETID_LABEL = 'PeeringdbNetID'
# Label used for the nodes representing the facility IDs
FACID_LABEL = 'PeeringdbFacID'

API_KEY = ''
if os.path.exists('config.json'):
    API_KEY = json.load(open('config.json', 'r'))['peeringdb']['apikey']


def handle_social_media(d: dict, website_set: set = None):
    """Flatten list of social media dictionaries in place and add the website to
    website_set if present."""
    if 'social_media' in d:
        social_media_list = d.pop('social_media')
        for entry in social_media_list:
            service = entry['service']
            identifier = entry['identifier']
            if website_set is not None and service == 'website':
                website_set.add(identifier.strip())
            d[f'social_media_{service}'] = identifier


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialisation for pushing peeringDB IXPs to IYP."""

        self.headers = {'Authorization': 'Api-Key ' + API_KEY}

        self.reference_ix = {
            'reference_org': ORG,
            'reference_name': NAME,
            'reference_url': URL_PDB_IXS,
            'reference_time': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
        }

        self.reference_lan = {
            'reference_org': ORG,
            'reference_name': NAME,
            'reference_url': URL_PDB_LANS,
            'reference_time': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
        }

        self.reference_netfac = {
            'reference_org': ORG,
            'reference_name': NAME,
            'reference_url': URL_PDB_NETFAC,
            'reference_time': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
        }

        # keep track of added networks
        self.nets = {}

        # Using cached queries
        self.requests = requests_cache.CachedSession(f'tmp/{ORG}', expire_after=timedelta(days=6))

        # connection to IYP database
        super().__init__(organization, url, name)

    def run(self):
        """Fetch ixs information from PeeringDB and push to IYP.

        Using multiple threads for better performances.
        """

        # get organization, country nodes
        self.org_id = self.iyp.batch_get_node_extid(ORGID_LABEL)
        self.fac_id = self.iyp.batch_get_node_extid(FACID_LABEL)
        self.country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code')

        req = self.requests.get(URL_PDB_IXS, headers=self.headers)
        if req.status_code != 200:
            logging.error(f'Error while fetching IXs data\n({req.status_code}) {req.text}')
            raise Exception(f'Cannot fetch peeringdb data, status code={req.status_code}\n{req.text}')

        self.ixs = json.loads(req.text)['data']

        # Register IXPs
        logging.warning('Pushing IXP info...')
        self.register_ixs()
        self.ix_id = self.iyp.batch_get_node_extid(IXID_LABEL)

        req = self.requests.get(URL_PDB_LANS, headers=self.headers)
        if req.status_code != 200:
            logging.error(f'Error while fetching IXLANs data\n({req.status_code}) {req.text}')
            raise Exception(f'Cannot fetch peeringdb data, status code={req.status_code}\n{req.text}')

        ixlans = json.loads(req.text)['data']

        # index ixlans by their id
        self.ixlans = {}
        for ixlan in ixlans:
            self.ixlans[ixlan['id']] = ixlan

        logging.warning('Pushing IXP LAN and members...')
        self.register_ix_membership()
        self.iyp.commit()

        # Link network to facilities
        req = self.requests.get(URL_PDB_NETFAC, headers=self.headers)
        if req.status_code != 200:
            logging.error(f'Error while fetching IXLANs data\n({req.status_code}) {req.text}')
            raise Exception(f'Cannot fetch peeringdb data, status code={req.status_code}\n{req.text}')

        self.netfacs = json.loads(req.text)['data']
        self.register_net_fac()

    def register_net_fac(self):
        """Link ASes to facilities."""

        net_id = self.iyp.batch_get_node_extid(NETID_LABEL)

        # compute links
        netfac_links = []

        for netfac in self.netfacs:
            if netfac['net_id'] not in net_id:
                as_qid = self.iyp.get_node('AS', {'asn': netfac['local_asn']})
                extid_qid = self.iyp.get_node(NETID_LABEL, {'id': netfac['net_id']})
                links = [['EXTERNAL_ID', extid_qid, self.reference_netfac]]
                self.iyp.add_links(as_qid, links)
                net_id[netfac['net_id']] = as_qid

            if netfac['fac_id'] not in self.fac_id:
                logging.error(f'Facility not found: net ID {netfac["fac_id"]} not registered')
                continue

            net_qid = net_id[netfac['net_id']]
            fac_qid = self.fac_id[netfac['fac_id']]
            flat_netfac = dict(flatdict.FlatDict(netfac))

            netfac_links.append({'src_id': net_qid, 'dst_id': fac_qid,
                                 'props': [self.reference_netfac, flat_netfac]})

        # Push links to IYP
        self.iyp.batch_add_links('LOCATED_IN', netfac_links)

    def register_ix_membership(self):
        """Add IXPs LAN and members."""

        # Create prefix nodes
        prefixes = set()
        net_names = set()
        net_extid = set()
        net_website = set()
        net_asn = set()

        for ix in self.ixs:
            if 'ixlan_set' in ix:
                for ixlan in ix['ixlan_set']:
                    if ixlan['id'] not in self.ixlans:
                        logging.error(f'LAN not found: ixlan ID {ixlan["id"]} not in {self.ixlans}')
                        continue

                    lan = self.ixlans[ixlan['id']]

                    for prefix in lan['ixpfx_set']:
                        prefixes.add(prefix['prefix'])

                    for network in lan['net_set']:
                        net_names.add(network['name'])
                        net_asn.add(int(network['asn']))
                        net_extid.add(network['id'])
                        net_website.add(network['website'])
                        handle_social_media(network, net_website)

        # TODO add the type PEERING_LAN? may break the unique constraint
        self.prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes)
        self.name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', net_names)
        self.website_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', net_website)
        self.netid_id = self.iyp.batch_get_nodes_by_single_prop(NETID_LABEL, 'id', net_extid)
        self.asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', net_asn)

        # compute links
        prefix_links = []
        name_links = []
        website_links = []
        netid_links = []
        netorg_links = []
        member_links = []

        processed_net = set()
        processed_membership = set()

        for ix in self.ixs:
            if 'ixlan_set' in ix:
                for ixlan in ix['ixlan_set']:
                    if ixlan['id'] not in self.ixlans:
                        logging.error(f'LAN not found: ixlan ID {ixlan["id"]} not in {self.ixlans}')
                        continue

                    ix_qid = self.ix_id[ix['id']]
                    lan = self.ixlans[ixlan['id']]

                    for prefix in lan['ixpfx_set']:
                        prefix_qid = self.prefix_id[prefix['prefix']]
                        prefix_links.append({'src_id': prefix_qid, 'dst_id': ix_qid,
                                             'props': [self.reference_lan]})

                    # Add networks found for the LAN
                    for network in lan['net_set']:

                        net_asn = int(network['asn'])
                        flat_net = dict(flatdict.FlatDict(network))
                        network_qid = self.asn_id[int(network['asn'])]

                        if f'{network_qid}-{ix_qid}' in processed_membership:
                            continue

                        if net_asn not in processed_net:
                            # Add network name, website and external ID
                            # (only once)

                            netid_qid = self.netid_id[network['id']]
                            name_qid = self.name_id[network['name']]
                            website_qid = self.website_id[network['website']]

                            if network['org_id'] in self.org_id:
                                org_qid = self.org_id[network['org_id']]
                                netorg_links.append({'src_id': network_qid, 'dst_id': org_qid,
                                                     'props': [self.reference_lan, flat_net]})
                            else:
                                logging.error(f'Organization unknown org_id={network["org_id"]}')

                            name_links.append({'src_id': network_qid, 'dst_id': name_qid,
                                               'props': [self.reference_lan, flat_net]})
                            website_links.append({'src_id': network_qid, 'dst_id': website_qid,
                                                  'props': [self.reference_lan, flat_net]})
                            netid_links.append({'src_id': network_qid, 'dst_id': netid_qid,
                                                'props': [self.reference_lan, flat_net]})

                            # Remember that this network has been processed
                            processed_net.add(net_asn)

                        member_links.append({'src_id': network_qid, 'dst_id': ix_qid,
                                             'props': [self.reference_lan, flat_net]})
                        processed_membership.add(f'{network_qid}-{ix_qid}')

        # Push all links to IYP
        self.iyp.batch_add_links('MANAGED_BY', prefix_links)
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('MEMBER_OF', member_links)
        self.iyp.batch_add_links('EXTERNAL_ID', netid_links)
        self.iyp.batch_add_links('MANAGED_BY', netorg_links)

    def register_ixs(self):
        """Add IXs to IYP and populate corresponding nodes' ID."""

        # Create nodes
        all_ixs_id = set()
        all_ixs_name = set()
        all_ixs_website = set()
        for ix in self.ixs:
            all_ixs_id.add(ix['id'])
            all_ixs_name.add(ix['name'])
            all_ixs_website.add(ix['website'])
            handle_social_media(ix, all_ixs_website)

        self.ixext_id = self.iyp.batch_get_nodes_by_single_prop(IXID_LABEL, 'id', all_ixs_id)
        self.ix_id = self.iyp.batch_get_nodes_by_single_prop('IXP', 'name', all_ixs_name)
        self.website_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', all_ixs_website)
        self.name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', all_ixs_name)

        # Compute links
        name_links = []
        org_links = []
        fac_links = []
        country_links = []
        id_links = []
        website_links = []

        for ix in self.ixs:
            ix_qid = self.ix_id[ix['name']]
            org_qid = self.org_id.get(ix['org_id'], None)

            # link to corresponding organization
            if org_qid is not None:
                org_links.append({'src_id': ix_qid, 'dst_id': org_qid, 'props': [self.reference_ix]})
            else:
                logging.error(f'Error this organization is not in IYP: {ix["org_id"]}')

            # link to corresponding facilities
            for fac in ix.get('fac_set', []):
                fac_qid = self.fac_id.get(fac['id'], None)
                if fac_qid is not None:
                    fac_links.append({'src_id': ix_qid, 'dst_id': fac_qid, 'props': [self.reference_ix]})
                else:
                    logging.error(f'Error this facility is not in IYP: {fac["id"]}')

            # set country
            if ix['country']:
                country_qid = self.country_id[ix['country']]
                country_links.append({'src_id': ix_qid, 'dst_id': country_qid, 'props': [self.reference_ix]})

            # set website
            if ix['website']:
                website_qid = self.website_id[ix['website']]
                website_links.append({'src_id': ix_qid, 'dst_id': website_qid, 'props': [self.reference_ix]})
            # set social media website if different from normal website
            if 'social_media_website' in ix and ix['social_media_website'] != ix['website']:
                website_qid = self.website_id[ix['social_media_website']]
                website_links.append({'src_id': ix_qid, 'dst_id': website_qid, 'props': [self.reference_ix]})

            id_qid = self.ixext_id[ix['id']]
            id_links.append({'src_id': ix_qid, 'dst_id': id_qid, 'props': [self.reference_ix]})

            id_name = self.name_id[ix['name']]
            name_links.append({'src_id': ix_qid, 'dst_id': id_name, 'props': [self.reference_ix]})

        # set traffic webpage
        # if ix['url_stats']:
            # statements.append([
            # self.wh.get_pid('website'), ix['url_stats'],  # statement

        # Push all links to IYP
        self.iyp.batch_add_links('MANAGED_BY', org_links)
        self.iyp.batch_add_links('LOCATED_IN', fac_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('EXTERNAL_ID', id_links)
        self.iyp.batch_add_links('NAME', name_links)


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
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

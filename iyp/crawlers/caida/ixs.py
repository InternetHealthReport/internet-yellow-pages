import argparse
import ipaddress
import json
import logging
import sys
from datetime import datetime, timezone

import arrow
import requests
from iso3166 import countries as cc_convert

from iyp import BaseCrawler

URL = 'https://publicdata.caida.org/datasets/ixps/'
ORG = 'CAIDA'
NAME = 'caida.ixs'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        """Initialization: Find the latest file and set the URL"""

        date = arrow.now()

        for _ in range(6):
            full_url = url + f'ixs_{date.year}{date.month:02d}.jsonl'
            req = requests.head(full_url)

            # Found the latest file
            if req.status_code == 200:
                url = full_url
                break

            date = date.shift(months=-1)

        else:
            # for loop was not 'broken', no file available
            raise Exception('No recent CAIDA ix-asns file available')
        date = date.datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        logging.info('going to use this URL: ' + url)
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://publicdata.caida.org/datasets/ixps/README.txt'
        self.reference['reference_time_modification'] = date

    def __set_modification_time_from_metadata_line(self, line):
        try:
            date_str = json.loads(line.lstrip('#'))['date']
            date = datetime.strptime(date_str, '%Y.%m.%d %H:%M:%S').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.warning(f'Failed to get modification date from metadata line: {line.strip()}')
            logging.warning(e)
            logging.warning('Using date from filename.')

    def run(self):
        """Fetch the latest file and process lines one by one."""

        req = requests.get(self.url)
        req.raise_for_status()

        lines = []
        caida_ids = set()
        pdb_ids = set()
        names = set()
        prefixes = set()
        urls = set()
        countries = set()

        # Find all possible values and create corresponding nodes
        for line in req.text.splitlines():
            if line.startswith('#'):
                self.__set_modification_time_from_metadata_line(line)
                continue

            ix = json.loads(line)
            lines.append(ix)

            caida_ids.add(ix.get('ix_id'))

            if ix.get('pdb_id'):
                pdb_ids.add(ix.get('pdb_id'))

            if ix.get('name'):
                names.add(ix.get('name'))

            # usually a single country code but can be a list of country codes
            if ix.get('country'):
                ixcc = ix.get('country')
                if isinstance(ixcc, list):
                    for cc in ixcc:
                        # We sometimes have a non-standard long name and the
                        # country code. We can ignore non-standard names
                        try:
                            countries.add(cc_convert.get(cc).alpha2)
                        except BaseException:
                            logging.warning(f'Unknown country: {cc}')
                else:
                    try:
                        countries.add(cc_convert.get(ix.get('country')).alpha2)
                    except BaseException:
                        logging.warning(f'Unknown country: {ix.get("country")}')

            # usually a single URL but can be a list of URLs
            if ix.get('url'):
                ixurl = ix.get('url')
                if isinstance(ixurl, list):
                    for url in ixurl:
                        urls.add(url)
                else:
                    urls.add(ix.get('url'))

            # IPv4 an IPv6 prefixes
            if ix.get('prefixes'):
                for pfx_af in ix['prefixes'].values():
                    for pfx in pfx_af:
                        pfx = ipaddress.ip_network(pfx).compressed
                        prefixes.add(pfx)

        # get node IDs for ASNs, names, and countries
        caida_id = self.iyp.batch_get_nodes_by_single_prop('CaidaIXID', 'id', caida_ids)
        ixp_id = self.iyp.batch_get_node_extid('PeeringdbIXID')
        name_id = self.iyp.batch_get_nodes_by_single_prop('Name', 'name', names)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)
        url_id = self.iyp.batch_get_nodes_by_single_prop('URL', 'url', urls)
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes, all=False)
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'PeeringLAN')

        # Compute links and add them to neo4j
        caida_id_links = []
        name_links = []
        country_links = []
        website_links = []
        prefix_links = []

        for ix in lines:
            caida_qid = caida_id[ix['ix_id']]
            name_qid = name_id[ix['name']]

            # optional attributes
            ixp_qid = ixp_id.get(ix.get('pdb_id'))

            if ixp_qid is None:
                # IXP not in PeeringDB:
                # Create this IXP, this should be rare.
                ixp_qid = self.iyp.get_node('IXP', {'name': ix['name']})

            # Compute new links
            caida_id_links.append({'src_id': ixp_qid, 'dst_id': caida_qid,
                                   'props': [self.reference]})

            name_links.append({'src_id': ixp_qid, 'dst_id': name_qid,
                               'props': [self.reference]})

            if 'country' in ix:
                ixcc = ix.get('country')
                if isinstance(ixcc, list):
                    for cc in ixcc:
                        # We sometimes have a non-standard long name and the
                        # country code. We can ignore non-standard names
                        try:
                            country_qid = country_id[cc_convert.get(cc).alpha2]
                            country_links.append({'src_id': ixp_qid, 'dst_id': country_qid,
                                                  'props': [self.reference]})
                        except BaseException:
                            logging.warning(f'Unknown country: {cc}')
                else:
                    try:
                        country_qid = country_id[cc_convert.get(ix['country']).alpha2]
                        country_links.append({'src_id': ixp_qid, 'dst_id': country_qid,
                                              'props': [self.reference]})
                    except BaseException:
                        logging.warning(f'Unknown country: {ix["country"]}')

            if 'url' in ix:
                urls = ix.get('url')
                if isinstance(urls, list):
                    for url in urls:
                        url_qid = url_id[url]
                        website_links.append({'src_id': ixp_qid, 'dst_id': url_qid,
                                              'props': [self.reference]})
                else:
                    url_qid = url_id[ix['url']]
                    website_links.append({'src_id': ixp_qid, 'dst_id': url_qid,
                                          'props': [self.reference]})

            if 'prefixes' in ix:
                for pfx_af in ix['prefixes'].values():
                    for pfx in pfx_af:
                        pfx = ipaddress.ip_network(pfx).compressed
                        pfx_qid = prefix_id[pfx]
                        prefix_links.append({'src_id': pfx_qid, 'dst_id': ixp_qid,
                                             'props': [self.reference]})

        # Push all links to IYP
        self.iyp.batch_add_links('EXTERNAL_ID', caida_id_links)
        self.iyp.batch_add_links('NAME', name_links)
        self.iyp.batch_add_links('COUNTRY', country_links)
        self.iyp.batch_add_links('WEBSITE', website_links)
        self.iyp.batch_add_links('MANAGED_BY', prefix_links)

    def unit_test(self):
        return super().unit_test(['EXTERNAL_ID', 'NAME', 'COUNTRY', 'WEBSITE', 'MANAGED_BY'])


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

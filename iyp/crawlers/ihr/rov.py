import argparse
import csv
import io
import logging
import sys
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network

import lz4.frame
import requests

from iyp import BaseCrawler, DataNotAvailableError

# URL to the API
URL = 'https://archive.ihr.live/ihr/rov/%Y/%m/%d/ihr_rov_%Y-%m-%d.csv.lz4'
ORG = 'IHR'
NAME = 'ihr.rov'


def replace_link_ids(links: list, src_id: dict, dst_id: dict):
    for link in links:
        link['src_id'] = src_id[link['src_id']]
        link['dst_id'] = dst_id[link['dst_id']]


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://archive.ihr.live/ihr/rov/README.txt'

    def run(self):
        """Fetch data from file and push to IYP."""
        today = datetime.now(tz=timezone.utc)
        max_lookback = today - timedelta(days=7)
        url = today.strftime(self.url)
        logging.info(url)
        req = requests.head(url)
        while req.status_code != 200 and today > max_lookback:
            today -= timedelta(days=1)
            url = today.strftime(self.url)
            logging.info(url)
            req = requests.head(url)
        if req.status_code != 200:
            logging.error('Failed to find data within the specified lookback interval.')
            raise DataNotAvailableError('Failed to find data within the specified lookback interval.')

        self.reference['reference_url_data'] = url
        self.reference['reference_time_modification'] = today.replace(hour=0,
                                                                      minute=0,
                                                                      second=0,
                                                                      microsecond=0,
                                                                      tzinfo=timezone.utc)

        logging.info(f'Fetching data from: {url}')
        req = requests.get(url)
        req.raise_for_status()

        with lz4.frame.open(io.BytesIO(req.content)) as f:
            csv_lines = [l.decode('utf-8').rstrip() for l in f]

        asns = set()
        prefixes = set()
        tags = set()
        countries = set()

        orig_links = list()
        tag_links = list()
        dep_links = list()
        country_links = list()

        logging.info('Computing links...')
        for rec in csv.DictReader(csv_lines):
            # header
            # id, timebin, prefix, hege, af, visibility, rpki_status, irr_status,
            # delegated_prefix_status, delegated_asn_status, descr, moas, asn_id,
            # country_id, originasn_id

            rec['hege'] = float(rec['hege'])
            rec['visibility'] = float(rec['visibility'])
            rec['af'] = int(rec['af'])

            try:
                prefix = ip_network(rec['prefix']).compressed
            except ValueError as e:
                logging.warning(f'Ignoring malformed prefix: "{rec["prefix"]}": {e}')
                continue
            prefixes.add(prefix)

            # make status/country/origin links only for lines where asn=originasn
            if rec['asn_id'] == rec['originasn_id']:
                # Make sure all nodes exist
                originasn = int(rec['originasn_id'])
                rpki_status = 'RPKI ' + rec['rpki_status']
                irr_status = 'IRR ' + rec['irr_status']
                cc = rec['country_id']

                asns.add(originasn)
                tags.add(rpki_status)
                tags.add(irr_status)
                countries.add(cc)

                # Compute links
                orig_links.append({
                    'src_id': originasn,
                    'dst_id': prefix,
                    'props': [self.reference, rec]
                })

                tag_links.append({
                    'src_id': prefix,
                    'dst_id': rpki_status,
                    'props': [self.reference, rec]
                })

                tag_links.append({
                    'src_id': prefix,
                    'dst_id': irr_status,
                    'props': [self.reference, rec]
                })

                country_links.append({
                    'src_id': prefix,
                    'dst_id': cc,
                    'props': [self.reference]
                })

            # Dependency links
            asn = int(rec['asn_id'])
            asns.add(asn)

            dep_links.append({
                'src_id': prefix,
                'dst_id': asn,
                'props': [self.reference, rec]
            })

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns)
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes, all=False)
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'BGPPrefix')
        tag_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label', tags, all=False)
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', countries)

        replace_link_ids(orig_links, asn_id, prefix_id)
        replace_link_ids(tag_links, prefix_id, tag_id)
        replace_link_ids(country_links, prefix_id, country_id)
        replace_link_ids(dep_links, prefix_id, asn_id)

        self.iyp.batch_add_links('ORIGINATE', orig_links)
        self.iyp.batch_add_links('CATEGORIZED', tag_links)
        self.iyp.batch_add_links('DEPENDS_ON', dep_links)
        self.iyp.batch_add_links('COUNTRY', country_links)

    def unit_test(self):
        return super().unit_test(['ORIGINATE', 'CATEGORIZED', 'DEPENDS_ON', 'COUNTRY'])


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

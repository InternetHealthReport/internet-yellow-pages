import argparse
import csv
import logging
import os
import sys
from datetime import timezone
from ipaddress import ip_network

import arrow
import lz4.frame
import requests

from iyp import BaseCrawler

# NOTE: Assumes ASNs and Prefixes are already registered in the database. Run
# bgpkit.pfx2asn before this one

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/rov/{year}/{month:02d}/{day:02d}/ihr_rov_{year}-{month:02d}-{day:02d}.csv.lz4'
ORG = 'IHR'
NAME = 'ihr.rov'


class lz4Csv:
    def __init__(self, filename):
        """Start reading a lz4 compress csv file."""

        self.fp = lz4.frame.open(filename)

    def __iter__(self):
        """Read file header line and set self.fields."""
        line = self.fp.readline()
        self.fields = line.decode('utf-8').rstrip().split(',')
        return self

    def __next__(self):
        line = self.fp.readline().decode('utf-8').rstrip()

        if len(line) > 0:
            return line
        else:
            raise StopIteration

    def close(self):
        self.fp.close()


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://ihr-archive.iijlab.net/ihr/rov/README.txt'

    def run(self):
        """Fetch data from file and push to IYP."""

        today = arrow.utcnow()
        url = URL.format(year=today.year, month=today.month, day=today.day)
        req = requests.head(url)
        if req.status_code != 200:
            today = today.shift(days=-1)
            url = URL.format(year=today.year, month=today.month, day=today.day)
            req = requests.head(url)
            if req.status_code != 200:
                today = today.shift(days=-1)
                url = URL.format(year=today.year, month=today.month, day=today.day)

        self.reference['reference_url_data'] = url
        self.reference['reference_time_modification'] = today.datetime.replace(hour=0,
                                                                               minute=0,
                                                                               second=0,
                                                                               microsecond=0,
                                                                               tzinfo=timezone.utc)

        os.makedirs('tmp/', exist_ok=True)
        os.system(f'wget {url} -P tmp/')

        local_filename = 'tmp/' + url.rpartition('/')[2]
        self.csv = lz4Csv(local_filename)

        logging.info('Getting node IDs from neo4j...')
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn')
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix')
        tag_id = self.iyp.batch_get_nodes_by_single_prop('Tag', 'label')
        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code')

        orig_links = []
        tag_links = []
        dep_links = []
        country_links = []

        logging.info('Computing links...')
        for line in csv.reader(self.csv, quotechar='"', delimiter=',', skipinitialspace=True):
            # header
            # id, timebin, prefix, hege, af, visibility, rpki_status, irr_status,
            # delegated_prefix_status, delegated_asn_status, descr, moas, asn_id,
            # country_id, originasn_id

            rec = dict(zip(self.csv.fields, line))
            rec['hege'] = float(rec['hege'])
            rec['visibility'] = float(rec['visibility'])
            rec['af'] = int(rec['af'])

            try:
                prefix = ip_network(rec['prefix']).compressed
            except ValueError as e:
                logging.warning(f'Ignoring malformed prefix: "{rec["prefix"]}": {e}')
                continue

            if prefix not in prefix_id:
                prefix_id[prefix] = self.iyp.get_node('Prefix', {'prefix': prefix})

            # make status/country/origin links only for lines where asn=originasn
            if rec['asn_id'] == rec['originasn_id']:
                # Make sure all nodes exist
                originasn = int(rec['originasn_id'])
                if originasn not in asn_id:
                    asn_id[originasn] = self.iyp.get_node('AS', {'asn': originasn})

                rpki_status = 'RPKI ' + rec['rpki_status']
                if rpki_status not in tag_id:
                    tag_id[rpki_status] = self.iyp.get_node('Tag', {'label': rpki_status})

                irr_status = 'IRR ' + rec['irr_status']
                if irr_status not in tag_id:
                    tag_id[irr_status] = self.iyp.get_node('Tag', {'label': irr_status})

                cc = rec['country_id']
                if cc not in country_id:
                    country_id[cc] = self.iyp.get_node('Country', {'country_code': cc})

                # Compute links
                orig_links.append({
                    'src_id': asn_id[originasn],
                    'dst_id': prefix_id[prefix],
                    'props': [self.reference, rec]
                })

                tag_links.append({
                    'src_id': prefix_id[prefix],
                    'dst_id': tag_id[rpki_status],
                    'props': [self.reference, rec]
                })

                tag_links.append({
                    'src_id': prefix_id[prefix],
                    'dst_id': tag_id[irr_status],
                    'props': [self.reference, rec]
                })

                country_links.append({
                    'src_id': prefix_id[prefix],
                    'dst_id': country_id[cc],
                    'props': [self.reference]
                })

            # Dependency links
            asn = int(rec['asn_id'])
            if asn not in asn_id:
                asn_id[asn] = self.iyp.get_node('AS', {'asn': asn})

            dep_links.append({
                'src_id': prefix_id[prefix],
                'dst_id': asn_id[asn],
                'props': [self.reference, rec]
            })

        self.csv.close()

        # Push links to IYP
        logging.info('Pushing links to neo4j...')
        self.iyp.batch_add_links('ORIGINATE', orig_links)
        self.iyp.batch_add_links('CATEGORIZED', tag_links)
        self.iyp.batch_add_links('DEPENDS_ON', dep_links)
        self.iyp.batch_add_links('COUNTRY', country_links)

        # Remove downloaded file
        os.remove(local_filename)

    def unit_test(self):
        super().unit_test(logging, ['ORIGINATE', 'CATEGORIZED', 'DEPENDS_ON', 'COUNTRY'])


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

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, time, timezone

import arrow
import lz4.frame
import requests

from iyp import BaseCrawler

# URL to the API
URL = 'https://ihr-archive.iijlab.net/ihr/hegemony/ipv4/local/{year}/{month:02d}/{day:02d}/ihr_hegemony_ipv4_local_{year}-{month:02d}-{day:02d}.csv.lz4'  # noqa: E501
ORG = 'IHR'
NAME = 'ihr.local_hegemony'


class lz4Csv:
    def __init__(self, filename):
        """Start reading a lz4 compress csv file."""

        self.fp = lz4.frame.open(filename, 'rb')

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


class Crawler(BaseCrawler):

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
                req = requests.head(url)

        self.reference = {
            'reference_url': url,
            'reference_org': ORG,
            'reference_name': NAME,
            'reference_time': datetime.combine(today.date(), time.min, timezone.utc)
        }

        os.makedirs('tmp/', exist_ok=True)
        os.system(f'wget {url} -P tmp/')

        local_filename = 'tmp/' + url.rpartition('/')[2]
        self.csv = lz4Csv(local_filename)

        self.timebin = None
        asn_id = self.iyp.batch_get_nodes('AS', 'asn', set())

        links = []

        for line in csv.reader(self.csv, quotechar='"', delimiter=',', skipinitialspace=True):
            # header
            # timebin,originasn,asn,hege

            rec = dict(zip(self.csv.fields, line))
            rec['hege'] = float(rec['hege'])

            if self.timebin is None:
                self.timebin = rec['timebin']
            elif self.timebin != rec['timebin']:
                break

            originasn = int(rec['originasn'])
            if originasn not in asn_id:
                asn_id[originasn] = self.iyp.get_node('AS', {'asn': originasn}, create=True)

            asn = int(rec['asn'])
            if asn not in asn_id:
                asn_id[asn] = self.iyp.get_node('AS', {'asn': asn}, create=True)

            links.append({
                'src_id': asn_id[originasn],
                'dst_id': asn_id[asn],
                'props': [self.reference, rec]
            })

        # Push links to IYP
        self.iyp.batch_add_links('DEPENDS_ON', links)

        # Remove downloaded file
        os.remove(local_filename)


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

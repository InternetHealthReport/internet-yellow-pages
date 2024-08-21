import csv
import os
from datetime import timezone

import arrow
import lz4.frame
import requests

from iyp import BaseCrawler


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


class HegemonyCrawler(BaseCrawler):
    def __init__(self, organization, url, name, af):
        self.af = af
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://ihr.iijlab.net/ihr/en-us/documentation#AS_dependency'

    def run(self):
        """Fetch data from file and push to IYP."""

        today = arrow.utcnow()
        url = self.url.format(year=today.year, month=today.month, day=today.day)
        req = requests.head(url)
        if req.status_code != 200:
            today = today.shift(days=-1)
            url = self.url.format(year=today.year, month=today.month, day=today.day)
            req = requests.head(url)
            if req.status_code != 200:
                today = today.shift(days=-1)
                url = self.url.format(year=today.year, month=today.month, day=today.day)
                req = requests.head(url)

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

        self.timebin = None
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', set())

        links = []

        for line in csv.reader(self.csv, quotechar='"', delimiter=',', skipinitialspace=True):
            # header
            # timebin,originasn,asn,hege

            rec = dict(zip(self.csv.fields, line))
            rec['hege'] = float(rec['hege'])
            rec['af'] = self.af

            if self.timebin is None:
                self.timebin = rec['timebin']
            elif self.timebin != rec['timebin']:
                break

            originasn = int(rec['originasn'])
            if originasn not in asn_id:
                asn_id[originasn] = self.iyp.get_node('AS', {'asn': originasn})

            asn = int(rec['asn'])
            if asn not in asn_id:
                asn_id[asn] = self.iyp.get_node('AS', {'asn': asn})

            links.append({
                'src_id': asn_id[originasn],
                'dst_id': asn_id[asn],
                'props': [self.reference, rec]
            })

        # Push links to IYP
        self.iyp.batch_add_links('DEPENDS_ON', links)

        # Remove downloaded file
        os.remove(local_filename)

    def unit_test(self):
        super().unit_test(['DEPENDS_ON'])

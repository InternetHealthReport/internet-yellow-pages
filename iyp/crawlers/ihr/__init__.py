import csv
import io
import logging
from datetime import datetime, timedelta, timezone

import lz4.frame
import requests

from iyp import BaseCrawler, DataNotAvailableError


class HegemonyCrawler(BaseCrawler):
    def __init__(self, organization, url, name, af):
        self.af = af
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://ihr.iijlab.net/ihr/en-us/documentation#AS_dependency'

    def run(self):
        """Fetch data from file and push to IYP."""

        today = datetime.now(tz=timezone.utc)
        max_lookback = today - timedelta(days=7)
        url = today.strftime(self.url)
        req = requests.head(url)
        while req.status_code != 200 and today > max_lookback:
            today -= timedelta(days=1)
            url = today.strftime(self.url)
            req = requests.head(url)
        if req.status_code != 200:
            logging.error('Failed to find data within the specified lookback interval.')
            raise DataNotAvailableError('Failed to find data within the specified lookback interval.')

        self.reference['reference_url_data'] = url

        logging.info(f'Fetching data from: {url}')
        req = requests.get(url)
        req.raise_for_status()

        # lz4.frame.decompress() and splitlines() break the CSV parsing due to some
        # weird input.
        with lz4.frame.open(io.BytesIO(req.content)) as f:
            csv_lines = [l.decode('utf-8').rstrip() for l in f]

        timebin = None
        asns = set()
        links = list()

        logging.info('Computing links...')
        for rec in csv.DictReader(csv_lines):
            # header
            # timebin,originasn,asn,hege

            rec['hege'] = float(rec['hege'])
            rec['af'] = self.af

            if timebin is None:
                timebin = rec['timebin']
                mod_time = datetime.strptime(timebin, '%Y-%m-%d %H:%M:%S+00').replace(tzinfo=timezone.utc)
                self.reference['reference_time_modification'] = mod_time
            elif timebin != rec['timebin']:
                break

            originasn = int(rec['originasn'])
            asn = int(rec['asn'])
            asns.add(originasn)
            asns.add(asn)

            links.append({
                'src_id': originasn,
                'dst_id': asn,
                'props': [self.reference, rec]
            })

        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        # Replace values in links with node IDs.
        for link in links:
            link['src_id'] = asn_id[link['src_id']]
            link['dst_id'] = asn_id[link['dst_id']]

        # Push links to IYP
        self.iyp.batch_add_links('DEPENDS_ON', links)

    def unit_test(self):
        return super().unit_test(['DEPENDS_ON'])

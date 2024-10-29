import bz2
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import requests
from bs4 import BeautifulSoup

from iyp import BaseCrawler


class ASRelCrawler(BaseCrawler):
    def __init__(self, organization, url, name, af):
        super().__init__(organization, url, name)
        self.af = af
        self.reference['reference_url_info'] = \
            'https://publicdata.caida.org/datasets/as-relationships/serial-1/README.txt'

    def __get_latest_file(self):
        index = requests.get(self.reference['reference_url_data'])
        index.raise_for_status()
        soup = BeautifulSoup(index.text, features='html.parser')
        if self.af == 4:
            filename_template = '%Y%m%d.as-rel.txt.bz2'
        else:
            filename_template = '%Y%m%d.as-rel.v6-stable.txt.bz2'
        links = soup.find_all('a')
        file_dates = list()
        for link in links:
            try:
                dt = datetime.strptime(link['href'], filename_template).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            file_dates.append((dt, link['href']))
        file_dates.sort()
        latest_file_date, latest_file_name = file_dates[-1]
        self.reference['reference_time_modification'] = latest_file_date
        self.reference['reference_url_data'] = os.path.join(self.reference['reference_url_data'], latest_file_name)
        logging.info(f'Fetching file: {self.reference["reference_url_data"]}')

    def run(self):
        self.__get_latest_file()
        req = requests.get(self.reference['reference_url_data'])
        req.raise_for_status()

        with bz2.open(BytesIO(req.content), 'rb') as f:
            text = f.read().decode()

        ases = set()
        peers_with_links = list()
        for line in text.splitlines():
            if line.startswith('#'):
                continue
            left_asn, right_asn, kind = map(int, line.split('|'))
            ases.add(left_asn)
            ases.add(right_asn)
            peers_with_links.append({'src_id': left_asn, 'dst_id': right_asn,
                                     'props': [self.reference, {'rel': kind, 'af': self.af}]})

        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', ases)

        for link in peers_with_links:
            link['src_id'] = as_id[link['src_id']]
            link['dst_id'] = as_id[link['dst_id']]

        self.iyp.batch_add_links('PEERS_WITH', peers_with_links)

    def unit_test(self):
        return super().unit_test(['PEERS_WITH'])

import argparse
import io
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from zipfile import ZipFile

import requests
import tldextract

from iyp import BaseCrawler, RequestStatusError

# URL to umbrella top 1M
URL = 'http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip'
ORG = 'Cisco'
NAME = 'cisco.umbrella_top1m'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://s3-us-west-1.amazonaws.com/umbrella-static/index.html'

    def __set_modification_time(self):
        """Set the modification time by looking for the last available historical file.
        The current (non-historical) file is created on the next day.

        For example, if a file for 2024-02-13 is available, it means the current file
        was created on 2024-02-14.
        """
        hist_url = 'http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-%Y-%m-%d.csv.zip'
        date = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        for attempt in range(7):
            r = requests.head(date.strftime(hist_url))
            if r.ok:
                break
            date -= timedelta(days=1)
        else:
            logging.warning(f'Failed to find historical list within search interval (>{date}); '
                            'Will not set modification time.')
            return

        # date now points to the last available historical file , which means the
        # current file is the day after this date.
        self.reference['reference_time_modification'] = date + timedelta(days=1)
        logging.info(f'Got list for date {self.reference["reference_time_modification"].strftime("%Y-%m-%d")}')

    def run(self):
        """Fetch Umbrella top 1M and push to IYP."""

        self.cisco_qid = self.iyp.get_node('Ranking', {'name': 'Cisco Umbrella Top 1 million'})

        logging.info('Downloading latest list...')
        req = requests.get(URL)
        if req.status_code != 200:
            raise RequestStatusError(f'Error while fetching Cisco Umbrella Top 1M csv file: {req.status_code}')

        self.__set_modification_time()

        links = []
        # open zip file and read top list
        with ZipFile(io.BytesIO(req.content)) as z:
            with z.open('top-1m.csv') as top_list:
                for i, row in enumerate(io.TextIOWrapper(top_list)):
                    row = row.rstrip()
                    rank, domain = row.split(',')

                    links.append({'src_name': domain, 'dst_id': self.cisco_qid,
                                  'props': [self.reference, {'rank': int(rank)}]})

        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name')
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name')

        # Umbrella mixes up domain and host names.
        # By order of preferences we rank:
        # 1) existing domain name
        # 2) existing host name
        # 3) do our best to figure out if it is a domain or host and create the
        # corresponding node

        new_domain_names = set()
        new_host_names = set()
        unprocessed_links = list()
        processed_links = list()

        logging.info('Building relationships...')
        for link in links:
            if link['src_name'] in domain_id:
                link['src_id'] = domain_id[link['src_name']]
                processed_links.append(link)
            elif link['src_name'] in host_id:
                link['src_id'] = host_id[link['src_name']]
                processed_links.append(link)
            else:
                unprocessed_links.append(link)
                ranked_thing = tldextract.extract(link['src_name'])
                name = link['src_name']
                if name == ranked_thing.registered_domain:
                    new_domain_names.add(name)
                else:
                    new_host_names.add(name)

        if new_domain_names:
            domain_id.update(self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', new_domain_names, all=False))
        if new_host_names:
            host_id.update(self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', new_host_names, all=False))

        for link in unprocessed_links:
            if link['src_name'] in domain_id:
                link['src_id'] = domain_id[link['src_name']]
            elif link['src_name'] in host_id:
                link['src_id'] = host_id[link['src_name']]
            else:
                logging.error(f'Missing DomainName/HostName node for name "{link["src_name"]}". Should not happen.')
                continue
            processed_links.append(link)

        # Push all links to IYP
        self.iyp.batch_add_links('RANK', processed_links)

    def unit_test(self):
        return super().unit_test(['RANK'])


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
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

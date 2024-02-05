import argparse
import io
import logging
import os
import sys
from zipfile import ZipFile

import requests
import tldextract

from iyp import BaseCrawler, RequestStatusError

# URL to Tranco top 1M
URL = 'http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip'
ORG = 'Cisco'
NAME = 'cisco.umbrella_top1M'


class Crawler(BaseCrawler):

    def run(self):
        """Fetch Umbrella top 1M and push to IYP."""

        self.cisco_qid = self.iyp.get_node('Ranking', {'name': 'Cisco Umbrella Top 1 million'})

        logging.info('Downloading latest list...')
        req = requests.get(URL)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching Cisco Umbrella Top 1M csv file')

        links = []
        domains = set()
        # open zip file and read top list
        with ZipFile(io.BytesIO(req.content)) as z:
            with z.open('top-1m.csv') as top_list:
                for i, row in enumerate(io.TextIOWrapper(top_list)):
                    row = row.rstrip()
                    rank, domain = row.split(',')

                    domains.add(domain)
                    links.append({'src_name': domain, 'dst_id': self.cisco_qid,
                                  'props': [self.reference, {'rank': int(rank)}]})

        logging.info('Fetching DomainName/HostName nodes...')
        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', domains, create=False)
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', domains, create=False)

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
            logging.info(f'Pushing {len(new_domain_names)} additional DomainName nodes...')
            domain_id.update(self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', new_domain_names, all=False))
        if new_host_names:
            logging.info(f'Pushing {len(new_host_names)} additional HostName nodes...')
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
        logging.info(f'Pushing {len(processed_links)} RANK relationships...')
        self.iyp.batch_add_links('RANK', processed_links)


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

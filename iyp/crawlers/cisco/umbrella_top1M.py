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

        sys.stderr.write('Downloading latest list...\n')
        req = requests.get(URL)
        if req.status_code != 200:
            raise RequestStatusError('Error while fetching Cisco Umbrella Top 1M csv file')

        links = []
        domains = set()
        # open zip file and read top list
        with ZipFile(io.BytesIO(req.content)) as z:
            with z.open('top-1m.csv') as list:
                for i, row in enumerate(io.TextIOWrapper(list)):
                    row = row.rstrip()
                    rank, domain = row.split(',')

                    domains.add(domain)
                    links.append({'src_name': domain, 'dst_id': self.cisco_qid,
                                  'props': [self.reference, {'rank': int(rank)}]})

        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', domains, create=False)
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', domains, create=False)

        # Umbrella mixes up domain and host names.
        # By order of preferences we rank:
        # 1) existing domain name
        # 2) existing host name
        # 3) do our best to figure out if it is a domain or host and create the
        # corresponding node

        for link in links:
            if link['src_name'] in domain_id:
                link['src_id'] = domain_id[link['src_name']]
            elif link['src_name'] in host_id:
                link['src_id'] = host_id[link['src_name']]
            else:
                # Create new nodes (should be rare as openintel should already
                # have created these nodes)
                ranked_thing = tldextract.extract(link['src_name'])
                prop = {'name': link['src_name']}
                if link['src_name'] == ranked_thing.registered_domain:
                    node_id = self.iyp.get_node('DomainName', prop)
                    link['src_id'] = node_id
                    created_node_label = 'DomainName'
                else:
                    node_id = self.iyp.get_node('HostName', prop)
                    link['src_id'] = node_id
                    created_node_label = 'HostName'
                logging.info(f'New {created_node_label} node created for name "{prop["name"]}"')

        # Push all links to IYP
        self.iyp.batch_add_links('RANK', links)


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

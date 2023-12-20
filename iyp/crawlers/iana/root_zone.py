import argparse
import ipaddress
import logging
import os
import sys

import requests

from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'IANA'
URL = 'https://www.internic.net/domain/root.zone'
NAME = 'iana.root_zone'  # should reflect the directory and name of this file


class Crawler(BaseCrawler):

    def run(self):
        r = requests.get(self.url)
        r.raise_for_status()

        lines = [line.split() for line in r.text.splitlines()]

        domainnames = set()
        ips = set()
        authoritativenameservers = set()
        resolves_to = set()
        managed_by = set()

        for l in lines:
            if not l:
                continue
            # Each line should have at least five fields:
            # NAME, TTL, CLASS, TYPE, RDATA
            if len(l) < 5:
                logging.warning(f'DNS record line is too short: {l}')
                print(f'DNS record line is too short: {l}', file=sys.stderr)
                continue
            if l[2] != 'IN':
                logging.warning(f'Unexpected DNS record class: "{l[2]}". Expecting only IN records.')
                print(f'Unexpected DNS record class: "{l[2]}". Expecting only IN records.', file=sys.stderr)
                continue
            record_type = l[3]
            if record_type not in {'A', 'AAAA', 'NS'}:
                continue
            name = l[0].rstrip('.')
            # We do not have a node for the DNS root ".".
            if not name:
                continue
            rdata = l[4]
            if record_type == 'NS':
                # Name server, value has to be a domain name.
                nsdname = rdata.rstrip('.')
                if not nsdname:
                    logging.warning(f'NS record points to root node? {l}')
                    print(f'NS record points to root node? {l}', file=sys.stderr)
                    continue
                if nsdname not in domainnames:
                    domainnames.add(nsdname)
                if nsdname not in authoritativenameservers:
                    authoritativenameservers.add(nsdname)
                managed_by.add((name, nsdname))
            else:
                # A or AAAA record, value has to be an IP address.
                try:
                    # This is useful for IPv6 addresses, since the root zone file does
                    # not do zero compression, for example:
                    #   a.nic.aaa. 172800 IN AAAA 2001:dcd:1:0:0:0:0:9
                    # should be 2001:dcd:1::9 instead.
                    ip = ipaddress.ip_address(rdata).compressed
                except ValueError as e:
                    logging.warning(f'Invalid IP address in A/AAAA record: {l}')
                    logging.warning(e)
                    print(f'Invalid IP address in A/AAAA record: {l}', file=sys.stderr)
                    print(e, file=sys.stderr)
                    continue
                if ip not in ips:
                    ips.add(ip)
                resolves_to.add((name, ip))
            # Only add now so that in case there is an error in the if/else above, we do
            # not create a dangling node.
            if name not in domainnames:
                domainnames.add(name)

        logging.info(f'Fetching/Creating {len(domainnames)} DomainName nodes')
        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', domainnames, all=False)
        logging.info(f'Fetching/Creating {len(ips)} IP nodes')
        ip_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', ips, all=False)
        authoritativenameservers_id = {name: domain_id[name] for name in authoritativenameservers}
        logging.info(f'Adding AuthoritativeNameServer label to {len(authoritativenameservers_id)} DomainName nodes.')
        self.iyp.batch_add_node_label(list(authoritativenameservers_id.values()), 'AuthoritativeNameServer')

        logging.info('Computing relationships')
        resolves_to_relationships = list()
        for (name, ip) in resolves_to:
            resolves_to_relationships.append({'src_id': domain_id[name],
                                              'dst_id': ip_id[ip],
                                              'props': [self.reference]})
        managed_by_relationships = list()
        for (name, nsdname) in managed_by:
            managed_by_relationships.append({'src_id': domain_id[name],
                                             'dst_id': authoritativenameservers_id[nsdname],
                                             'props': [self.reference]})
        logging.info(f'Pushing {len(resolves_to_relationships)} RESOLVES_TO relationships.')
        self.iyp.batch_add_links('RESOLVES_TO', resolves_to_relationships)
        logging.info(f'Pushing {len(managed_by)} MANAGED_BY relationships.')
        self.iyp.batch_add_links('MANAGED_BY', managed_by_relationships)


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

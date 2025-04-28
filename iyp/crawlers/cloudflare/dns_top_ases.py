import argparse
import logging
import sys

import flatdict

from iyp.crawlers.cloudflare import DnsTopCrawler

ORG = 'Cloudflare'
URL = 'https://api.cloudflare.com/client/v4/radar/dns/top/ases/'
NAME = 'cloudflare.dns_top_ases'


class Crawler(DnsTopCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)

        self.reference['reference_url_info'] = 'https://developers.cloudflare.com/api/operations/radar-get-dns-top-ases'

    def compute_link(self, param):

        name, ases = param

        # 'meta' result it not a domain, but contains metadata so skip.
        if name == 'meta':
            return

        qids = list()
        if name in self.domain_names_id:
            qids.append(self.domain_names_id[name])
        if name in self.host_names_id:
            qids.append(self.host_names_id[name])

        for entry in ases:
            if not entry:
                continue

            asn = entry['clientASN']
            self.to_nodes.add(asn)

            entry['value'] = float(entry['value'])

            flat_prop = dict(flatdict.FlatDict(entry))
            for qid in qids:
                self.links.append({
                    'src_id': qid,
                    'dst_id': asn,
                    'props': [flat_prop, self.reference]
                })

    def map_links(self):
        as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', self.to_nodes, all=False)
        for link in self.links:
            link['dst_id'] = as_id[link['dst_id']]


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

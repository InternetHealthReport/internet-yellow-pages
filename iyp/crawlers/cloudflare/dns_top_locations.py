import argparse
import logging
import sys

import flatdict

from iyp.crawlers.cloudflare import DnsTopCrawler

ORG = 'Cloudflare'
URL = 'https://api.cloudflare.com/client/v4/radar/dns/top/locations/'
NAME = 'cloudflare.dns_top_locations'


class Crawler(DnsTopCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)

        self.reference['reference_url_info'] = 'https://developers.cloudflare.com/radar/investigate/dns/#top-locations'

    def compute_link(self, param):

        domain, countries = param

        # 'meta' result it not a domain, but contains metadata so skip.
        if domain == 'meta':
            return

        domain_qid = self.domain_names_id[domain]

        for entry in countries:
            if not entry:
                continue

            cc = entry['clientCountryAlpha2']
            self.to_nodes.add(cc)

            entry['value'] = float(entry['value'])

            flat_prop = dict(flatdict.FlatDict(entry))
            self.links.append({
                'src_id': domain_qid,
                'dst_id': cc,
                'props': [flat_prop, self.reference]
            })

    def map_links(self):
        cc_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code', self.to_nodes, all=False)
        for link in self.links:
            link['dst_id'] = cc_id[link['dst_id']]


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

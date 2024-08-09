# Cloudflare radar's top location and ASes is available for both domain names
# and host names. Results are likely accounting for all NS, A, AAAA queries made to
# Cloudflare's resolver. Since NS queries for host names make no sense it seems
# more intuitive to link these results to DomainName nodes.

import argparse
import logging
import os
import sys

import flatdict

from iyp.crawlers.cloudflare.dns_top_locations import Crawler

# Organization name and URL to data
ORG = 'Cloudflare'
URL = 'https://api.cloudflare.com/client/v4/radar/dns/top/ases/'
NAME = 'cloudflare.dns_top_ases'


class Crawler(Crawler):

    def run(self):
        """Push data to IYP."""

        self.as_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn')

        super().run()

    def compute_link(self, param):
        """Compute link for the domain name' top ases and corresponding properties."""

        domain, ases = param

        if domain == 'meta' or domain not in self.domain_names_id:
            return

        for entry in ases:
            asn = entry['clientASN']

            # set link
            entry['value'] = float(entry['value'])
            flat_prop = dict(flatdict.FlatDict(entry))
            self.statements.append({
                'src_id': self.domain_names_id[domain],
                'dst_id': self.as_id[asn],
                'props': [flat_prop, self.reference]
            })

    # already defined in imported Crawler
    # def unit_test(self):
    #     pass


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

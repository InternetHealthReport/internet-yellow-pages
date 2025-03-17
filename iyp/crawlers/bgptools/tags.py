import argparse
import logging
import sys
from datetime import datetime, time, timezone

import requests

from iyp import BaseCrawler

# curl -s https://bgp.tools/asns.csv | head -n 5
URL = 'https://bgp.tools/tags/'
ORG = 'BGP.Tools'
NAME = 'bgptools.tags'

TAGS = {
    'cdn': 'Content Delivery Network',
    'dsl': 'Home ISP',
    'a10k': 'Tranco 10k Host',
    'icrit': 'Internet Critical Infra',
    'tor': 'ToR Services',
    'anycast': 'Anycast',
    'perso': 'Personal ASN',
    'ddosm': 'DDoS Mitigation',
    'vpn': 'VPN Host',
    'vpsh': 'Server Hosting',
    'uni': 'Academic',
    'gov': 'Government',
    'event': 'Event',
    'mobile': 'Mobile Data/Carrier',
    'satnet': 'Satellite Internet',
    'biznet': 'Business Broadband',
    'corp': 'Corporate/Enterprise',
    'rpkirov': 'Validating RPKI ROV'
}


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://bgp.tools/kb/api'

        self.headers = {
            'user-agent': 'IIJ/Internet Health Report - admin@ihr.live'
        }

    def run(self):
        """Fetch the AS name file from BGP.Tools website and process lines one by
        one."""

        for tag, label in TAGS.items():
            url = URL + tag + '.csv'
            # Reference information for data pushed to the wikibase
            self.reference = {
                'reference_org': ORG,
                'reference_url_data': url,
                'reference_name': NAME,
                'reference_time_fetch': datetime.combine(datetime.utcnow(), time.min, timezone.utc)
            }

            req = requests.get(url, headers=self.headers)
            req.raise_for_status()

            self.tag_qid = self.iyp.get_node('Tag', {'label': label})
            for line in req.text.splitlines():
                # skip header
                if line.startswith('asn'):
                    continue

                # Parse given line to get ASN, name, and country code
                asn, _, _ = line.partition(',')
                asn_qid = self.iyp.get_node('AS', {'asn': asn[2:]})
                statements = [['CATEGORIZED', self.tag_qid, self.reference]]  # Set AS name

                # Update AS name and country
                self.iyp.add_links(asn_qid, statements)

    def unit_test(self):
        return super().unit_test(['CATEGORIZED'])


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

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests

from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'Cloudflare'
URL = 'https://api.cloudflare.com/client/v4/radar/ranking/top?name=top&limit=100&format=json'
NAME = 'cloudflare.top100'

API_KEY = ''
if os.path.exists('config.json'):
    API_KEY = json.load(open('config.json', 'r'))['cloudflare']['apikey']


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference
    #
    # Cloudflare ranks second and third level domain names (not host names).
    # See https://blog.cloudflare.com/radar-domain-rankings/
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://developers.cloudflare.com/radar/investigate/domain-ranking-datasets/'  # noqa: E501

    def run(self):
        """Fetch data and push to IYP."""

        self.cf_qid = self.iyp.get_node(
            'Ranking', {'name': 'Cloudflare top 100 domains'})

        # Fetch data
        headers = {
            'Authorization': 'Bearer ' + API_KEY,
            'Content-Type': 'application/json'
        }

        req = requests.get(self.reference['reference_url_data'], headers=headers)
        req.raise_for_status()

        results = req.json()['result']

        try:
            date_str = results['meta']['dateRange'][0]['endTime']
            date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            self.reference['reference_time_modification'] = date
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f'Failed to get modification time: {e}')

        # Process line one after the other
        processed = list(map(self.update, results['top']))
        logging.info(f'Processed {len(processed)} lines')

    def update(self, entry):
        """Add the entry to IYP if it's not already there and update its properties."""

        # set rank
        statements = [['RANK', self.cf_qid, dict({'rank': entry['rank']}, **self.reference)]]

        # Commit to IYP
        # Get the AS's node ID (create if it is not yet registered) and commit changes
        domain_qid = self.iyp.get_node('DomainName', {'name': entry['domain']})
        self.iyp.add_links(domain_qid, statements)

    def unit_test(self):
        return super().unit_test(['RANK'])


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

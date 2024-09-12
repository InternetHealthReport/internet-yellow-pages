# Cloudflare radar's top location and ASes is available for both domain names
# and host names. Results are likely accounting for all NS, A, AAAA queries made to
# Cloudflare's resolver. Since NS queries for host names make no sense it seems
# more intuitive to link these results to DomainName nodes.

import argparse
import glob
import json
import logging
import os
import sys
from datetime import datetime, timezone

import flatdict
import requests
from requests.adapters import HTTPAdapter, MaxRetryError, Retry

from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'Cloudflare'
URL = 'https://api.cloudflare.com/client/v4/radar/dns/top/locations/'
NAME = 'cloudflare.dns_top_locations'
BATCH_SIZE = 1
RANK_THRESHOLD = 10000
TOP_LIMIT = 100 / BATCH_SIZE

# API credentials
USER_ID = ''
AUTH_EMAIL = ''
AUTH_KEY = ''
if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    API_KEY = config['cloudflare']['apikey']


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference

    def __init__(self, organization, url, name):

        # Initialize IYP connection
        super().__init__(organization, url, name)

        # Not super elegant.
        if name == 'cloudflare.dns_top_ases':
            self.reference['reference_url_info'] = 'https://developers.cloudflare.com/api/operations/radar-get-dns-top-ases'  # noqa: E501
        elif name == 'cloudflare.dns_top_locations':
            self.reference['reference_url_info'] = 'https://developers.cloudflare.com/radar/investigate/dns/#top-locations'  # noqa: E501

        # Fetch domain names registered in IYP
        existing_dn = self.iyp.tx.run(
            f"""MATCH (dn:DomainName)-[r:RANK]-(:Ranking)
                WHERE r.rank < {RANK_THRESHOLD}
                RETURN ID(dn) AS _id, dn.name AS dname;""")

        self.domain_names_id = {node['dname']: node['_id'] for node in existing_dn}
        self.domain_names = list(self.domain_names_id.keys())

    def fetch(self):
        """Download top locations for top RANK_THRESHOLD domain names registered in IYP
        and save it on disk."""

        # setup HTTPS session with credentials and retry
        req_session = requests.Session()
        req_session.headers['Authorization'] = 'Bearer ' + API_KEY
        req_session.headers['Content-Type'] = 'application/json'

        retries = Retry(total=10,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        req_session.mount('http://', HTTPAdapter(max_retries=retries))
        req_session.mount('https://', HTTPAdapter(max_retries=retries))

        # Clear the cache
        tmp_dir = self.create_tmp_dir()

        # Query Cloudflare API in batches
        for i in range(0, len(self.domain_names), BATCH_SIZE):

            # Don't overide existing files
            fname = 'data_' + '_'.join(self.domain_names[i:i + BATCH_SIZE])
            fpath = f'{tmp_dir}/{fname}.json'

            if os.path.exists(fpath):
                continue

            get_params = f'?limit={TOP_LIMIT}'
            for domain in self.domain_names[i:i + BATCH_SIZE]:
                get_params += f'&dateRange=7d&domain={domain}&name={domain}'

            url = self.url + get_params

            # Fetch data
            try:
                req = req_session.get(url)
                if req.status_code != 200:
                    logging.error(f'Cannot download data for {get_params}, status code {req.status_code}: {req.text}')
                    # Cannot get the data? skip this one
                    continue
                    # sys.exit('Error while fetching data file')

                with open(fpath, 'wb') as fp:
                    fp.write(req.content)

            except MaxRetryError as e:
                logging.error(f'Max retry error for {get_params}; {e}')
                continue

    def run(self):
        """Push data to IYP."""

        # FIXME this should be called before/separately
        self.fetch()

        self.country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code')
        self.statements = []

        tmp_dir = self.get_tmp_dir()

        files = glob.glob(f'{tmp_dir}/data_*.json')
        for i, file in enumerate(files):
            with open(file, 'rb') as fp:
                # Process line one after the other
                results = json.load(fp)['result']
                if not self.reference['reference_time_modification']:
                    # Get the reference time from the first file.
                    try:
                        date_str = results['meta']['dateRange'][0]['endTime']
                        date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                        self.reference['reference_time_modification'] = date
                    except (KeyError, ValueError, TypeError) as e:
                        logging.warning(f'Failed to get modification time: {e}')

                for domain_top in results.items():
                    self.compute_link(domain_top)

            if i % 100 == 0:
                self.iyp.batch_add_links('QUERIED_FROM', self.statements)
                self.statements = []

        if self.statements:
            self.iyp.batch_add_links('QUERIED_FROM', self.statements)

    def compute_link(self, param):
        """Compute link for the given domain name' top countries and corresponding
        properties."""

        domain, countries = param

        if domain == 'meta' or domain not in self.domain_names_id:
            return

        for entry in countries:
            cc = entry['clientCountryAlpha2']

            # set link
            entry['value'] = float(entry['value'])
            flat_prop = dict(flatdict.FlatDict(entry))
            self.statements.append({
                'src_id': self.domain_names_id[domain],
                'dst_id': self.country_id[cc],
                'props': [flat_prop, self.reference]
            })

    def unit_test(self):
        return super().unit_test(['QUERIED_FROM'])


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

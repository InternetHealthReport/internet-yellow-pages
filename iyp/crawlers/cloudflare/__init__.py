import json
import logging
import os
from concurrent.futures import as_completed
from datetime import datetime, timezone

from requests.adapters import HTTPAdapter, Retry
from requests_futures.sessions import FuturesSession

from iyp import BaseCrawler

BATCH_SIZE = 10
RANK_THRESHOLD = 10000
TOP_LIMIT = 100
PARALLEL_DOWNLOADS = 4

API_KEY = str()
if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    API_KEY = config['cloudflare']['apikey']

# Cloudflare radar's top location and ASes is available for both domain names
# and host names. Results are likely accounting for all NS, A, AAAA queries made to
# Cloudflare's resolver. Since NS queries for host names make no sense it seems
# more intuitive to link these results to DomainName nodes.


class DnsTopCrawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)

        # Fetch domain names registered in IYP
        existing_dn = self.iyp.tx.run(
            """MATCH (dn:DomainName)-[r:RANK]-(:Ranking)
                WHERE r.rank <= $rank_threshold
                RETURN elementId(dn) AS _id, dn.name AS dname""",
            rank_threshold=RANK_THRESHOLD)

        self.domain_names_id = {node['dname']: node['_id'] for node in existing_dn}
        self.domain_names = list(sorted(self.domain_names_id.keys()))
        # Contains unique values that connect to the domains, depending on the crawler.
        # ASNs for top_ases, country codes for top_locations.
        self.to_nodes = set()
        self.links = list()

    def fetch(self):
        """Download data for top RANK_THRESHOLD domain names registered in IYP and save
        it on disk."""

        req_session = FuturesSession(max_workers=PARALLEL_DOWNLOADS)
        # Set API authentication headers.
        req_session.headers['Authorization'] = 'Bearer ' + API_KEY
        req_session.headers['Content-Type'] = 'application/json'

        retries = Retry(total=10,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        req_session.mount('https://', HTTPAdapter(max_retries=retries))

        # Clear the cache.
        tmp_dir = self.create_tmp_dir()

        queries = list()

        # Query Cloudflare API in batches.
        for i in range(0, len(self.domain_names), BATCH_SIZE):

            batch_domains = self.domain_names[i: i + BATCH_SIZE]

            # Do not override existing files.
            fname = 'data_' + '_'.join(batch_domains) + '.json'
            fpath = os.path.join(tmp_dir, fname)

            if os.path.exists(fpath):
                continue

            get_params = f'?limit={TOP_LIMIT}'
            for domain in batch_domains:
                get_params += f'&dateRange=7d&domain={domain}&name={domain}'

            url = self.url + get_params
            future = req_session.get(url)
            future.domains = batch_domains
            future.fpath = fpath
            queries.append(future)

        for query in as_completed(queries):
            try:
                res = query.result()
                res.raise_for_status()
                # Confirm JSON integrity.
                data = res.json()
                if not data['success']:
                    raise ValueError('Response contains success=False')

                with open(query.fpath, 'wb') as fp:
                    fp.write(res.content)

            except Exception as e:
                logging.error(f'Failed to fetch data for domains: {query.domains}: {e}')
                continue

    def run(self):
        self.fetch()

        tmp_dir = self.get_tmp_dir()

        for entry in os.scandir(tmp_dir):
            if not entry.is_file() or not entry.name.endswith('.json'):
                continue
            file = os.path.join(tmp_dir, entry.name)

            with open(file, 'rb') as fp:
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

        self.map_links()

        self.iyp.batch_add_links('QUERIED_FROM', self.links)

    def compute_link(self, param):
        """Create link entries for result."""
        raise NotImplementedError()

    def map_links(self):
        """Fetch/create destination nodes of links and replace link destination with
        QID."""
        raise NotImplementedError()

    def unit_test(self):
        return super().unit_test(['QUERIED_FROM'])

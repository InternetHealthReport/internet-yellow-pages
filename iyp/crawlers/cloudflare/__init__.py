import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from time import sleep

from requests.adapters import HTTPAdapter, Retry
from requests_futures.sessions import FuturesSession

from iyp import BaseCrawler

BATCH_SIZE = 10
RANK_THRESHOLD = 10000
TOP_LIMIT = 100
PARALLEL_DOWNLOADS = 4
MAX_RETRIES = 5

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

        # TODO Fetching data for HostName nodes does not scale at the moment.
        self.host_names_id = dict()
        # existing_hn = self.iyp.tx.run(
        #     """MATCH (hn:HostName)-[r:RANK]-(:Ranking)
        #         WHERE r.rank <= $rank_threshold
        #         RETURN elementId(hn) AS _id, hn.name AS hname""",
        #     rank_threshold=RANK_THRESHOLD)
        # self.host_names_id = {node['hname']: node['_id'] for node in existing_hn}

        # There might be overlap between these two, but we don't want to fetch the same
        # data twice.
        self.names = list(sorted(set(self.domain_names_id.keys()).union(self.host_names_id.keys())))
        # Contains unique values that connect to the names, depending on the crawler.
        # ASNs for top_ases, country codes for top_locations.
        self.to_nodes = set()
        self.links = list()
        # Clear the cache.
        self.tmp_dir = self.create_tmp_dir()
        self.batches = dict()

    def __init_session(self):
        # The session.close() implementation of the FuturesSession does not actually
        # cancel scheduled futures as described in the documentation, so we have to
        # specify our own executor.
        self.executor = ThreadPoolExecutor(PARALLEL_DOWNLOADS)
        self.session = FuturesSession(executor=self.executor)
        # Set API authentication headers.
        self.session.headers['Authorization'] = 'Bearer ' + API_KEY
        self.session.headers['Content-Type'] = 'application/json'

        retries = Retry(total=10,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def make_batches(self):
        # Query Cloudflare API in batches.
        batch = list()
        batch_id = 0
        fpaths = dict()
        for idx, name in enumerate(self.names):
            # Do not overwrite existing files.
            fname = f'data_{name}.json'
            fpath = os.path.join(self.tmp_dir, fname)

            if os.path.exists(fpath):
                continue

            fpaths[name] = fpath

            batch.append(name)
            if len(batch) < BATCH_SIZE and idx < len(self.names):
                # Batch not yet full and not in last iteration.
                continue

            if not batch:
                # If the number of batches perfectly lines up, we do not want to send a
                # broken request without names.
                break

            get_params = f'?limit={TOP_LIMIT}'
            for domain in batch:
                get_params += f'&dateRange=7d&domain={domain}'

            url = self.url + get_params
            self.batches[batch_id] = {'url': url, 'domains': batch, 'fpaths': fpaths}
            batch = list()
            fpaths = dict()
            batch_id += 1
        logging.info(f'Prepared {batch_id} batches')

    def fetch(self):
        """Download data for top RANK_THRESHOLD domain names registered in IYP and save
        it on disk."""

        self.__init_session()

        num_retries = 0
        while self.batches and num_retries <= MAX_RETRIES:
            queries = list()
            for batch_id, batch_data in self.batches.items():
                future = self.session.get(batch_data['url'])
                future.batch_id = batch_id
                queries.append(future)
            logging.info(f'Queued {len(self.batches)} requests. Try: {num_retries}')
            for query in as_completed(queries):
                try:
                    res = query.result()
                    if res.status_code == 429:
                        logging.warning('Got HTTP 429 too many requests. Sleeping for 5 minutes.')
                        self.executor.shutdown(wait=True, cancel_futures=True)
                        self.session.close()
                        sleep(5 * 60)
                        self.__init_session()
                        break

                    res.raise_for_status()
                    # Confirm JSON integrity.
                    data = res.json()
                    if not data['success']:
                        raise ValueError('Response contains success=False')

                    data = data['result']

                    if not self.reference['reference_time_modification']:
                        # Get the reference time from the first file.
                        try:
                            date_str = data['meta']['dateRange'][0]['endTime']
                            date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                            self.reference['reference_time_modification'] = date
                        except (KeyError, ValueError, TypeError) as e:
                            logging.warning(f'Failed to get modification time: {e}')

                    # Remove successful queries from batches list.
                    batch_info = self.batches.pop(query.batch_id)
                    for idx, (placeholder, result) in enumerate(data.items()):
                        if placeholder == 'meta':
                            continue
                        domain = batch_info['domains'][idx]
                        with open(batch_info['fpaths'][domain], 'w') as fp:
                            json.dump({domain: result}, fp)

                except Exception as e:
                    logging.error(f'Failed to fetch data for domains: {self.batches[query.batch_id]["domains"]}: {e}')
                    continue
            num_retries += 1
        if self.batches:
            raise RuntimeError(f'Failed to fetch data for {len(self.batches)} batches.')

    def run(self):
        self.make_batches()
        self.fetch()

        for entry in os.scandir(self.tmp_dir):
            if not entry.is_file() or not entry.name.endswith('.json'):
                continue
            file = os.path.join(self.tmp_dir, entry.name)

            with open(file, 'rb') as fp:
                results = json.load(fp)
                if not results:
                    continue

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

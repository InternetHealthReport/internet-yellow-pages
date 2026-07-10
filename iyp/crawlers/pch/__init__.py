import gzip
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network
from multiprocessing import Pool
from typing import Iterable, Tuple

from requests.adapters import HTTPAdapter
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry

from iyp import (AddressValueError, BaseCrawler, CacheHandler,
                 DataNotAvailableError)
from iyp.crawlers.pch.show_bgp_parser import ShowBGPParser

PARALLEL_DOWNLOADS = 1
PARALLEL_PARSERS = 8
if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    PARALLEL_DOWNLOADS = config['pch']['parallel_downloads']
    PARALLEL_PARSERS = config['pch']['parallel_parsers']

COLLECTOR_LIST_URL_FMT = 'http://downloads.pch.net/api/files/Routing_Data/IPv{af}_daily_snapshots/%Y/%m/'
FILE_FMT = os.path.join(COLLECTOR_LIST_URL_FMT, '{collector}/{collector}-ipv{af}_bgp_routes.%Y.%m.%d.gz')


class RoutingSnapshotCrawler(BaseCrawler):
    """Crawler for PCH route collector data[0].

    Fetches the latest IPv4/IPv6 snapshots in parallel from the PCH website and converts
    them to prefix-AS maps in parallel. This data is used to populate
    (:AS)-[:ORIGINATE]->(:BGPPrefix) entries in the graph.

    If there are no results for the current day for some collectors, the crawler tries
    to fetch older results, up to a maximum of 7 days (configured by self.MAX_LOOKBACK).

    Caches individual route collector entries to prevent restarting from the beginning
    when interrupted.

    [0]
    https://www.pch.net/resources/Routing_Data/
    """

    def __init__(self, organization: str, url: str, name: str, af: int):
        """af: Address family of the crawler. Must be 4 or 6."""
        if af not in (4, 6):
            logging.error(f'Invalid address family: {af}')
            raise AddressValueError(f'Invalid address family: {af}')
        self.MAX_LOOKBACK = timedelta(days=7)
        # self.curr_date = datetime.now(tz=timezone.utc)
        self.curr_date = datetime(2026, 7, 3, tzinfo=timezone.utc)
        self.max_lookback_dt = self.curr_date - self.MAX_LOOKBACK
        self.latest_available_date = None
        self.af = af
        self.parser = ShowBGPParser(self.af)
        cache_file_prefix = f'CACHED.{self.curr_date.strftime("%Y%m%d")}.v{self.af}.'
        self.cache_handler = CacheHandler(self.get_tmp_dir(), cache_file_prefix)
        self.collector_files = dict()
        self.collector_urls = dict()
        self.__initialize_session()
        super().__init__(organization, url, name)
        self.reference['reference_url_data'] = self.curr_date.strftime(COLLECTOR_LIST_URL_FMT.format(af=self.af))
        self.reference['reference_url_info'] = 'https://www.pch.net/resources/Routing_Data/'

    def __initialize_session(self) -> None:
        self.session = FuturesSession(max_workers=PARALLEL_DOWNLOADS)
        self.session.headers['User-Agent'] = 'Internet Yellow Pages - admin@ihr.live'
        retry = Retry(
            backoff_factor=0.1,
            status_forcelist=(429, 500, 502, 503, 504),
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def fetch_urls(self, urls: list) -> Iterable:
        """Fetch the specified URLs in parallel and generate results.

        Per URL, returns the status, binary content, and collector
        name that was passed together with the URL.

        urls: List of (name, url) tuples where the name is returned
              together with the result to keep association.
        """
        queries = list()
        for name, url in urls:
            queries.append((name, self.session.get(url, timeout=60)))
        for name, query in queries:
            try:
                resp = query.result()
                yield resp.ok, resp.content, name
            except Exception as e:
                logging.error(f'Failed to retrieve data for {query}')
                logging.error(e)
                yield False, str(), name

    def fetch_url(self, url: str, name: str = str()) -> Tuple[bool, str, str]:
        """Helper function for single URL."""
        for status, resp, resp_name in self.fetch_urls([(name, url)]):
            return status, resp, resp_name
        return False, str(), str()

    def fetch_and_parse_collector_urls(self, date: datetime) -> None:
        """Fetch the list of collectors available on the specified date.

        Only the year and month components of the date are used, since collectors are
        listed by month. Then create the direct file URL by using the mtime value
        specified for each collector.

        This function propagates self.collector_urls with the URL to the latest file per
        collector and sets self.latest_available_date to the newest mtime. In
        particular, collectors with existing entries in self.collector_urls are not
        overwritten so this function can be called for multiple dates.
        """
        status, resp, _ = self.fetch_url(date.strftime(COLLECTOR_LIST_URL_FMT.format(af=self.af)))
        if not status:
            return
        try:
            collector_list = json.loads(resp)
        except json.JSONDecodeError as e:
            logging.error(f'Failed to decode collector list: {e}')
            return
        for entry in collector_list:
            collector = entry['name']
            if collector in self.collector_urls:
                continue
            mtime = entry['mtime']
            try:
                mtime_dt = datetime.strptime(mtime, '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=timezone.utc)
            except ValueError as e:
                logging.warning(f'Failed to parse mtime "{mtime}" for collector {collector}: {e}')
                continue
            if mtime_dt < self.max_lookback_dt:
                print(f'Ignoring collector {collector} due to stale entry: {mtime_dt.isoformat()}')
                continue
            self.collector_urls[collector] = mtime_dt.strftime(FILE_FMT.format(af=self.af, collector=collector))
            if self.latest_available_date is None or self.latest_available_date < mtime_dt:
                self.latest_available_date = mtime_dt

    def get_collector_urls(self):
        """Get the latest file URL per collector.

        This wrapper function handles the edge case when the lookback interval overlaps
        a month boundary and we need to check two directories for collectors.
        """
        self.fetch_and_parse_collector_urls(self.curr_date)
        if self.curr_date.month != self.max_lookback_dt.month:
            self.fetch_and_parse_collector_urls(self.max_lookback_dt)

    def fetch(self) -> None:
        """Fetch and cache all data.

        First get a list of collector names and their associated files. Then fetch the
        files in parallel.

        All downloaded files are cached, so if this process is restarted, only files
        that are not in the cache are fetched, the rest is loaded from cache.

        Return True if there was an error during the fetching process, else False.
        """

        tmp_dir = self.get_tmp_dir()
        if not os.path.exists(tmp_dir):
            tmp_dir = self.create_tmp_dir()

        self.get_collector_urls()
        if not self.collector_urls:
            raise DataNotAvailableError('Failed to find valid collectors.')

        self.reference['reference_time_modification'] = self.latest_available_date

        # Build list of URLs for files that are not yet cached, and
        # load existing files from cache.
        to_fetch = list()
        for collector_name in self.collector_urls:
            if self.cache_handler.cached_object_exists(collector_name):
                collector_file = self.cache_handler.load_cached_object(collector_name)
                self.collector_files[collector_name] = collector_file
            else:
                to_fetch.append((collector_name, self.collector_urls[collector_name]))

        # Fetch remaining files from PCH.
        attempt = 1
        while to_fetch and attempt <= 10:
            logging.info(f' Attempt {attempt}: {len(self.collector_files)}/{len(self.collector_urls)} collector files '
                         f'in cache, fetching {len(to_fetch)}')
            for ok, content, name in self.fetch_urls(to_fetch):
                if not ok:
                    continue
                # Files are compressed with gzip.
                content = gzip.decompress(content).decode('utf-8')
                self.collector_files[name] = content
                self.cache_handler.save_cached_object(name, content)

            missing_collectors = set(self.collector_urls.keys()) - self.collector_files.keys()
            to_fetch = [(collector, self.collector_urls[collector]) for collector in missing_collectors]
            attempt += 1

    def run(self) -> None:
        """Fetch data from PCH, parse the files, and push nodes and relationships to the
        database."""
        # Pre-fetch all data.
        self.fetch()

        # Parse files in parallel.
        logging.info(f'Parsing {len(self.collector_files)} collector files.')
        fixtures = list()
        for collector_name, collector_file in self.collector_files.items():
            fixtures.append((collector_name, collector_file))
        with Pool(processes=PARALLEL_PARSERS) as p:
            results = p.map(self.parser.parse_parallel, fixtures)
        prefix_maps = dict()
        for collector_name, prefix_map in results:
            prefix_maps[collector_name] = prefix_map

        # Create sets containing all nodes and prefixes and create
        # links.
        ases = set()
        prefixes = set()
        raw_links = defaultdict(set)
        for collector_name, prefix_map in prefix_maps.items():
            for prefix, asn_set in prefix_map.items():
                try:
                    prefix = ip_network(prefix).compressed
                except ValueError as e:
                    logging.warning(f'Ignoring malformed prefix: "{prefix}": {e}')
                    continue
                ases.update(asn_set)
                prefixes.add(prefix)
                for asn in asn_set:
                    raw_links[(asn, prefix)].add(collector_name)

        # Get/push nodes.
        as_ids = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', ases, all=False)
        prefix_ids = self.iyp.batch_get_nodes_by_single_prop('BGPPrefix', 'prefix', prefixes, all=False)
        self.iyp.batch_add_node_label(list(prefix_ids.values()), 'Prefix')

        # Push relationships.
        relationships = list()
        for (asn, prefix), collector_set in raw_links.items():
            props = {'count': len(collector_set),
                     'seen_by_collectors': list(collector_set)}
            relationships.append({'src_id': as_ids[asn],
                                  'dst_id': prefix_ids[prefix],
                                  'props': [props, self.reference]})

        self.iyp.batch_add_links('ORIGINATE', relationships)

        # Clear cache.
        self.cache_handler.clear_cache()

    def unit_test(self):
        return super().unit_test(['ORIGINATE'])

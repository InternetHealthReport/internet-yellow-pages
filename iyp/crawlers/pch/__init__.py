import gzip
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network
from multiprocessing import Pool
from typing import Iterable, Tuple

from bs4 import BeautifulSoup
from bs4.element import ResultSet
from requests.adapters import HTTPAdapter, Response
from requests.exceptions import ChunkedEncodingError
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry

from iyp import (AddressValueError, BaseCrawler, CacheHandler,
                 DataNotAvailableError)
from iyp.crawlers.pch.show_bgp_parser import ShowBGPParser

PARALLEL_DOWNLOADS = 8
PARALLEL_PARSERS = 8
if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    PARALLEL_DOWNLOADS = config['pch']['parallel_downloads']
    PARALLEL_PARSERS = config['pch']['parallel_parsers']


class RoutingSnapshotCrawler(BaseCrawler):
    """Crawler for PCH route collector data[0].

    Fetches the latest IPv4/IPv6 snapshots in parallel from the PCH website and converts
    them to prefix-AS maps in parallel. This data is used to populate
    (:AS)-[:ORIGINATE]->(:Prefix) entries in the graph.

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
        self.af = af
        if self.af == 4:
            self.file_format = '{collector}-ipv4_bgp_routes.{year}.{month:02d}.{day:02d}.gz'
        else:
            self.file_format = '{collector}-ipv6_bgp_routes.{year}.{month:02d}.{day:02d}.gz'
        self.parser = ShowBGPParser(self.af)
        cache_file_prefix = f'CACHED.{datetime.now().strftime("%Y%m%d")}.v{self.af}.'
        self.cache_handler = CacheHandler(self.get_tmp_dir(), cache_file_prefix)
        self.collector_files = dict()
        self.collector_site_url = str()
        self.__initialize_session()
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://www.pch.net/resources/Routing_Data/'

    def __initialize_session(self) -> None:
        self.session = FuturesSession(max_workers=PARALLEL_DOWNLOADS)
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
            except ChunkedEncodingError as e:
                logging.error(f'Failed to retrieve data for {query}')
                logging.error(e)
                return False, str(), name

    def fetch_url(self, url: str, name: str = str()) -> Tuple[bool, str, str]:
        """Helper function for single URL."""
        for status, resp, resp_name in self.fetch_urls([(name, url)]):
            return status, resp, resp_name
        return False, str(), str()

    def fetch_collector_site(self) -> str:
        """Fetch the HTML code of the collector site for the current month.

        If the site does not yet exist, check the previous month, as long as it is
        within the lookback interval.
        """
        logging.info('Fetching list of collectors.')
        today = datetime.now(tz=timezone.utc)
        self.collector_site_url = self.url + today.strftime('%Y/%m/')
        resp = self.session.get(self.collector_site_url).result()
        if resp.ok:
            return resp.text
        logging.warning(f'Failed to retrieve collector site from: {self.collector_site_url}')
        curr_month = today.month
        lookback = today - self.MAX_LOOKBACK
        if lookback.month == curr_month:
            logging.error('Failed to find current data.')
            raise DataNotAvailableError('Failed to find current data.')
        self.collector_site_url = self.url + today.strftime('%Y/%m/')
        resp: Response = self.session.get(self.collector_site_url).result()
        if resp.ok:
            return resp.text
        logging.warning(f'Failed to retrieve collector site from: {self.collector_site_url}')
        logging.error('Failed to find current data.')
        raise DataNotAvailableError('Failed to find current data.')

    @staticmethod
    def filter_route_collector_links(links: ResultSet) -> list:
        """Extract route collector names from HTML code."""
        collector_names = list()
        for a in links:
            if 'href' not in a.attrs or not a['href'].startswith('route-collector'):
                continue
            collector_names.append(a['href'].rstrip('/'))
        return collector_names

    def make_url(self, collector_name: str, date: datetime) -> str:
        """Create file URLs based on the template and gathered route collector names."""
        file_name = self.file_format.format(collector=collector_name,
                                            year=date.year,
                                            month=date.month,
                                            day=date.day)
        file_url = f'{self.url}{date.strftime("%Y/%m/")}{collector_name}/{file_name}'
        return file_url

    def probe_latest_set(self, collector_name: str) -> datetime:
        """Find the date of the latest available dataset for the specified collector.

        Start with the current date and look up to MAX_LOOKBACK days into the past if no
        current data is found.

        Return None if no data is found within the valid interval.
        """
        logging.info('Probing latest available dataset.')
        curr_date = datetime.now(tz=timezone.utc)
        max_lookback = curr_date - self.MAX_LOOKBACK
        while curr_date >= max_lookback:
            file_name = self.file_format.format(collector=collector_name,
                                                year=curr_date.year,
                                                month=curr_date.month,
                                                day=curr_date.day)
            probe_url = f'{self.url}{curr_date.strftime("%Y/%m/")}{collector_name}/{file_name}'
            resp = self.session.head(probe_url).result()
            if resp.status_code == 200:
                logging.info(f'Latest available dataset: {curr_date.strftime("%Y-%m-%d")}')
                return curr_date
            curr_date -= timedelta(days=1)
        logging.error('Failed to find current data.')
        raise DataNotAvailableError('Failed to find current data.')

    def fetch(self) -> None:
        """Fetch and cache all data.

        First get a list of collector names and their associated files. Then fetch the
        files in parallel. If some files are not available for the current date, try
        fetching older data as long as it is within the lookback interval.

        All downloaded files are cached, so if this process is restarted, only files
        that are not in the cache are fetched, the rest is loaded from cache.

        Return True if there was an error during the fetching process, else False.
        """
        collector_names_name = 'collectors'

        tmp_dir = self.get_tmp_dir()
        if not os.path.exists(tmp_dir):
            tmp_dir = self.create_tmp_dir()

        # Get a list of collector names
        if self.cache_handler.cached_object_exists(collector_names_name):
            self.collector_site_url, collector_names = self.cache_handler.load_cached_object(collector_names_name)
        else:
            collector_site = self.fetch_collector_site()
            soup = BeautifulSoup(collector_site, features='html.parser')
            links = soup.find_all('a')
            collector_names = self.filter_route_collector_links(links)
            self.cache_handler.save_cached_object(collector_names_name, (self.collector_site_url, collector_names))
        self.reference['reference_url_data'] = self.collector_site_url

        # Get the date of the latest available dataset based on the
        # first collector in the list.
        # This may be not the best method if only the first collector
        # is missing the most up-to-date data for some reason, but
        # generally this prevents a lot of requests to non-existing
        # files (one per collector) if the data for the current date
        # is not yet available for all collectors.
        latest_available_date = self.probe_latest_set(collector_names[0])
        self.reference['reference_time_modification'] = latest_available_date.replace(hour=0,
                                                                                      minute=0,
                                                                                      second=0,
                                                                                      microsecond=0)
        curr_date = datetime.now(tz=timezone.utc)
        max_lookback = curr_date - self.MAX_LOOKBACK

        # Build list of URLs for files that are not yet cached, and
        # load existing files from cache.
        to_fetch = list()
        for collector_name in collector_names:
            if self.cache_handler.cached_object_exists(collector_name):
                collector_file = self.cache_handler.load_cached_object(collector_name)
                self.collector_files[collector_name] = collector_file
            else:
                collector_url = self.make_url(collector_name, latest_available_date)
                to_fetch.append((collector_name, collector_url))

        # Fetch remaining files from PCH.
        if to_fetch:
            logging.info(f'{len(self.collector_files)}/{len(collector_names)} collector files in cache, fetching '
                         f'{len(to_fetch)}')

            # If some collectors do not have current data available,
            # try again until the max lookback window is reached.
            failed_fetches = list()
            while to_fetch and latest_available_date >= max_lookback:
                failed_fetches = list()
                for ok, content, name in self.fetch_urls(to_fetch):
                    if not ok:
                        failed_fetches.append(name)
                        continue
                    # Files are compressed with gzip.
                    content = gzip.decompress(content).decode('utf-8')
                    self.collector_files[name] = content
                    self.cache_handler.save_cached_object(name, content)

                # Create new URLs for collectors that have failed.
                to_fetch = list()
                latest_available_date -= timedelta(days=1)
                for collector_name in failed_fetches:
                    collector_url = self.make_url(collector_name, latest_available_date)
                    to_fetch.append((collector_name, collector_url))
                if to_fetch:
                    logging.info(f'Retrying fetch for {len(to_fetch)} collectors for date '
                                 f'{latest_available_date.strftime("%Y-%m-%d")}')
            if failed_fetches:
                # Max lookback reached.
                logging.warning(f'Failed to find current data for {len(failed_fetches)} collectors: {failed_fetches}')

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
        prefix_ids = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes, all=False)

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

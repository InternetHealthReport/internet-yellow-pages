import ipaddress
import logging
import os
from collections import defaultdict
from concurrent.futures import as_completed
from datetime import datetime
from json import JSONDecodeError
from typing import Iterable, Tuple

import flatdict
import radix
from requests.adapters import HTTPAdapter, Response
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry

from iyp import BaseCrawler, CacheHandler

# Alice-LG Rest API
#
# The API provides endpoints for getting
# information from the routeservers / alice datasources.
#
# Endpoints:
#
#   Config
#     Show         /api/v1/config
#
#   Routeservers
#     List         /api/v1/routeservers
#     Status       /api/v1/routeservers/:id/status
#     Neighbors    /api/v1/routeservers/:id/neighbors
#     Routes       /api/v1/routeservers/:id/neighbors/:neighborId/routes
#                  /api/v1/routeservers/:id/neighbors/:neighborId/routes/received
#                  /api/v1/routeservers/:id/neighbors/:neighborId/routes/filtered
#                  /api/v1/routeservers/:id/neighbors/:neighborId/routes/not-exported
#
#   Querying
#     LookupPrefix   /api/v1/lookup/prefix?q=<prefix>
#     LookupNeighbor /api/v1/lookup/neighbor?asn=1235

# Model
#   /config
#     AS of peering LAN -> Not linkable to IXP without manual relationship
#     Could model the looking glass, but not interesting I think
#   /routeservers
#     Model route server? (:RouteServer)-[:ROUTESERVER]->(:IXP)
#     group specifies to which IXP the route server belongs. May or may not match name
#     of IXP node.

# Find IXP via neighbor IP -> peeringLAN lookup
#   /routeservers/:id/neighbors
#     (:AS)-[:MEMBER_OF]->(:IXP)
#     Get IXP peering LANs:
#       MATCH (p:Prefix)-[:MANAGED_BY]->(i:IXP)
#       RETURN p.prefix AS peering_lan, ID(i) AS ixp_qid
#     neighbors -> list of neighbors
#       neighbor['address'] -> map to prefix
#   /routeservers/:id/neighbors/:neighborId/routes/received
#     (:AS)-[:ORIGINATE]->(:Prefix)
#     received['imported'] -> list of routes
#       route['network'] -> prefix
#       route['bgp']['as_path'][-1] -> originating ASN


class Crawler(BaseCrawler):
    """Import IXP members and optionally prefix announcements based on routes received
    via members from Alice-LG-based looking glasses."""
    # In principle, the fetching process can be sped up by performing parallel queries.
    # However, some tests showed that many looking glasses perform poorly when queried
    # in parallel, which is why I leave the functionality in the code, but set the
    # default values to not query in parallel.
    # Similarly, querying the received routes is infeasible for large IXPs since the
    # queries just take too long, which is why the functionality is disabled by default.

    def __init__(self,
                 organization: str,
                 url: str,
                 name: str,
                 parallel_downloads: int = 1,
                 fetch_routes: bool = False,
                 fetch_routes_batch_size: int = 1) -> None:
        super().__init__(organization, url, name)

        # URLs to the API
        url = url.rstrip('/')
        if url.endswith('/api/v1'):
            self.reference['reference_url_info'] = url[:-len('/api/v1')]
        else:
            logging.warning(f'Data URL does not end with "/api/v1", will not set info URL: {url}')
        self.urls = {
            'routeservers': f'{url}/routeservers',
            'neighbors': url + '/routeservers/{rs}/neighbors',
            'routes': url + '/routeservers/{rs}/neighbors/{neighbor}/routes/received'
        }
        cache_file_prefix = f'CACHED.{datetime.now().strftime("%Y%m%d")}.'
        self.cache_handler = CacheHandler(self.get_tmp_dir(), cache_file_prefix)
        self.workers = parallel_downloads
        # List of route server dicts.
        self.routeservers = list()
        # List of neighbor dicts. Each dict contains information about the route server,
        # so we do not keep track of that separately.
        self.neighbors = list()
        # Dict mapping routeserver_id to the cache time of that server.
        self.routeserver_cached_at = dict()
        # Dict mapping (routeserver_id, neighbor_id) tuple to a list of route dicts.
        self.routes = dict()
        # If routes should be fetched or not.
        self.fetch_routes = fetch_routes
        self.fetch_routes_batch_size = fetch_routes_batch_size
        self.__initialize_session()

    def __initialize_session(self) -> None:
        self.session = FuturesSession(max_workers=self.workers)
        retry = Retry(
            backoff_factor=0.1,
            status_forcelist=(429, 500, 502, 503, 504),
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    @staticmethod
    def decode_json(resp: Response, *args, **kwargs) -> None:
        """Process json in background."""
        logging.debug(f'Processing response: {resp.url} Status: {resp.ok}')

        try:
            resp.data = resp.json()
        except JSONDecodeError as e:
            logging.error(f'Error while reading json data: {e}')
            logging.error(resp.status_code)
            logging.error(resp.headers)
            logging.error(resp.text)
            resp.data = dict()

    def fetch_urls(self, urls: list, additional_data=list()) -> Iterable:
        """Fetch the specified URLs in parallel and yield the status, result, and
        optionally an additional data item.

        Since results are yielded in order of completion, which might differ from the
        order of URLs in the list, the additional_data list can be used to pass
        arbitrary objects (e.g., an identifier) that will be returned together with the
        corresponding result. For this purpose the additional_data list, if specified,
        must have the same length as the urls list.
        """
        if additional_data and len(additional_data) != len(urls):
            raise ValueError('Additional data list must have the same length as URL list.')
        if not additional_data:
            # Create empty data list.
            additional_data = [None] * len(urls)
        queries = list()
        for idx, url in enumerate(urls):
            future = self.session.get(url,
                                      hooks={'response': self.decode_json},
                                      timeout=60)
            future.additional_data = additional_data[idx]
            queries.append(future)
        for future in as_completed(queries):
            try:
                resp = future.result()
                yield resp.ok, resp.data, future.additional_data
            except Exception as e:
                logging.error(f'Failed to retrieve data for {future}')
                logging.error(e)
                return False, dict(), None

    def fetch_url(self, url: str) -> Tuple[bool, dict]:
        """Helper function for single URL."""
        for status, resp, _ in self.fetch_urls([url]):
            return status, resp
        return False, dict()

    def __fetch_routeservers(self) -> None:
        """Fetch the list of routeservers or load from cache."""
        routeserver_object_name = 'routeservers'
        if self.cache_handler.cached_object_exists(routeserver_object_name):
            logging.info('Using cached route server information.')
            self.routeservers = self.cache_handler.load_cached_object(routeserver_object_name)
        else:
            logging.info(f'Fetching route servers from {self.urls["routeservers"]}')
            is_ok, routeservers_root = self.fetch_url(self.urls['routeservers'])
            if not is_ok:
                raise Exception('Failed to fetch route servers.')
            self.routeservers = routeservers_root['routeservers']
            self.cache_handler.save_cached_object(routeserver_object_name, self.routeservers)

    def __fetch_neighbors(self) -> None:
        """Fetch neighbor information in parallel or load from cache."""
        neighbor_object_name = 'neighbors'
        if self.cache_handler.cached_object_exists(neighbor_object_name):
            logging.info('Using cached neighbor information.')
            neighbor_object = self.cache_handler.load_cached_object(neighbor_object_name)
            self.routeserver_cached_at = neighbor_object['routeserver_cached_at']
            self.neighbors = neighbor_object['neighbors']
        else:
            logging.info(f'Fetching neighbor information from {len(self.routeservers)} route servers.')
            neighbor_urls = [self.urls['neighbors'].format(rs=rs['id']) for rs in self.routeservers]
            failed_routeservers = list()
            for is_ok, neighbor_list_root, routeserver in self.fetch_urls(neighbor_urls,
                                                                          additional_data=self.routeservers):
                routeserver_id = routeserver['id']
                if not is_ok:
                    failed_routeservers.append(routeserver_id)
                    continue
                try:
                    cached_at_str = neighbor_list_root['api']['cache_status']['cached_at']
                except KeyError:
                    cached_at_str = str()
                if cached_at_str:
                    cached_at = None
                    # Alice-LG uses nanosecond-granularity timestamps, which are not
                    # valid ISO format...
                    try:
                        pre, suf = cached_at_str.rsplit('.', maxsplit=1)
                        if suf.endswith('Z'):
                            # UTC
                            frac_seconds = suf[:-1]
                            tz_suffix = '+00:00'
                        elif '+' in suf:
                            # Hopefully a timezone identifier of form +HH:MM
                            frac_seconds, tz_suffix = suf.split('+')
                            tz_suffix = '+' + tz_suffix
                        else:
                            raise ValueError(f'Failed to get timezone from timestamp :{cached_at_str}')
                        if not frac_seconds.isdigit():
                            raise ValueError(f'Fractional seconds are not digits: {cached_at_str}')
                        # Reduce to six digits (ms).
                        frac_seconds = frac_seconds[:6]
                        cached_at_str = f'{pre}.{frac_seconds}{tz_suffix}'
                        cached_at = datetime.fromisoformat(cached_at_str)
                    except ValueError as e:
                        logging.warning(f'Failed to get cached_at timestamp for routeserver "{routeserver_id}": {e}')
                    if cached_at:
                        self.routeserver_cached_at[routeserver_id] = cached_at
                # Spelling of neighbors/neighbours field is not consistent...
                if 'neighbors' in neighbor_list_root:
                    neighbor_list = neighbor_list_root['neighbors']
                elif 'neighbours' in neighbor_list_root:
                    neighbor_list = neighbor_list_root['neighbours']
                else:
                    logging.error(f'Missing "neighbors"/"neighbours" field in reply: {neighbor_list_root}')
                    continue
                self.neighbors += neighbor_list
            neighbor_object = {'routeserver_cached_at': self.routeserver_cached_at,
                               'neighbors': self.neighbors}
            self.cache_handler.save_cached_object(neighbor_object_name, neighbor_object)
            if failed_routeservers:
                logging.warning(f'Failed to get neighbor information for {len(failed_routeservers)} routeservers: '
                                f'{failed_routeservers}')

    def __fetch_routes(self) -> None:
        """Fetch received route information or load from cache."""
        routes_object_name_prefix = 'routes.'
        cached_route_objects = 0
        fetch_required = list()
        for neighbor in self.neighbors:
            if neighbor['routes_received'] == 0:
                # No query required.
                continue
            neighbor_id = neighbor['id']
            routeserver_id = neighbor['routeserver_id']
            key = (routeserver_id, neighbor_id)
            object_name = f'{routes_object_name_prefix}{routeserver_id}.{neighbor_id}'
            if self.cache_handler.cached_object_exists(object_name):
                cached_route_objects += 1
                self.routes[key] = self.cache_handler.load_cached_object(object_name)
            else:
                fetch_required.append(key)

        total_route_objects = cached_route_objects + len(fetch_required)
        logging.info(f'{cached_route_objects}/{total_route_objects} route objects in cache. Fetching '
                     f'{len(fetch_required)}')

        if fetch_required:
            for i in range(0, len(fetch_required), self.fetch_routes_batch_size):
                logging.debug(f'Batch {i}')
                batch = fetch_required[i: i + self.fetch_routes_batch_size]
                # Fetch in two rounds. First round gets the initial page for all
                # neighbors and generates the remaining URLs based on that. Second round
                # fetched the remaining pages.
                urls = list()
                additional_data = list()
                for routeserver_id, neighbor_id in batch:
                    url = self.urls['routes'].format(rs=routeserver_id, neighbor=neighbor_id)
                    urls.append(url)
                    additional_data.append((routeserver_id, neighbor_id))
                next_urls = list()
                next_additional_data = list()
                failed_neighbors = list()
                for ok, data, key in self.fetch_urls(urls, additional_data):
                    if not ok:
                        failed_neighbors.append(key)
                        continue
                    self.routes[key] = data['imported']
                    if data['pagination']['total_pages'] > 1:
                        base_url = self.urls['routes'].format(rs=key[0], neighbor=key[1])
                        # Alice LG pagination is zero indexed, i.e., first page is 0.
                        for page in range(1, min(data['pagination']['total_pages'], 10)):
                            next_urls.append(f'{base_url}?page={page}')
                            next_additional_data.append(key)
                logging.debug('First round done.')
                logging.debug(f'Fetching {len(next_urls)} additional pages.')
                failed_pages = defaultdict(int)
                for ok, data, key in self.fetch_urls(next_urls, next_additional_data):
                    if not ok:
                        failed_pages[key] += 1
                        continue
                    self.routes[key] += data['imported']
                logging.debug('Second round done.')
                logging.debug('Caching.')
                for routeserver_id, neighbor_id in batch:
                    key = (routeserver_id, neighbor_id)
                    if key not in self.routes:
                        continue
                    object_name = f'{routes_object_name_prefix}{routeserver_id}.{neighbor_id}'
                    self.cache_handler.save_cached_object(object_name, self.routes[key])

            if failed_neighbors:
                logging.warning(f'Failed to fetch routes for {len(failed_neighbors)} neighbors: {failed_neighbors}')
            if failed_pages:
                logging.warning(
                    f'Failed to fetch {sum(failed_pages.values())} pages for {len(failed_pages)} neighbors:')
                for key, count in failed_pages.items():
                    logging.warning(f'  {key}: {count}')

    def fetch(self) -> None:
        tmp_dir = self.get_tmp_dir()
        if not os.path.exists(tmp_dir):
            logging.info(f'Creating tmp dir: {tmp_dir}')
            self.create_tmp_dir()

        self.__fetch_routeservers()
        self.__fetch_neighbors()
        if self.fetch_routes:
            self.__fetch_routes()

    def __get_peering_lans(self) -> radix.Radix:
        """Get IXP peering LANs from IYP and return a radix tree containing the QID of
        the IXP node in the data['ixp_qid'] field of each tree node."""
        query = """MATCH (p:Prefix)-[:MANAGED_BY]->(i:IXP)
                   RETURN p.prefix AS peering_lan, ID(i) AS ixp_qid"""
        peering_lans = radix.Radix()
        for res in self.iyp.tx.run(query):
            n = peering_lans.add(res['peering_lan'])
            n.data['ixp_qid'] = res['ixp_qid']
        logging.info(f'Fetched {len(peering_lans.nodes())} peering LANs')
        return peering_lans

    def run(self) -> None:
        self.fetch()

        peering_lans = self.__get_peering_lans()

        # Compute MEMBER_OF relationships from neighbor data.
        asns = set()
        member_of_rels = list()
        logging.info('Iterating neighbors.')
        for neighbor in self.neighbors:
            member_ip = neighbor['address']
            n = peering_lans.search_best(member_ip)
            if n is None:
                logging.warning(f'Failed to map member IP to peering LAN: {member_ip}')
                continue
            member_asn = neighbor['asn']
            if not member_asn or not isinstance(member_asn, int):
                logging.warning(f'Malformed member ASN: "{member_asn}"')
                continue
            asns.add(member_asn)

            # This is a bit silly, but for some neighbors that are in a weird state,
            # there can be an empty dict in details:route_changes, which will remain as
            # a FlatDict. Since neo4j does not like maps as properties, remove any empty
            # dicts left in the property.
            flattened_neighbor = dict(flatdict.FlatDict(neighbor))
            if ('details:route_changes' in flattened_neighbor
                    and isinstance(flattened_neighbor['details:route_changes'], flatdict.FlatDict)):
                flattened_neighbor.pop('details:route_changes')
            routeserver_id = neighbor['routeserver_id']
            self.reference['reference_url_data'] = self.urls['neighbors'].format(rs=routeserver_id)
            if routeserver_id in self.routeserver_cached_at:
                self.reference['reference_time_modification'] = self.routeserver_cached_at[routeserver_id]
            else:
                logging.info(f'No modification time for routeserver: {routeserver_id}')
                # Set to None to not reuse value of previous loop iteration.
                self.reference['reference_time_modification'] = None

            member_of_rels.append({'src_id': member_asn,  # Translate to QID later.
                                   'dst_id': n.data['ixp_qid'],
                                   'props': [flattened_neighbor, self.reference.copy()]})

        # Compute ORIGINATE relationships from received routes.
        prefixes = set()
        originate_rels = list()
        if self.fetch_routes:
            logging.info('Iterating routes.')
            for (routeserver_id, neighbor_id), routes in self.routes.items():
                self.reference['reference_url_data'] = self.urls['routes'].format(rs=routeserver_id,
                                                                                  neighbor=neighbor_id)
                for route in routes:
                    prefix = ipaddress.ip_network(route['network']).compressed
                    origin_asn = route['bgp']['as_path'][-1]
                    prefixes.add(prefix)
                    asns.add(origin_asn)
                    # route object contains lists of lists so FlatterDict is required.
                    # Similar to above, there can be an empty dicts inside the object,
                    # but for different keys, which is why we just iterate over all of
                    # them.
                    flattened_route = dict(flatdict.FlatterDict(route))
                    to_delete = list()
                    for k, v in flattened_route.items():
                        if isinstance(v, flatdict.FlatterDict):
                            to_delete.append(k)
                    for k in to_delete:
                        flattened_route.pop(k)
                    flattened_route['routeserver_id'] = routeserver_id
                    originate_rels.append({'src_id': origin_asn,  # Translate to QIDs later.
                                           'dst_id': prefix,
                                           'props': [flattened_route, self.reference.copy()]})

        # Get/create nodes.
        logging.info(f'Getting {len(asns)} AS nodes.')
        asn_id = self.iyp.batch_get_nodes_by_single_prop('AS', 'asn', asns, all=False)
        prefix_id = dict()
        if prefixes:
            logging.info(f'Getting {len(prefixes)} Prefix nodes.')
            prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefixes, all=False)

        # Translate raw values to QID.
        for relationship in member_of_rels:
            asn = relationship['src_id']
            relationship['src_id'] = asn_id[asn]
        for relationship in originate_rels:
            asn = relationship['src_id']
            prefix = relationship['dst_id']
            relationship['src_id'] = asn_id[asn]
            relationship['dst_id'] = prefix_id[prefix]

        # Push relationships.
        logging.info(f'Pushing {len(member_of_rels)} MEMBER_OF relationships.')
        self.iyp.batch_add_links('MEMBER_OF', member_of_rels)
        if originate_rels:
            logging.info(f'Pushing {len(originate_rels)} ORIGINATE relationships.')
            self.iyp.batch_add_links('ORIGINATE', originate_rels)

    def unit_test(self):
        super().unit_test(['MEMBER_OF', 'ORIGINATE', 'MANAGED_BY'])

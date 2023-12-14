import logging
import re
import sys
from collections import defaultdict, namedtuple
from ipaddress import (AddressValueError, IPv4Address, IPv4Network,
                       IPv6Address, IPv6Network)
from typing import Tuple

Route = namedtuple('Route', 'status_codes network next_hop metric weight path origin_code')


class ShowBGPParser:
    """A parser that transforms the output of a Cisco-style 'show ip bgp' command to a
    prefix-AS map.

    Supports IPv4 and IPv6 output. Note that this was only tested on PCH route collector
    data [0], so there might be edge cases that break the parser, which are not showing
    up in this data.

    [0]
    https://www.pch.net/resources/Routing_Data/
    """

    def __init__(self, af: int) -> None:
        """af: Address family of the parser. Must be 4 or 6."""
        if af not in (4, 6):
            logging.error(f'Invalid address family specified: {af}')
            sys.exit('Invalid address family specified.')
        self.af = af
        self.status_codes = {'s': 'suppressed',
                             'd': 'damped',
                             'h': 'history',
                             '*': 'valid',
                             '>': 'best',
                             '=': 'multipath',
                             'i': 'internal',
                             'r': 'RIB-failure',
                             'S': 'Stale',
                             'R': 'Removed'}
        self.origin_codes = {'i': 'IGP',
                             'e': 'EGP',
                             '?': 'incomplete'}
        # AS hops can be integers, single integers wrapped in curly
        # parenthesis, or AS sets that are comma-separated integers
        # wrapped in curly parenthesis.
        self.as_hop_pattern = re.compile(r'[0-9]+|\{[0-9]+(,[0-9]+)*}')
        self.collector = str()

    def __handle_status_codes(self, code: str) -> set:
        """Parse sequence of status codes into set with labels."""
        ret = set()
        for c in code:
            if c not in self.status_codes:
                logging.critical(f'{self.collector}: Invalid status code {c} in code {code}')
                print(f'{self.collector}: Invalid status code {c} in code {code}', file=sys.stderr)
                return set()
            ret.add(self.status_codes[c])
        return ret

    def __handle_origin_code(self, code: str) -> str:
        """Map origin code to its label."""
        if code not in self.origin_codes:
            logging.critical(f'{self.collector}: Invalid origin code {code}')
            print(f'{self.collector}: Invalid origin code {code}', file=sys.stderr)
            return str()
        return self.origin_codes[code]

    def __parse_line(self, line_split: list, last_pfx: str) -> Route:
        """Parse a single line into a Route object.

        Since the number of fields is not the same for all lines (e.g., some lines do
        not have a network field, but inherit it from the previous line), instead of
        juggling indexes, this function mostly looks at the first entry and pops it from
        the list once it has been processed.
        """
        if ':' not in line_split[0] and set(line_split[0]).intersection(self.status_codes.keys()):
            # Not all lines have a status code apparently.
            # But IPv6 addresses can contain a 'd', so we need to
            # extra-exclude them...
            status_codes = self.__handle_status_codes(line_split.pop(0))
        else:
            status_codes = set()
        if '/' in line_split[0]:
            network = line_split.pop(0)
            last_pfx = network
        elif not line_split[1].isdigit():
            # Edge case where prefix size needs to be inferred from
            # classful address. This is recognized by a lookahead
            # that checks if the next entry is the metric.
            # Can only happen to IPv4 addresses.
            # See RFC 791:
            # https://datatracker.ietf.org/doc/html/rfc791
            try:
                address_str = line_split.pop(0)
                address = IPv4Address(address_str)
            except AddressValueError as e:
                logging.error(f'{self.collector}: Invalid classful address: {address_str}')
                logging.error(f'{self.collector}: {e}')
                return None
            address_int = int(address.packed.hex(), base=16)
            if address_int >> 31 == 0b0:
                network = f'{address}/8'
            elif address_int >> 30 == 0b10:
                network = f'{address}/16'
            elif address_int >> 29 == 0b110:
                network = f'{address}/24'
            else:
                logging.error(f'{self.collector}: Invalid classful address: {address}')
                return None
        else:
            # Prefix inherited from previous line.
            network = last_pfx
        next_hop = line_split[0]
        metric = line_split[1]
        weight = line_split[2]
        path = line_split[3:-1]
        origin_code = self.__handle_origin_code(line_split[-1])
        return Route(status_codes, network, next_hop, metric, weight, path, origin_code)

    def __valid_route(self, route: Route) -> bool:
        """Check that all parsed fields are sensible."""
        try:
            if self.af == 4:
                IPv4Network(route.network)
                IPv4Address(route.next_hop)
            else:
                IPv6Network(route.network)
                IPv6Address(route.next_hop)
        except AddressValueError as e:
            logging.error(f'{self.collector}: Invalid network or next hop in route: {route}')
            logging.error(f'{self.collector}: {e}')
            return False

        if not route.metric.isdigit():
            logging.error(f'{self.collector}: {route}')
            logging.error(f'{self.collector}: Invalid metric {route.metric}')
            return False
        if not route.weight.isdigit():
            logging.error(f'{self.collector}: {route}')
            logging.error(f'{self.collector}: Invalid weight: {route.weight}')
            return False
        for asn in route.path:
            if not self.as_hop_pattern.match(asn):
                logging.error(f'{self.collector}: {route}')
                logging.error(f'{self.collector}: Invalid AS path: {route.path}')
                return False
        if not route.status_codes or not route.origin_code:
            return False
        return True

    def __build_prefix_map(self, routes: list) -> dict:
        """Build a prefix map from the list of Route objects.

        Only include routes that have a 'valid' status code and whose origin code is
        _not_ 'incomplete'. Ignore AS sets for now.
        """
        prefix_map = defaultdict(set)
        not_valid_routes = 0
        as_sets = 0
        incomplete_origin_routes = 0
        for route in routes:
            if 'valid' not in route.status_codes:
                not_valid_routes += 1
                continue
            if route.origin_code == '?':
                incomplete_origin_routes += 1
                continue
            if not route.path:
                logging.debug(f'{self.collector}: Route without AS path: {route}')
                continue
            origin = route.path[-1].strip('{}')
            if ',' in origin:
                # TODO Handle AS sets.
                as_sets += 1
                continue
            origin = int(origin)
            prefix = route.network
            prefix_map[prefix].add(origin)
        if not_valid_routes:
            logging.info(f'{self.collector}: Ignored {not_valid_routes} not valid routes.')
            print(f'{self.collector}: Ignored {not_valid_routes} not valid routes.', file=sys.stderr)
        if as_sets:
            logging.info(f'{self.collector}: Ignored {as_sets} AS set origins.')
            print(f'{self.collector}: Ignored {as_sets} AS set origins.', file=sys.stderr)
        if incomplete_origin_routes:
            logging.info(f'{self.collector}: Ignored {incomplete_origin_routes} routes with incomplete origin.')
        return prefix_map

    def parse_file(self, input_file: str) -> dict:
        """Read a file containing the input and return a prefix-AS map."""
        with open(input_file, 'r') as f:
            return self.parse(f.read())

    def parse_parallel(self, fixture: tuple) -> Tuple[str, dict]:
        """Helper function for use with parallel parsing.

        The collector name is passed together with the input string
        so that it can be returned together with the prefix-AS map
        to associate the result with the collector.

        fixture: Tuple of (collector_name, input_str)
        """
        collector_name, input_str = fixture
        logging.info(collector_name)
        self.collector = collector_name
        return collector_name, self.parse(input_str)

    def parse(self, input_str: str) -> dict:
        """Parse the input string and return a prefix-AS map.

        Return an empty dictionary in case of an error.
        """
        lines = iter(input_str.splitlines())
        try:
            while not next(lines).lstrip().startswith('Network'):
                pass
        except StopIteration:
            logging.warning(f'{self.collector}: Empty file.')
            print(f'{self.collector}: Empty file.', file=sys.stderr)
            return dict()
        routes = list()
        last_pfx = str()
        for line in lines:
            line_split = line.strip().split()
            if not line_split:
                # End of file.
                break
            # Route output can be split over multiple lines, so we
            # need to reassemble them before parsing.
            if self.af == 4:
                # IPv4 lines can be split in two.
                if len(line_split) <= 2:
                    line_split += next(lines).strip().split()
            else:
                # IPv6 lines can be split in two.
                if len(line_split) <= 3:
                    line_split += next(lines).strip().split()
                if len(line_split) <= 3:
                    # Sometimes even three...
                    line_split += next(lines).strip().split()
            route = self.__parse_line(line_split, last_pfx)
            if route is None:
                continue
            # Keep track of the last seen prefix, since it can be
            # inherited by subsequent lines.
            last_pfx = route.network
            if self.__valid_route(route):
                routes.append(route)
        return self.__build_prefix_map(routes)

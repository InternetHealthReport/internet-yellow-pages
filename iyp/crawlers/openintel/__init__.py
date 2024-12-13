# Simple Python script to fetch domain name to IP address mappings from OpenINTEL data
# OpenIntelCrawler is based on code from Mattijs Jonker <m.jonker@utwente.nl>

import json
import logging
import os
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from ipaddress import IPv6Address

import arrow
import boto3
import botocore
import pandas as pd
import requests

from iyp import BaseCrawler, DataNotAvailableError

TMP_DIR = './tmp'
os.makedirs(TMP_DIR, exist_ok=True)

# credentials
OPENINTEL_ACCESS_KEY = ''
OPENINTEL_SECRET_KEY = ''

if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    OPENINTEL_ACCESS_KEY = config['openintel']['access_key']
    OPENINTEL_SECRET_KEY = config['openintel']['secret_key']

# We use the AWS interface to get data, but can not provide AWS URLs as data source, so
# at least for the Tranco and Umbrella datasets we can point to the publicly available
# archives.
TRANCO_REFERENCE_URL_DATA_FMT = 'https://data.openintel.nl/data/tranco1m/%Y/openintel-tranco1m-%Y%m%d.tar'
UMBRELLA_REFERENCE_URL_DATA_FMT = 'https://data.openintel.nl/data/umbrella1m/%Y/openintel-umbrella1m-%Y%m%d.tar'


class OpenIntelCrawler(BaseCrawler):
    def __init__(self, organization, url, name, dataset):
        """Initialization of the OpenIntel crawler requires the name of the dataset
        (e.g. tranco or infra:ns)."""

        self.dataset = dataset
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://www.openintel.nl/'
        if dataset == 'tranco':
            self.reference['reference_url_info'] = 'https://data.openintel.nl/data/tranco1m'
        elif dataset == 'umbrella':
            self.reference['reference_url_info'] = 'https://data.openintel.nl/data/umbrella1m'

    def get_parquet(self):
        """Fetch the forward DNS data, populate a data frame, and process lines one by
        one."""

        # Get a boto3 resource
        S3A_OPENINTEL_ENDPOINT = 'https://object.openintel.nl'
        S3R_OPENINTEL = boto3.resource(
            's3',
            'nl-utwente',
            aws_access_key_id=OPENINTEL_ACCESS_KEY,
            aws_secret_access_key=OPENINTEL_SECRET_KEY,
            endpoint_url=S3A_OPENINTEL_ENDPOINT,
            config=botocore.config.Config(
                signature_version='v4'
            )
        )

        # Prevent some request going to AWS instead of the OpenINTEL server
        S3R_OPENINTEL.meta.client.meta.events.unregister('before-sign.s3', botocore.utils.fix_s3_host)

        # The OpenINTEL bucket
        WAREHOUSE_BUCKET = S3R_OPENINTEL.Bucket('openintel')

        # OpenINTEL measurement data objects base prefix
        FDNS_WAREHOUSE_S3 = 'category=fdns/type=warehouse'

        # Get latest available data.
        date = arrow.utcnow()
        for lookback_days in range(6):
            objects = list(WAREHOUSE_BUCKET.objects.filter(
                # Build a partition path for the given source and date
                Prefix=os.path.join(
                    FDNS_WAREHOUSE_S3,
                    'source={}'.format(self.dataset),
                    'year={}'.format(date.year),
                    'month={:02d}'.format(date.month),
                    'day={:02d}'.format(date.day)
                )).all())
            if len(objects) > 0:
                break
            date = date.shift(days=-1)
        else:
            logging.error('Failed to find data within the specified lookback interval.')
            raise DataNotAvailableError('Failed to find data within the specified lookback interval.')
        self.reference['reference_time_modification'] = \
            date.datetime.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        if self.dataset == 'tranco':
            self.reference['reference_url_data'] = date.strftime(TRANCO_REFERENCE_URL_DATA_FMT)
        elif self.dataset == 'umbrella':
            self.reference['reference_url_data'] = date.strftime(UMBRELLA_REFERENCE_URL_DATA_FMT)

        logging.info(f'Fetching data for {date.strftime("%Y-%m-%d")}')

        # Iterate objects in bucket with given (source, date)-partition prefix
        for i_obj in objects:

            # Open a temporary file to download the Parquet object into
            with tempfile.NamedTemporaryFile(mode='w+b',
                                             dir=TMP_DIR,
                                             prefix='{}.'.format(date.date().isoformat()),
                                             suffix='.parquet',
                                             delete=True) as tempFile:

                logging.info("Opened temporary file for object download: '{}'.".format(tempFile.name))
                WAREHOUSE_BUCKET.download_fileobj(
                    Key=i_obj.key, Fileobj=tempFile, Config=boto3.s3.transfer.TransferConfig(
                        multipart_chunksize=16 * 1024 * 1024))
                logging.info("Downloaded '{}' [{:.2f}MiB] into '{}'.".format(
                    os.path.join(S3A_OPENINTEL_ENDPOINT, WAREHOUSE_BUCKET.name, i_obj.key),
                    os.path.getsize(tempFile.name) / (1024 * 1024),
                    tempFile.name
                ))
                # Use Pandas to read file into a DF and append to list
                self.pandas_df_list.append(
                    pd.read_parquet(tempFile.name,
                                    engine='fastparquet',
                                    columns=[
                                        'query_type',
                                        'query_name',
                                        'response_type',
                                        'response_name',
                                        'ip4_address',
                                        'ip6_address',
                                        'ns_address',
                                        'cname_name',
                                    ])
                )

    @staticmethod
    def recurse_chain(current_chain: list, chain_links: dict, records: dict, state: dict):
        """Recurse CNAME chains and populate state dictionary.

        This is a depth-first search that just follows every possible chain from the
        root.
        If the current tail of the chain is an A/AAAA records, the IPs are added to all
        names currently in the chain. As a consequence, the state dict only contains
        names that resolve to at least one IP so if there is a branch that does not end
        in an IP, the corresponding names will not be in the state and can be pruned.

        Args:
            current_chain (list): List of names that form the current chain.
            chain_links (dict): Dictionary mapping names to a set of names forming the
            chain links.
            records (dict): Dictionary mapping names to a set of IPs (A/AAAA records).
            state (dict): State dictionary that will be populated by this function.
        """
        chain_tail = current_chain[-1]
        if chain_tail in records:
            for link in current_chain:
                state[link].update(records[chain_tail])
        if chain_tail in chain_links:
            for link in chain_links[chain_tail]:
                current_chain.append(link)
                OpenIntelCrawler.recurse_chain(current_chain, chain_links, records, state)
                current_chain.pop()

    def run(self):
        """Fetch the forward DNS data, populate a data frame, and process lines one by
        one."""
        attempt = 5
        self.pandas_df_list = list()  # List of Parquet file-specific Pandas DataFrames

        while len(self.pandas_df_list) == 0 and attempt > 0:
            self.get_parquet()
            attempt -= 1

        # Concatenate Parquet file-specific DFs
        pandas_df = pd.concat(self.pandas_df_list)

        # Select A, AAAA, and NS mappings from the measurement data
        df = pandas_df[
            (
                (pandas_df.query_type == 'A') |
                (pandas_df.query_type == 'AAAA') |
                (pandas_df.query_type == 'NS')

            ) &
            (
                (pandas_df.response_type == 'A') |
                (pandas_df.response_type == 'AAAA') |
                (pandas_df.response_type == 'NS') |
                (pandas_df.response_type == 'CNAME')
            ) &
            # Filter missing addresses (there is at least one...)
            (
                (pandas_df.ip4_address.notnull()) |
                (pandas_df.ip6_address.notnull()) |
                (pandas_df.ns_address.notnull()) |
                (pandas_df.cname_name.notnull())
            )
        ].drop_duplicates()
        # Remove root '.' from fields.
        df.query_name = df.query_name.str[:-1]
        df.response_name = df.response_name.str[:-1]
        df.ns_address = df.ns_address.map(lambda x: x[:-1] if x is not None else None)
        df.cname_name = df.cname_name.map(lambda x: x[:-1] if x is not None else None)

        logging.info(f'Read {len(df)} unique records from {len(self.pandas_df_list)} Parquet file(s).')

        # query_names for NS records are domain names
        domain_names = set(df[df.response_type == 'NS']['query_name'])

        # response values of NS records are name servers
        name_servers = set(df[(df.ns_address.notnull()) & (df.response_type == 'NS')]['ns_address'])

        # query_names for A and AAAA records are host names
        host_names = set(df[(df.response_type == 'A') | (df.response_type == 'AAAA')]['query_name'])

        ipv6_addresses = set()
        # Normalize IPv6 addresses.
        for ip in df[df.ip6_address.notnull()]['ip6_address']:
            try:
                ip_normalized = IPv6Address(ip).compressed
            except ValueError as e:
                logging.error(f'Ignoring invalid IPv6 address "{ip}": {e}')
                continue
            ipv6_addresses.add(ip_normalized)

        # Handle CNAME entries.
        # A query where the result is obtained via a CNAME is indicated by a
        # response name that is different from the query name. This means there will be
        # a CNAME response linking the initial query name to the cname name. However,
        # the entry with the resolved IP only contains the last entry of a potential
        # CNAME chain, so we need to check the CNAME responses as well.
        # An example CNAME chain looks like this:
        #
        #    query_type   query_name    response_type   response_name   ip4_address    cname_name    # noqa: W505
        #   ------------ ------------- --------------- --------------- ------------- --------------- # noqa: W505
        #    A            example.org   CNAME           example.org                   a.example.org  # noqa: W505
        #    A            example.org   CNAME           a.example.org                 b.example.org  # noqa: W505
        #    A            example.org   A               b.example.org     192.0.2.1
        #
        # The beginning of the chain is the CNAME entry where query_name is equal to
        # response_name. Chains can also branch out so parts of a chain resolve to
        # different IPs.
        #
        # The dataset also contains CNAME chains that do not resolve to an IP (i.e., no
        # response with type A/AAAA exists), so we need to filter these out.

        # Get query names which contain CNAMEs and resolved to an IP.
        cname_ip_records = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        for row in (
            df
            [
                (df.response_type == 'A') |
                (df.response_type == 'AAAA')
            ]
            .query('query_name != response_name')
            [[
                'response_type',
                'query_name',
                'response_name',
                'ip4_address',
                'ip6_address'
            ]]
            .drop_duplicates()
        ).itertuples():
            # There are cases where a single query name has multiple CNAME chains that
            # end in different IPs. To check if chains are valid, we need to know the
            # last entry that resolves to an IP to identify broken chains.
            if row.response_type == 'A':
                ip = row.ip4_address
            else:
                ip = IPv6Address(row.ip6_address).compressed
            cname_ip_records[row.response_type][row.query_name][row.response_name].add(ip)

        # Get the components of CNAME chains for queries that successfully resolved.
        cnames = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        # There are cases where NS queries receive a CNAME response, which we want to
        # ignore.
        for row in df[(df.query_type.isin(['A', 'AAAA'])) & (df.response_type == 'CNAME')].itertuples():
            if row.query_name not in cname_ip_records[row.query_type]:
                # No need to build chains for entries that did not resolve.
                continue
            cname_dict = cnames[row.query_type]
            # We keep the chain links as a dict and order them later since there is no
            # guarantee that our dataframe rows are ordered.
            # Links can also branch, i.e., there are two CNAME records for one response
            # name.
            cname_dict[row.query_name][row.response_name].add(row.cname_name)

        # Assemble chains.
        cname_resolves_to = defaultdict(dict)
        for query_type, chain_dict in cnames.items():
            for query_name, chain_links in chain_dict.items():
                records = cname_ip_records[query_type][query_name]
                state = defaultdict(set)
                self.recurse_chain([query_name], chain_links, records, state)
                cname_resolves_to[query_type][query_name] = state
                # Also create HostName nodes for all names that resolved.
                host_names.update(state.keys())

        # Get/create all nodes:
        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', domain_names, all=False)
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', host_names, all=False)
        ns_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', name_servers, all=False)
        self.iyp.batch_add_node_label(list(ns_id.values()), 'AuthoritativeNameServer')
        ip4_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip',
                                                         set(df[df.ip4_address.notnull()]['ip4_address']),
                                                         all=False)
        ip6_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', ipv6_addresses, all=False)

        logging.info(f'Got {len(domain_id)} domains, {len(ns_id)} nameservers, {len(host_id)} hosts, '
                     f'{len(ip4_id)} IPv4, {len(ip6_id)} IPv6')

        # Compute links
        res_links = list()
        mng_links = list()
        partof_links = list()
        aliasof_links = list()
        unique_alias = set()
        unique_res = set()
        processed_cnames = set()

        # RESOLVES_TO and MANAGED_BY links
        for row in df.itertuples():

            # NS Record
            if row.response_type == 'NS' and row.ns_address:
                domain_qid = domain_id[row.query_name]
                ns_qid = ns_id[row.ns_address]
                mng_links.append({'src_id': domain_qid, 'dst_id': ns_qid, 'props': [self.reference]})

            # A Record
            elif row.response_type == 'A' and row.ip4_address:
                host_qid = host_id[row.query_name]
                ip_qid = ip4_id[row.ip4_address]
                if row.query_name == row.response_name:
                    # A record
                    if (host_qid, ip_qid, row.response_type) not in unique_res:
                        res_links.append({'src_id': host_qid, 'dst_id': ip_qid, 'props': [
                            self.reference, {'record': row.response_type}]})
                        unique_res.add((host_qid, ip_qid, row.response_type))
                # If the final CNAME has multiple A/AAAA records, we would enter
                # this loop multiple times, so ignore for slight performance
                # optimization.
                elif (row.response_type, row.query_name) not in processed_cnames:
                    # CNAME result
                    processed_cnames.add((row.response_type, row.query_name))
                    # Add RESOLVES_TO entries
                    resolved_names = cname_resolves_to[row.response_type][row.query_name]
                    records = cname_ip_records[row.response_type][row.query_name]
                    for hostname, ips in resolved_names.items():
                        host_qid = host_id[hostname]
                        for ip in ips:
                            # Keep track of the source for the RESOLVES_TO relationship.
                            # Either CNAME if it was added transitively or A/AAAA if it
                            # is the end of the chain.
                            # A name can have both, i.e., IPs coming from a transitive
                            # CNAME and direct A/AAAA records.
                            record = 'CNAME'
                            if hostname in records and ip in records[hostname]:
                                record = row.response_type
                            ip_qid = ip4_id[ip]
                            if (host_qid, ip_qid, record) not in unique_res:
                                res_links.append({'src_id': host_qid, 'dst_id': ip_qid,
                                                 'props': [self.reference, {'record': record}]})
                                unique_res.add((host_qid, ip_qid, record))
                    # Add ALIAS_OF links for resolvable parts of the chain.
                    for source, destinations in cnames[row.response_type][row.query_name].items():
                        if source not in resolved_names:
                            # Link is not resolvable so ignore.
                            continue
                        source_qid = host_id[source]
                        for destination in destinations:
                            if destination not in resolved_names:
                                continue
                            destination_qid = host_id[destination]
                            if (source_qid, destination_qid) not in unique_alias:
                                aliasof_links.append({'src_id': source_qid,
                                                      'dst_id': destination_qid,
                                                      'props': [self.reference]})
                                unique_alias.add((source_qid, destination_qid))

            # AAAA Record
            elif row.response_type == 'AAAA' and row.ip6_address:
                try:
                    ip_normalized = IPv6Address(row.ip6_address).compressed
                except ValueError:
                    # Error message was already logged above.
                    continue
                host_qid = host_id[row.query_name]
                ip_qid = ip6_id[ip_normalized]
                if row.query_name == row.response_name:
                    # AAAA record
                    if (host_qid, ip_qid, row.response_type) not in unique_res:
                        res_links.append({'src_id': host_qid, 'dst_id': ip_qid, 'props': [
                            self.reference, {'record': row.response_type}]})
                        unique_res.add((host_qid, ip_qid, row.response_type))
                elif (row.response_type, row.query_name) not in processed_cnames:
                    processed_cnames.add((row.response_type, row.query_name))
                    # CNAME result
                    # Add RESOLVES_TO entries
                    resolved_names = cname_resolves_to[row.response_type][row.query_name]
                    records = cname_ip_records[row.response_type][row.query_name]
                    for hostname, ips in resolved_names.items():
                        host_qid = host_id[hostname]
                        for ip in ips:
                            record = 'CNAME'
                            if hostname in records and ip in records[hostname]:
                                record = row.response_type
                            ip_qid = ip6_id[ip]
                            if (host_qid, ip_qid, record) not in unique_res:
                                res_links.append({'src_id': host_qid, 'dst_id': ip_qid,
                                                 'props': [self.reference, {'record': record}]})
                                unique_res.add((host_qid, ip_qid, record))
                    # Add ALIAS_OF links for resolvable parts of the chain.
                    for source, destinations in cnames[row.response_type][row.query_name].items():
                        if source not in resolved_names:
                            # Link is not resolvable so ignore.
                            continue
                        source_qid = host_id[source]
                        for destination in destinations:
                            if destination not in resolved_names:
                                continue
                            destination_qid = host_id[destination]
                            if (source_qid, destination_qid) not in unique_alias:
                                aliasof_links.append({'src_id': source_qid,
                                                      'dst_id': destination_qid,
                                                      'props': [self.reference]})
                                unique_alias.add((source_qid, destination_qid))

        # PART_OF links between HostNames and DomainNames
        for hd in host_names.intersection(domain_names):
            partof_links.append({'src_id': host_id[hd], 'dst_id': domain_id[hd], 'props': [self.reference]})

        logging.info(f'Computed {len(res_links)} RESOLVES_TO links and {len(mng_links)} MANAGED_BY links')

        # Push all links to IYP
        self.iyp.batch_add_links('RESOLVES_TO', res_links)
        self.iyp.batch_add_links('MANAGED_BY', mng_links)
        self.iyp.batch_add_links('PART_OF', partof_links)
        self.iyp.batch_add_links('ALIAS_OF', aliasof_links)

    def unit_test(self):
        # infra_ns only has RESOLVES_TO and ALIAS_OF relationships.
        if self.reference['reference_name'] == 'openintel.infra_ns':
            return super().unit_test(['RESOLVES_TO', 'ALIAS_OF'])
        return super().unit_test(['RESOLVES_TO', 'MANAGED_BY', 'PART_OF', 'ALIAS_OF'])


class DnsgraphCrawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://dnsgraph.dacs.utwente.nl'

    @staticmethod
    def remove_root(name):
        if name == '.':
            return name
        return name.rstrip('.')

    @staticmethod
    def normalize_ipv6(address):
        if ':' in address:
            # The source data should never contain invalid IPs, so let it crash if that
            # should ever happen.
            return IPv6Address(address).compressed
        return address

    def run(self):
        # Extract current date for partitioning
        logging.info('Probing available data')
        max_lookback_in_weeks = 1
        for lookback in range(0, max_lookback_in_weeks + 1):
            current_date = datetime.now(tz=timezone.utc) - timedelta(weeks=lookback)
            year = current_date.strftime('%Y')
            week = current_date.strftime('%U')
            base_url = f'{self.reference["reference_url_data"]}/year={year}/week={week}'
            probe_url = f'{base_url}/connections.json.gz'
            if requests.head(probe_url).ok:
                logging.info(base_url)
                logging.info(f'Using year={year}/week={week} ({current_date.strftime("%Y-%m-%d")})')
                break
        else:
            logging.error('Failed to find data within the specified lookback interval.')
            raise DataNotAvailableError('Failed to find data within the specified lookback interval.')

        # Shift to Monday and set to midnight.
        mod_date = (current_date - timedelta(days=current_date.weekday())).replace(hour=0,
                                                                                   minute=0,
                                                                                   second=0,
                                                                                   microsecond=0)
        self.reference['reference_time_modification'] = mod_date

        logging.info('Reading connections')
        connections = pd.read_json(f'{base_url}/connections.json.gz', lines=True)

        logging.info('Stripping root "." and normalizing IPs')
        # Remove root "." from names that are not the root.
        # Currently there are only DOMAIN and HOSTNAME entries in from_nodeType, but
        # maybe that changes in the future.
        connections.loc[connections['from_nodeType'].isin(('DOMAIN', 'HOSTNAME')), 'from_nodeKey'] = \
            connections.loc[connections['from_nodeType'].isin(('DOMAIN', 'HOSTNAME')), 'from_nodeKey'].map(self.remove_root)  # noqa: E501
        connections.loc[connections['to_nodeType'].isin(('DOMAIN', 'HOSTNAME')), 'to_nodeKey'] = \
            connections.loc[connections['to_nodeType'].isin(('DOMAIN', 'HOSTNAME')), 'to_nodeKey'].map(self.remove_root)
        # Normalize IPv6 addresses.
        connections.loc[connections['from_nodeType'] == 'IP', 'from_nodeKey'] = \
            connections.loc[connections['from_nodeType'] == 'IP', 'from_nodeKey'].map(self.normalize_ipv6)
        connections.loc[connections['to_nodeType'] == 'IP', 'to_nodeKey'] = \
            connections.loc[connections['to_nodeType'] == 'IP', 'to_nodeKey'].map(self.normalize_ipv6)

        # Pandas' unique is faster than plain set.
        unique_domain_names = set()
        unique_host_names = set()
        unique_ips = set()
        logging.info('Getting unique nodes')
        for node_type, node_key in [('from_nodeType', 'from_nodeKey'), ('to_nodeType', 'to_nodeKey')]:
            unique_domain_names.update(connections[connections[node_type] == 'DOMAIN'][node_key].unique())
            unique_host_names.update(connections[connections[node_type] == 'HOSTNAME'][node_key].unique())
            unique_ips.update(connections[connections[node_type] == 'IP'][node_key].unique())

        domains_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', unique_domain_names)
        hosts_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', unique_host_names)
        ips_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', unique_ips)

        links_parent = list()
        links_part_of = list()
        links_alias_of = list()
        links_managed_by = list()
        links_resolves_to = list()
        unique_relationships = set()

        logging.info('Computing relationships...')
        for connection in connections.itertuples():
            relationship_tuple = (connection.relation_name,
                                  connection.from_nodeType,
                                  connection.from_nodeKey,
                                  connection.to_nodeType,
                                  connection.to_nodeKey,
                                  str(connection.properties))
            if relationship_tuple in unique_relationships:
                continue
            unique_relationships.add(relationship_tuple)
            if connection.relation_name == 'PARENT':
                links_parent.append({
                    'src_id': domains_id[connection.from_nodeKey],
                    'dst_id': domains_id[connection.to_nodeKey],
                    'props': [self.reference, connection.properties],
                })
            elif connection.relation_name == 'MANAGED_BY':
                links_managed_by.append({
                    'src_id': domains_id[connection.from_nodeKey],
                    'dst_id': hosts_id[connection.to_nodeKey],
                    'props': [self.reference, connection.properties],
                })
            elif connection.relation_name == 'PART_OF':
                links_part_of.append({
                    'src_id': hosts_id[connection.from_nodeKey],
                    'dst_id': domains_id[connection.to_nodeKey],
                    'props': [self.reference, connection.properties],
                })
            elif connection.relation_name == 'ALIAS_OF':
                links_alias_of.append({
                    'src_id': hosts_id[connection.from_nodeKey],
                    'dst_id': hosts_id[connection.to_nodeKey],
                    'props': [self.reference, connection.properties],
                })
            elif connection.relation_name == 'RESOLVES_TO':
                links_resolves_to.append({
                    'src_id': hosts_id[connection.from_nodeKey],
                    'dst_id': ips_id[connection.to_nodeKey],
                    'props': [self.reference, connection.properties],
                })
            else:
                logging.error(f'Unknown relationship type: {connection.relation_name}')

        # Push all links to IYP
        self.iyp.batch_add_links('PARENT', links_parent)
        self.iyp.batch_add_links('PART_OF', links_part_of)
        self.iyp.batch_add_links('ALIAS_OF', links_alias_of)
        self.iyp.batch_add_links('MANAGED_BY', links_managed_by)
        self.iyp.batch_add_links('RESOLVES_TO', links_resolves_to)

        # Push the Authoritative NS Label
        ns_id = [link['dst_id'] for link in links_managed_by]
        self.iyp.batch_add_node_label(ns_id, 'AuthoritativeNameServer')

    def unit_test(self):
        return super().unit_test(['PARENT', 'PART_OF', 'ALIAS_OF', 'MANAGED_BY', 'RESOLVES_TO'])

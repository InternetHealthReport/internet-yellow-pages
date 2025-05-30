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
from bs4 import BeautifulSoup

from iyp import BaseCrawler, DataNotAvailableError

# credentials
OPENINTEL_ACCESS_KEY = ''
OPENINTEL_SECRET_KEY = ''

if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    OPENINTEL_ACCESS_KEY = config['openintel']['access_key']
    OPENINTEL_SECRET_KEY = config['openintel']['secret_key']

REF_URL_DATA = 'https://openintel.nl/download/forward-dns/basis=toplist/source={dataset}/year=%Y/month=%m/day=%d'

S3A_OPENINTEL_ENDPOINT = 'https://object.openintel.nl'


class OpenIntelCrawler(BaseCrawler):
    def __init__(self, organization, url, name, dataset):
        """Initialization of the OpenIntel crawler requires the name of the dataset
        (e.g. tranco or infra:ns)."""

        self.dataset = dataset
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://openintel.nl/data/forward-dns/top-lists/'
        if dataset == 'crux':
            # We cannot link to the precise date, since the data is separated by country
            # code first.
            self.reference['reference_url_data'] = \
                'https://openintel.nl/download/forward-dns/basis=toplist/source=crux/'
        self.warehouse_bucket = None
        self.fdns_warehouse_s3 = str()
        self.create_tmp_dir()

    @staticmethod
    def fetch_crux_country_codes():
        """Fetch the list of available country codes for the CrUX dataset by scraping
        the public website."""
        r = requests.get('https://openintel.nl/download/forward-dns/basis=toplist/source=crux/',
                         cookies={'openintel-data-agreement-accepted': 'true'})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, features='html.parser')
        country_codes = list()
        for link in soup.find_all('a'):
            text = link.text.strip()
            if text.startswith('country-code'):
                country_codes.append(text.split('=')[1])
        if not country_codes:
            raise DataNotAvailableError('Failed to scrape country codes from website.')
        return country_codes

    def init_public_s3_bucket(self):
        """Initialize the S3 bucket for the public endpoint."""
        # Get a boto3 resource
        S3R_OPENINTEL = boto3.resource(
            's3',
            'nl-utwente',
            endpoint_url=S3A_OPENINTEL_ENDPOINT,
            config=botocore.config.Config(
                signature_version=botocore.UNSIGNED
            )
        )

        # Prevent some request going to AWS instead of the OpenINTEL server
        S3R_OPENINTEL.meta.client.meta.events.unregister('before-sign.s3', botocore.utils.fix_s3_host)

        # The OpenINTEL bucket
        self.warehouse_bucket = S3R_OPENINTEL.Bucket('openintel-public')

    def get_parquet_public(self):
        """Fetch and read dataframes for the specified toplist dataset from the public
        S3 bucket."""

        self.init_public_s3_bucket()
        # OpenINTEL measurement data objects base prefix
        self.fdns_warehouse_s3 = 'fdns/basis=toplist'

        self.fetch_warehouse_data()

    def get_parquet_closed(self):
        """Fetch and read dataframes for the specified dataset from the closed S3
        bucket."""

        # Get a boto3 resource
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
        self.warehouse_bucket = S3R_OPENINTEL.Bucket('openintel')

        # OpenINTEL measurement data objects base prefix
        self.fdns_warehouse_s3 = 'category=fdns/type=warehouse'

        self.fetch_warehouse_data()

    def get_parquet_crux(self):
        """Fetch and read dataframes for CRuX toplist.

        Only get data for countries available in IYP.
        """
        crux_country_codes = OpenIntelCrawler.fetch_crux_country_codes()
        logging.info(f'{len(crux_country_codes)} countries available.')

        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code')

        self.init_public_s3_bucket()

        prefix = f'fdns/basis=toplist/source={self.dataset}'
        for country_code in crux_country_codes:
            if country_code.upper() not in country_id:
                continue
            logging.info(f'Fetching {country_code.upper()}')
            self.fetch_warehouse_data(os.path.join(prefix, f'country-code={country_code}'))

    def fetch_warehouse_data(self, prefix: str = str()):
        """Fetch and read dataframes.

        Requires initialization of the S3 bucket.

        Args:
            prefix (str, optional): Custom filter prefix.
        """
        if not prefix:
            prefix = os.path.join(self.fdns_warehouse_s3, f'source={self.dataset}')
        # Get latest available data.
        date = arrow.utcnow()
        for lookback_days in range(6):
            objects = list(self.warehouse_bucket.objects.filter(
                # Build a partition path for the given source and date
                Prefix=os.path.join(
                    prefix,
                    'year={}'.format(date.year),
                    'month={:02d}'.format(date.month),
                    'day={:02d}'.format(date.day)
                )).all())
            if len(objects) > 0:
                break
            date = date.shift(days=-1)
        else:
            if self.dataset == 'crux':
                # For CRuX not all countries have lists all the time...
                logging.warning('Failed to find data within the specified lookback interval.')
                return
            logging.error('Failed to find data within the specified lookback interval.')
            raise DataNotAvailableError('Failed to find data within the specified lookback interval.')
        self.reference['reference_time_modification'] = \
            date.datetime.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        if self.dataset in ['tranco', 'umbrella']:
            # Set data URL for public datasets.
            self.reference['reference_url_data'] = date.strftime(REF_URL_DATA.format(dataset=self.dataset))

        logging.info(f'Fetching data for {date.strftime("%Y-%m-%d")}')

        # Iterate objects in bucket with given (source, date)-partition prefix
        for i_obj in objects:

            # Open a temporary file to download the Parquet object into
            with tempfile.NamedTemporaryFile(mode='w+b',
                                             dir=self.get_tmp_dir(),
                                             prefix='{}.'.format(date.date().isoformat()),
                                             suffix='.parquet',
                                             delete=False) as tempFile:
                logging.info("Opened temporary file for object download: '{}'.".format(tempFile.name))
                self.warehouse_bucket.download_fileobj(
                    Key=i_obj.key, Fileobj=tempFile,
                    Config=boto3.s3.transfer.TransferConfig(multipart_chunksize=128 * 1024 * 1024)
                )
                logging.info("Downloaded '{}' [{:.2f}MiB] into '{}'.".format(
                    os.path.join(S3A_OPENINTEL_ENDPOINT, self.warehouse_bucket.name, i_obj.key),
                    os.path.getsize(tempFile.name) / (1024 * 1024),
                    tempFile.name
                ))
                # For some files read_parquet() fails without this...
                tempFile.flush()
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

        This is a depth-first traversal that just follows every possible chain from the
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
            for record_type, ips in records[chain_tail].items():
                # The state should only contain RESOLVES_TO relationships caused by
                # CNAMEs, so ignore the end of the chain, which is the actual A/AAAA
                # record.
                for link in current_chain[:-1]:
                    state[link][record_type].update(ips)
        if chain_tail in chain_links:
            for link in chain_links[chain_tail]:
                if link in current_chain:
                    # Prevent infinite recursion due to CNAME loops.
                    continue
                current_chain.append(link)
                OpenIntelCrawler.recurse_chain(current_chain, chain_links, records, state)
                current_chain.pop()

    def run(self):
        """Fetch the forward DNS data, populate a data frame, and process lines one by
        one."""
        attempt = 5
        self.pandas_df_list = list()  # List of Parquet file-specific Pandas DataFrames

        while len(self.pandas_df_list) == 0 and attempt > 0:
            if self.dataset == 'tranco':
                self.get_parquet_public()
            elif self.dataset == 'umbrella':
                self.get_parquet_public()
            elif self.dataset == 'infra:ns':
                self.get_parquet_closed()
            elif self.dataset == 'crux':
                self.get_parquet_crux()
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
        host_names = set(df[(df.response_type == 'A') | (df.response_type == 'AAAA')]['response_name'])

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
        cname_query_names = set()
        cname_ip_records = defaultdict(lambda: defaultdict(set))
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
            cname_query_names.add(row.query_name)
            if row.response_type == 'A':
                ip = row.ip4_address
            else:
                ip = IPv6Address(row.ip6_address).compressed
            cname_ip_records[row.response_name][row.response_type].add(ip)

        # Get the components of CNAME chains for queries that successfully resolved.
        cnames = defaultdict(set)
        # There are cases where NS queries receive a CNAME response, which we want to
        # ignore.
        for row in df[(df.query_type.isin(['A', 'AAAA'])) & (df.response_type == 'CNAME')].itertuples():
            # Links can branch, i.e., there are two CNAME records for one response name,
            # so keep a set.
            cnames[row.response_name].add(row.cname_name)

        # Assemble chains.
        cname_resolves_to = defaultdict(lambda: {'A': set(), 'AAAA': set()})
        for query_name in cname_query_names:
            self.recurse_chain([query_name], cnames, cname_ip_records, cname_resolves_to)
        # Also need to create HostName nodes for all CNAME entries that resolve to an
        # IP.
        host_names.update(cname_resolves_to.keys())

        # Get/create all nodes:
        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName',
                                                            'name',
                                                            domain_names,
                                                            all=False,
                                                            batch_size=100000)
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', host_names, all=False, batch_size=100000)
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

        # RESOLVES_TO and MANAGED_BY links
        for row in df.itertuples():

            # NS Record
            if row.response_type == 'NS' and row.ns_address:
                domain_qid = domain_id[row.query_name]
                ns_qid = ns_id[row.ns_address]
                mng_links.append({'src_id': domain_qid, 'dst_id': ns_qid, 'props': [self.reference]})

            # We only add the actual A/AAAA records, for which the host name is
            # indicated by the response name. This can be different from the query name
            # in case of CNAME entries.
            # The transitive RESOLVES_TO entries caused by CNAMES are added later.
            # A Record
            elif row.response_type == 'A' and row.ip4_address:
                host_qid = host_id[row.response_name]
                ip_qid = ip4_id[row.ip4_address]
                if (host_qid, ip_qid, row.response_type) not in unique_res:
                    res_links.append({'src_id': host_qid,
                                      'dst_id': ip_qid,
                                      'props': [self.reference, {'source': row.response_type}]})
                    unique_res.add((host_qid, ip_qid, row.response_type))
            # AAAA Record
            elif row.response_type == 'AAAA' and row.ip6_address:
                try:
                    ip_normalized = IPv6Address(row.ip6_address).compressed
                except ValueError:
                    # Error message was already logged above.
                    continue
                host_qid = host_id[row.response_name]
                ip_qid = ip6_id[ip_normalized]
                if (host_qid, ip_qid, row.response_type) not in unique_res:
                    res_links.append({'src_id': host_qid,
                                      'dst_id': ip_qid,
                                      'props': [self.reference, {'source': row.response_type}]})
                    unique_res.add((host_qid, ip_qid, row.response_type))

        normal_resolve_to_links = len(res_links)

        # Process CNAMES
        # RESOLVES_TO relationships
        for hostname, entries in cname_resolves_to.items():
            host_qid = host_id[hostname]
            for response_type, ips in entries.items():
                ip_id = ip4_id if response_type == 'A' else ip6_id
                for ip in ips:
                    ip_qid = ip_id[ip]
                    if (host_qid, ip_qid, 'CNAME') not in unique_res:
                        res_links.append({'src_id': host_qid,
                                          'dst_id': ip_qid,
                                          'props': [self.reference, {'source': 'CNAME'}]})
                        unique_res.add((host_qid, ip_qid, 'CNAME'))
        # Add ALIAS_OF links for resolvable parts.
        for source, destinations in cnames.items():
            if source not in cname_resolves_to:
                # Link is not resolvable so ignore.
                continue
            source_qid = host_id[source]
            for destination in destinations:
                if destination not in cname_resolves_to and destination not in cname_ip_records:
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

        cname_resolve_to_links = len(res_links) - normal_resolve_to_links

        logging.info(f'Computed {normal_resolve_to_links} A/AAAA and {cname_resolve_to_links} CNAME RESOLVES_TO links '
                     f'and {len(mng_links)} MANAGED_BY links')

        # Push all links to IYP
        self.iyp.batch_add_links('RESOLVES_TO', res_links)
        self.iyp.batch_add_links('MANAGED_BY', mng_links)
        self.iyp.batch_add_links('PART_OF', partof_links)
        self.iyp.batch_add_links('ALIAS_OF', aliasof_links)

    def unit_test(self):
        # infra_ns and crux only have RESOLVES_TO and ALIAS_OF relationships.
        if self.reference['reference_name'] in {'openintel.infra_ns', 'openintel.crux'}:
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

    @staticmethod
    def recurse_cnames(source: str, cnames: dict, ips: set, state: dict, processed_cnames: set):
        for target in cnames[source]:
            if target in processed_cnames:
                # Prevent infinite recursion due to CNAME loops.
                continue
            processed_cnames.add(target)
            state[target].update(ips)
            DnsgraphCrawler.recurse_cnames(target, cnames, ips, state, processed_cnames)

    def run(self):
        # Extract current date for partitioning
        logging.info('Probing available data')
        if self.name == 'openintel.dnsgraph_crux':
            # CRuX data is available monthly.
            max_lookback_in_months = 2
            current_date = datetime.now(tz=timezone.utc)
            year = current_date.year
            month = current_date.month
            for lookback in range(0, max_lookback_in_months + 1):
                base_url = f'{self.reference["reference_url_data"]}/year={year}/month={month:02d}'
                probe_url = f'{base_url}/connections.json.gz'
                logging.info(probe_url)
                if requests.head(probe_url).ok:
                    logging.info(base_url)
                    logging.info(f'Using year={year}/month={month:02d}')
                    break
                month -= 1
                if month == 0:
                    month = 12
                    year -= 1
            else:
                logging.error('Failed to find data within the specified lookback interval.')
                raise DataNotAvailableError('Failed to find data within the specified lookback interval.')
            mod_date = datetime(year, month, 1, tzinfo=timezone.utc)
        else:
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

        domains_id = self.iyp.batch_get_nodes_by_single_prop('DomainName',
                                                             'name',
                                                             unique_domain_names,
                                                             all=False,
                                                             batch_size=100000)
        hosts_id = self.iyp.batch_get_nodes_by_single_prop('HostName',
                                                           'name',
                                                           unique_host_names,
                                                           all=False,
                                                           batch_size=100000)
        ips_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', unique_ips, all=False, batch_size=100000)

        links_parent = list()
        links_part_of = list()
        links_alias_of = list()
        links_managed_by = list()
        links_resolves_to = list()
        unique_relationships = set()
        resolves_to = defaultdict(set)
        cnames = defaultdict(set)

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
                # Keep reverse map of CNAMES.
                cnames[connection.to_nodeKey].add(connection.from_nodeKey)
                links_alias_of.append({
                    'src_id': hosts_id[connection.from_nodeKey],
                    'dst_id': hosts_id[connection.to_nodeKey],
                    'props': [self.reference, connection.properties],
                })
            elif connection.relation_name == 'RESOLVES_TO':
                resolves_to[connection.from_nodeKey].add(connection.to_nodeKey)
                source = 'AAAA' if ':' in connection.to_nodeKey else 'A'
                links_resolves_to.append({
                    'src_id': hosts_id[connection.from_nodeKey],
                    'dst_id': ips_id[connection.to_nodeKey],
                    'props': [self.reference, connection.properties, {'source': source}],
                })
            else:
                logging.error(f'Unknown relationship type: {connection.relation_name}')

        normal_resolve_to_links = len(links_resolves_to)

        # Start at the A/AAAA records and work backwards up to all CNAMES potentially
        # pointing to it.
        cname_resolves = defaultdict(set)
        for name, ips in resolves_to.items():
            self.recurse_cnames(name, cnames, ips, cname_resolves, {name})
        for hostname, ips in cname_resolves.items():
            host_qid = hosts_id[hostname]
            for ip in ips:
                links_resolves_to.append({
                    'src_id': host_qid,
                    'dst_id': ips_id[ip],
                    'props': [self.reference, {'source': 'CNAME'}],
                })

        cname_resolve_to_links = len(links_resolves_to) - normal_resolve_to_links
        logging.info(f'Calculated {normal_resolve_to_links} A/AAAA and '
                     f'{cname_resolve_to_links} CNAME RESOLVES_TO links')

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

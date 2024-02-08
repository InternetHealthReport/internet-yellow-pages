# Simple Python script to fetch domain name to IP address mappings from OpenINTEL data
# OpenIntelCrawler is based on code from Mattijs Jonker <m.jonker@utwente.nl>

import argparse
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from ipaddress import IPv6Address

import arrow
import boto3
import botocore
import pandas as pd
import requests

from iyp import BaseCrawler, RequestStatusError

TMP_DIR = './tmp'
os.makedirs(TMP_DIR, exist_ok=True)

# credentials
OPENINTEL_ACCESS_KEY = ''
OPENINTEL_SECRET_KEY = ''

if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    OPENINTEL_ACCESS_KEY = config['openintel']['access_key']
    OPENINTEL_SECRET_KEY = config['openintel']['secret_key']


def valid_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        msg = 'not a valid ISO 8601 date: {0!r}'.format(s)
        raise argparse.ArgumentTypeError(msg)


class OpenIntelCrawler(BaseCrawler):
    def __init__(self, organization, url, name, dataset):
        """Initialization of the OpenIntel crawler requires the name of the dataset
        (e.g. tranco or infra:ns)."""

        self.dataset = dataset
        super().__init__(organization, url, name)

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

        # check on the website if yesterday's data is available
        yesterday = arrow.utcnow().shift(days=-1)
        # FIXME Check at the proper place. Remove flake8 exception afterwards.
        # flake8: noqa
        # url = URL.format(year=yesterday.year, month=yesterday.month, day=yesterday.day)
        # try:
        #     req = requests.head(url)

        #     attempt = 3
        #     while req.status_code != 200 and attempt > 0:
        #         print(req.status_code)
        #         attempt -= 1
        #         yesterday = yesterday.shift(days=-1)
        #         url = URL.format(year=yesterday.year, month=yesterday.month, day=yesterday.day)
        #         req = requests.head(url)

        # except requests.exceptions.ConnectionError:
        #     logging.warning("Cannot reach OpenINTEL website, try yesterday's data")
        #     yesterday = arrow.utcnow().shift(days=-1)
        #     url = URL.format(year=yesterday.year, month=yesterday.month, day=yesterday.day)

        logging.warning(f'Fetching data for {yesterday}')

        # Start one day before ? # TODO remove this line?
        yesterday = yesterday.shift(days=-1)

        # Iterate objects in bucket with given (source, date)-partition prefix
        for i_obj in WAREHOUSE_BUCKET.objects.filter(
            # Build a partition path for the given source and date
            Prefix=os.path.join(
                FDNS_WAREHOUSE_S3,
                'source={}'.format(self.dataset),
                'year={}'.format(yesterday.year),
                'month={:02d}'.format(yesterday.month),
                'day={:02d}'.format(yesterday.day)
            )
        ):

            # Open a temporary file to download the Parquet object into
            with tempfile.NamedTemporaryFile(mode='w+b',
                                             dir=TMP_DIR,
                                             prefix='{}.'.format(yesterday.date().isoformat()),
                                             suffix='.parquet',
                                             delete=True) as tempFile:

                print("Opened temporary file for object download: '{}'.".format(tempFile.name))
                WAREHOUSE_BUCKET.download_fileobj(
                    Key=i_obj.key, Fileobj=tempFile, Config=boto3.s3.transfer.TransferConfig(
                        multipart_chunksize=16 * 1024 * 1024))
                print("Downloaded '{}' [{:.2f}MiB] into '{}'.".format(
                    os.path.join(S3A_OPENINTEL_ENDPOINT, WAREHOUSE_BUCKET.name, i_obj.key),
                    os.path.getsize(tempFile.name) / (1024 * 1024),
                    tempFile.name
                ))
                # Use Pandas to read file into a DF and append to list
                self.pandas_df_list.append(
                    pd.read_parquet(tempFile.name,
                                    engine='fastparquet',
                                    columns=[
                                        'query_name',
                                        'response_type',
                                        'ip4_address',
                                        'ip6_address',
                                        'ns_address'])
                )

    def run(self):
        """Fetch the forward DNS data, populate a data frame, and process lines one by
        one."""
        attempt = 5
        self.pandas_df_list = []  # List of Parquet file-specific Pandas DataFrames

        while len(self.pandas_df_list) == 0 and attempt > 0:
            self.get_parquet()
            attempt -= 1

        # Concatenate Parquet file-specific DFs
        pandas_df = pd.concat(self.pandas_df_list)

        # Select A, AAAA, and NS mappings from the measurement data
        df = pandas_df[
            (
                (pandas_df.response_type == 'A') |
                (pandas_df.response_type == 'AAAA') |
                (pandas_df.response_type == 'NS')
            ) &
            # Filter out non-apex records
            (~pandas_df.query_name.str.startswith('www.')) &
            # Filter missing addresses (there is at least one...)
            (
                (pandas_df.ip4_address.notnull()) |
                (pandas_df.ip6_address.notnull()) |
                (pandas_df.ns_address.notnull())
            )
        ][['query_name', 'response_type', 'ip4_address', 'ip6_address', 'ns_address']].drop_duplicates()
        df.query_name = df.query_name.str[:-1]  # Remove root '.'
        df.ns_address = df.ns_address.map(lambda x: x[:-1] if x is not None else None)  # Remove root '.'

        print(f'Read {len(df)} unique records from {len(self.pandas_df_list)} Parquet file(s).')

        # query_names for NS records are domain names
        domain_names = set(df[df.response_type == 'NS']['query_name'])

        # response values of NS records are name servers
        name_servers = set(df[df.ns_address.notnull()]['ns_address'])

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

        # Get/create all nodes:
        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', domain_names)
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', host_names)
        ns_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', name_servers)
        self.iyp.batch_add_node_label(list(ns_id.values()), 'AuthoritativeNameServer')
        ip4_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', set(df[df.ip4_address.notnull()]['ip4_address']))
        ip6_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', ipv6_addresses)

        print(f'Got {len(domain_id)} domains, {len(ns_id)} nameservers, {len(host_id)} hosts, {len(ip4_id)} IPv4, '
              f'{len(ip6_id)} IPv6')

        # Compute links
        res_links = []
        mng_links = []
        partof_links = []

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
                res_links.append({'src_id': host_qid, 'dst_id': ip_qid, 'props': [self.reference]})

            # AAAA Record
            elif row.response_type == 'AAAA' and row.ip6_address:
                try:
                    ip_normalized = IPv6Address(row.ip6_address).compressed
                except ValueError:
                    # Error message was already logged above.
                    continue
                host_qid = host_id[row.query_name]
                ip_qid = ip6_id[ip_normalized]
                res_links.append({'src_id': host_qid, 'dst_id': ip_qid, 'props': [self.reference]})

        # PART_OF links between HostNames and DomainNames
        for hd in host_names.intersection(domain_names):
            partof_links.append({'src_id': host_id[hd], 'dst_id': domain_id[hd], 'props': [self.reference]})

        print(f'Computed {len(res_links)} RESOLVES_TO links and {len(mng_links)} MANAGED_BY links')

        # Push all links to IYP
        self.iyp.batch_add_links('RESOLVES_TO', res_links)
        self.iyp.batch_add_links('MANAGED_BY', mng_links)
        self.iyp.batch_add_links('PART_OF', partof_links)


class DnsDependencyCrawler(BaseCrawler):

    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)

    def run(self):
        # Extract current date for partitioning
        logging.info('Probing available data')
        max_lookback_in_weeks = 1
        for lookback in range(0, max_lookback_in_weeks + 1):
            current_date = datetime.now(tz=timezone.utc) - timedelta(weeks=lookback)
            year = current_date.strftime('%Y')
            week = current_date.strftime('%U')
            base_url = f'{self.reference["reference_url"]}/year={year}/week={week}'
            probe_url = f'{base_url}/domain_nodes.json.gz'
            if requests.head(probe_url).ok:
                logging.info(f'Using year={year}/week={week} ({current_date.strftime("%Y-%m-%d")})')
                break
        else:
            logging.error('Failed to find data within the specified lookback interval.')
            raise RequestStatusError('Failed to find data within the specified lookback interval.')

        logging.info('Reading domain names')
        domains = pd.read_json(f'{base_url}/domain_nodes.json.gz', lines=True)
        logging.info('Reading host names')
        hosts = pd.read_json(f'{base_url}/host_nodes.json.gz', lines=True)
        logging.info('Reading IPs')
        ips = pd.read_json(f'{base_url}/ip_nodes.json.gz', lines=True)
        logging.info('Reading connections')
        connections = pd.read_json(f'{base_url}/connections.json.gz', lines=True)

        unique_domain_names = set(domains['name'])
        unique_host_names = set(hosts['name'])
        unique_ips = set(ips['address'])
        logging.info(f'Pushing/getting {len(unique_domain_names)} DomainName {len(unique_host_names)} HostName '
                     f'{len(unique_ips)} IP nodes...')
        domains_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', unique_domain_names)
        hosts_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', unique_host_names)
        ips_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', unique_ips)

        links_parent = list()
        links_part_of = list()
        links_alias_of = list()
        links_managed_by = list()
        links_resolves_to = list()

        logging.info('Computing relationships...')
        start_ts = datetime.now().timestamp()
        for connection in connections.itertuples():
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
        stop_ts = datetime.now().timestamp()
        logging.info(f'{stop_ts - start_ts:.2f}s elapsed')

        # Push all links to IYP
        logging.info(f'Pushing {len(links_parent)} PARENT {len(links_part_of)} PART_OF {len(links_alias_of)} ALIAS_OF '
                     f'{len(links_managed_by)} MANAGED_BY {len(links_resolves_to)} RESOLVES_TO relationships...')
        self.iyp.batch_add_links('PARENT', links_parent)
        self.iyp.batch_add_links('PART_OF', links_part_of)
        self.iyp.batch_add_links('ALIAS_OF', links_alias_of)
        self.iyp.batch_add_links('MANAGED_BY', links_managed_by)
        self.iyp.batch_add_links('RESOLVES_TO', links_resolves_to)

        # Push the Authoritative NS Label
        ns_id = [link['dst_id'] for link in links_managed_by]
        logging.info(f'Adding AuthoritativeNameServer label to {len(ns_id)} nodes')
        self.iyp.batch_add_node_label(ns_id, 'AuthoritativeNameServer')

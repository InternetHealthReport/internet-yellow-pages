# Simple Python script to fetch domain name to IP address mappings from OpenINTEL data
# Based on code from Mattijs Jonker <m.jonker@utwente.nl>

import argparse
import datetime
import json
import logging
import os
import tempfile

import arrow
import boto3
import botocore
import pandas as pd
import requests

from iyp import BaseCrawler

TMP_DIR = './tmp'
os.makedirs(TMP_DIR, exist_ok=True)

URL = 'https://data.openintel.nl/data/'
ORG = 'OpenINTEL'
NAME = 'openintel.*'

# credentials
OPENINTEL_ACCESS_KEY = ''
OPENINTEL_SECRET_KEY = ''

if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    OPENINTEL_ACCESS_KEY = config['openintel']['access_key']
    OPENINTEL_SECRET_KEY = config['openintel']['secret_key']


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, '%Y-%m-%d')
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
        url = URL.format(year=yesterday.year, month=yesterday.month, day=yesterday.day)
        try:
            req = requests.head(url)

            attempt = 3
            while req.status_code != 200 and attempt > 0:
                print(req.status_code)
                attempt -= 1
                yesterday = yesterday.shift(days=-1)
                url = URL.format(year=yesterday.year, month=yesterday.month, day=yesterday.day)
                req = requests.head(url)

        except requests.exceptions.ConnectionError:
            logging.warning("Cannot reach OpenINTEL website, try yesterday's data")
            yesterday = arrow.utcnow().shift(days=-1)
            url = URL.format(year=yesterday.year, month=yesterday.month, day=yesterday.day)

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

        # response values of NS records are names servers
        name_servers = set(df[df.ns_address.notnull()]['ns_address'])

        # query_names for A and AAAA records are host names
        host_names = set(df[(df.response_type == 'A') | (df.response_type == 'AAAA')]['query_name'])

        # Get/create all nodes:
        domain_id = self.iyp.batch_get_nodes_by_single_prop('DomainName', 'name', domain_names)
        host_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', host_names)
        ns_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', name_servers)
        self.iyp.batch_add_node_label(list(ns_id.values()), 'AuthoritativeNameServer')
        ip4_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', set(df[df.ip4_address.notnull()]['ip4_address']))
        ip6_id = self.iyp.batch_get_nodes_by_single_prop('IP', 'ip', set(df[df.ip6_address.notnull()]['ip6_address']))
        dns_qid = self.iyp.get_node('Service', {'name': 'DNS'})

        print(f'Got {len(domain_id)} domains, {len(ns_id)} nameservers, {len(host_id)} hosts, \
                {len(ip4_id)} IPv4, {len(ip6_id)} IPv6')

        # Compute links
        res_links = []
        mng_links = []
        partof_links = []
        serve_links = []

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

                # Authoritative name servers serve DNS
                if host_qid in ns_id:
                    serve_links.append({'src_id': ip_qid, 'dst_id': dns_qid, 'props': [self.reference]})

            # AAAA Record
            elif row.response_type == 'AAAA' and row.ip6_address:
                host_qid = host_id[row.query_name]
                ip_qid = ip6_id[row.ip6_address]
                res_links.append({'src_id': host_qid, 'dst_id': ip_qid, 'props': [self.reference]})

                # Authoritative name servers serve DNS
                if host_qid in ns_id:
                    serve_links.append({'src_id': ip_qid, 'dst_id': dns_qid, 'props': [self.reference]})

        # PART_OF links between HostNames and DomainNames
        for hd in host_names.intersection(domain_names):
            partof_links.append({'src_id': host_id[hd], 'dst_id': domain_id[hd], 'props': [self.reference]})

        print(f'Computed {len(res_links)} RESOLVES_TO links and {len(mng_links)} MANAGED_BY links')

        # Push all links to IYP
        self.iyp.batch_add_links('RESOLVES_TO', res_links)
        self.iyp.batch_add_links('MANAGED_BY', mng_links)
        self.iyp.batch_add_links('PART_OF', partof_links)
        self.iyp.batch_add_links('SERVE', serve_links)

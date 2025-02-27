import argparse
import bz2
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network

import boto3
import botocore
import pandas as pd

from iyp import BaseCrawler, DataNotAvailableError

URL = 'https://rir-data.org/'
ORG = 'SimulaMet'
NAME = 'simulamet.rirdata_rdns'

S3A_RIR_DATA_ENDPOINT = 'https://data.rir-data.org'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://rir-data.org/#reverse-dns'

    @staticmethod
    def __read_json(file_path):
        data = list()
        with bz2.open(file_path, 'rt') as f:
            for line in f:
                entry = json.loads(line)
                # If there are multiple sources for a prefix, they will be encoded as a
                # list of dicts, otherwise it is just a single dict.
                # To simplify the loop below, wrap single dicts in a list.
                if isinstance(entry, dict):
                    entry = [entry]
                for source_entry in entry:
                    rdns = source_entry['rdns']
                    if 'NS' not in rdns['rdatasets']:
                        continue
                    ttl = rdns['ttl']
                    source = source_entry['source']
                    for prefix in source_entry['prefixes']:
                        if not prefix:
                            continue
                        for nameserver in rdns['rdatasets']['NS']:
                            if not nameserver:
                                continue
                            data.append((nameserver, prefix, ttl, source))
        df = pd.DataFrame(data, columns=['auth_ns', 'prefix', 'ttl', 'source'])
        df.drop_duplicates(inplace=True)
        return df

    def fetch(self):
        # Modified from https://rir-data.org/pyspark-local.html
        S3R_RIR_DATA = boto3.resource(
            's3',
            'nl-utwente',
            endpoint_url=S3A_RIR_DATA_ENDPOINT,
            config=botocore.config.Config(
                signature_version=botocore.UNSIGNED,
            )
        )

        # Get its client, for lower-level actions, if needed
        S3C_RIR_DATA = S3R_RIR_DATA.meta.client
        # Prevent some request going to AWS instead of our server
        S3C_RIR_DATA.meta.events.unregister('before-sign.s3', botocore.utils.fix_s3_host)

        # The RIR data bucket
        RIR_DATA_BUCKET = S3R_RIR_DATA.Bucket('rir-data')

        # The rDNS data base prefix
        RIR_DATA_RDNS_BASE = 'rirs-rdns-formatted/type=enriched'
        # Get current date
        current_date = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        for lookback_days in range(6):
            objects = list(RIR_DATA_BUCKET.objects.filter(
                # Build a partition path for the given source and date
                Prefix=os.path.join(
                    RIR_DATA_RDNS_BASE,
                    'year={}'.format(current_date.year),
                    'month={:02d}'.format(current_date.month),
                    'day={:02d}'.format(current_date.day)
                )).all())
            if len(objects) > 0:
                break
            current_date -= timedelta(days=1)
        else:
            logging.error('Failed to find data within the specified lookback interval.')
            raise DataNotAvailableError('Failed to find data within the specified lookback interval.')
        self.reference['reference_time_modification'] = current_date

        tmp_dir = self.create_tmp_dir()

        if len(objects) > 1:
            # We always should have only one file, but the example code uses a loop.
            # Since we set the reference URL from this, warn if there are multiple
            # files.
            logging.warning('More than one object found in bucket.')

        pandas_df_list = list()
        for obj in objects:
            # Open a temporary file to download the object into
            with tempfile.NamedTemporaryFile(mode='w+b',
                                             dir=tmp_dir,
                                             prefix=current_date.strftime('%Y-%m-%d.'),
                                             suffix='.jsonl.bz2',
                                             delete=False) as tempFile:

                logging.info(f'Opened temporary file for object download: {tempFile.name}')
                RIR_DATA_BUCKET.download_fileobj(
                    Key=obj.key,
                    Fileobj=tempFile,
                    Config=boto3.s3.transfer.TransferConfig(multipart_chunksize=16 * 1024 * 1024)
                )
                data_url = os.path.join(S3A_RIR_DATA_ENDPOINT, RIR_DATA_BUCKET.name, obj.key)
                self.reference['reference_url_data'] = data_url
                logging.info("Downloaded '{}' [{:.2f}MiB] into '{}'.".format(
                    data_url,
                    os.path.getsize(tempFile.name) / (1024 * 1024),
                    tempFile.name
                ))
                # Use Pandas to read file into a DF and append to list
                pandas_df_list.append(self.__read_json(tempFile.name))

        # Concatenate object-specific DFs
        pandas_df = pd.concat(pandas_df_list)
        return pandas_df

    def run(self):
        rir_data_df = self.fetch()

        # Remove trailing root "."
        rir_data_df['auth_ns'] = rir_data_df['auth_ns'].str[:-1]
        # Normalize prefixes.
        rir_data_df.loc[:, 'prefix'] = rir_data_df.loc[:, 'prefix'].map(lambda pfx: ip_network(pfx).compressed)

        logging.info('Reading NSes')
        ns_set = set(rir_data_df['auth_ns'].unique())
        logging.info('Reading Prefixes')
        prefix_set = set(rir_data_df['prefix'].unique())

        ns_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', ns_set, all=False)
        self.iyp.batch_add_node_label(list(ns_id.values()), 'AuthoritativeNameServer')
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefix_set, all=False)
        self.iyp.batch_add_node_label(list(prefix_id.values()), 'RDNSPrefix')

        logging.info('Computing relationships')
        links_managed_by = list()
        for relationship in rir_data_df.itertuples():
            links_managed_by.append({
                'src_id': prefix_id[relationship.prefix],
                'dst_id': ns_id[relationship.auth_ns],
                'props': [self.reference, {'source': relationship.source, 'ttl': relationship.ttl}],
            })

        self.iyp.batch_add_links('MANAGED_BY', links_managed_by)

    def unit_test(self):
        return super().unit_test(['MANAGED_BY'])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    crawler = Crawler(ORG, URL, NAME)
    if args.unit_test:
        crawler.unit_test()
    else:
        crawler.run()
        crawler.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)

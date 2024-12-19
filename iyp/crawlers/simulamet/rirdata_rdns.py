import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from ipaddress import ip_network

import pyspark.sql.functions as psf
from pyspark import SparkConf
from pyspark.sql import SparkSession

from iyp import BaseCrawler, DataNotAvailableError

URL = 'https://rir-data.org/'
ORG = 'SimulaMet'
NAME = 'simulamet.rirdata_rdns'


class Crawler(BaseCrawler):
    def __init__(self, organization, url, name):
        super().__init__(organization, url, name)
        self.reference['reference_url_info'] = 'https://rir-data.org/#reverse-dns'

    # Function to load data from S3 for current or previous days

    def load_data_for_current_or_previous_days(self, spark):
        # Get current date
        current_date = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        # Attempt to load data for current date or up to 8 days back
        MAX_LOOKBACK_IN_DAYS = 8
        PATH_FMT = 's3a://rir-data/rirs-rdns-formatted/type=enriched/year=%Y/month=%m/day=%d/'
        for i in range(MAX_LOOKBACK_IN_DAYS + 1):
            date_to_load = current_date - timedelta(days=i)
            try:
                # Generate path for the given date
                path = date_to_load.strftime(PATH_FMT)

                # Try to load data
                data = spark.read.format('json').option('basePath',
                                                        's3a://rir-data/rirs-rdns-formatted/type=enriched').load(path)
                logging.info(f'Data loaded successfully for date {date_to_load}')
                self.reference['reference_time_modification'] = date_to_load
                return data
            except Exception as e:
                # Log error
                logging.info(e)
                logging.info(f'Failed to load data for date {date_to_load}. Trying previous day...')
                continue
        # If data is still not loaded after attempting for 8 days, throw an Exception
        logging.error(f'Failed to load data for current date and up to {MAX_LOOKBACK_IN_DAYS} days back.')
        raise DataNotAvailableError(f'Failed to load data for current date and up to{MAX_LOOKBACK_IN_DAYS} days back.')

    def run(self):
        # See https://rir-data.org/pyspark-local.html
        # Create Spark Config
        sparkConf = SparkConf()
        sparkConf.setMaster('local[1]')
        sparkConf.setAppName('pyspark-{}-{}'.format(os.getuid(), int(time.time())))
        # executors
        sparkConf.set('spark.executor.instances', '1')
        sparkConf.set('spark.executor.cores', '1')
        sparkConf.set('spark.executor.memory', '4G')
        sparkConf.set('spark.executor.memoryOverhead', '512M')
        # driver
        sparkConf.set('spark.driver.cores', '1')
        sparkConf.set('spark.driver.memory', '2G')

        # RIR-data.org Object Storage settings
        sparkConf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')
        sparkConf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        sparkConf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
                      'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
        sparkConf.set('spark.hadoop.fs.s3a.endpoint', 'https://data.rir-data.org')
        sparkConf.set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
        sparkConf.set('spark.hadoop.fs.s3a.signing-algorithm', 'S3SignerType')
        sparkConf.set('spark.hadoop.fs.s3a.path.style.access', 'true')
        sparkConf.set('spark.hadoop.fs.s3a.block.size', '16M')
        sparkConf.set('spark.hadoop.fs.s3a.readahead.range', '1M')
        sparkConf.set('spark.hadoop.fs.s3a.experimental.input.fadvise', 'normal')
        sparkConf.set('spark.io.file.buffer.size', '67108864')
        sparkConf.set('spark.buffer.size', '67108864')

        # Initialize our Spark Session
        spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()
        spark.sparkContext.setLogLevel('OFF')

        logging.info('Started SparkSession')

        rir_data_df = self.load_data_for_current_or_previous_days(spark)

        rir_data_df = (
            rir_data_df.withColumn('prefix', psf.explode('prefixes'))
            .withColumn('auth_ns', psf.explode('rdns.rdatasets.NS'))
            .select('auth_ns', 'prefix', psf.col('rdns.ttl').name('ttl'), 'source')
            .where("auth_ns!='' and prefix!=''").distinct().toPandas()
        )
        # Remove trailing root "."
        rir_data_df['auth_ns'] = rir_data_df['auth_ns'].str[:-1]
        # Normalize prefixes.
        rir_data_df.loc[:, 'prefix'] = rir_data_df.loc[:, 'prefix'].map(lambda pfx: ip_network(pfx).compressed)

        logging.info('Reading NSes')
        # Get unique nameservers and remove trailing root "."
        ns_set = set(rir_data_df['auth_ns'].unique())
        logging.info('Reading Prefixes')
        prefix_set = set(rir_data_df['prefix'].unique())
        spark.stop()

        ns_id = self.iyp.batch_get_nodes_by_single_prop('HostName', 'name', ns_set, all=False)
        self.iyp.batch_add_node_label(list(ns_id.values()), 'AuthoritativeNameServer')
        prefix_id = self.iyp.batch_get_nodes_by_single_prop('Prefix', 'prefix', prefix_set, all=False)

        logging.info('Computing relationship')
        links_managed_by = []
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

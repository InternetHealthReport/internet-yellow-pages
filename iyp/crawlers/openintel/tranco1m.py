# Simple Python script to fetch domain name to IP address mappings from OpenINTEL data
# Based on code from Mattijs Jonker <m.jonker@utwente.nl>

import arrow
import os
import sys
import logging
import requests
import tarfile
import datetime
import boto3
import botocore
import tempfile
import pandas as pd
import fastparquet
import argparse

from iyp import BaseCrawler

# OpenINTEL source: Forward DNS data set to use (e.g., tranco)
SOURCE = 'tranco'

TMP_DIR = './tmp'
os.makedirs(TMP_DIR, exist_ok=True)

URL = 'https://data.openintel.nl/data/tranco1m/'
ORG = 'OpenINTEL'
NAME = 'openintel.tranco1m'

def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "not a valid ISO 8601 date: {0!r}".format(s)
        raise argparse.ArgumentTypeError(msg)

class Crawler(BaseCrawler):

    def get_parquet(self):
        """Fetch the forward DNS data, populate a data frame, and process lines one by one"""

        # Get a boto3 resource
        S3A_OPENINTEL_ENDPOINT="https://object.openintel.nl"
        S3R_OPENINTEL = boto3.resource(
            "s3",
            "nl-utwente",
            endpoint_url = S3A_OPENINTEL_ENDPOINT,
            config = botocore.config.Config(
                signature_version = botocore.UNSIGNED,
            )
        )

        # Prevent some request going to AWS instead of the OpenINTEL server
        S3R_OPENINTEL.meta.client.meta.events.unregister('before-sign.s3', botocore.utils.fix_s3_host)

        # The OpenINTEL bucket
        WAREHOUSE_BUCKET=S3R_OPENINTEL.Bucket("openintel")

        # OpenINTEL measurement data objects base prefix
        FDNS_WAREHOUSE_S3 = "category=fdns/type=warehouse"


        # check on the website if today's data is available
        today = arrow.utcnow()
        url = URL.format(year=today.year, month=today.month, day=today.day)
        try:
            req = requests.head(url)

            attempt = 3
            while req.status_code != 200 and attempt > 0:
                print(req.status_code)
                attempt -= 1
                today = today.shift(days=-1)
                url = URL.format(year=today.year, month=today.month, day=today.day)
                req = requests.head(url)

        except requests.exceptions.ConnectionError:
            logging.warning("Cannot reach OpenINTEL website, try yesterday's data")
            today = arrow.utcnow().shift(days=-1)
            url = URL.format(year=today.year, month=today.month, day=today.day)

        logging.warning(f'Fetching data for {today}')

        # Start one day before ? # TODO remove this line?
        today = today.shift(days=-1)

        # Iterate objects in bucket with given (source, date)-partition prefix
        for i_obj in WAREHOUSE_BUCKET.objects.filter(
            # Build a partition path for the given source and date
            Prefix=os.path.join(
                FDNS_WAREHOUSE_S3,
                "source={}".format(SOURCE),
                "year={}".format(today.year),
                "month={:02d}".format(today.month),
                "day={:02d}".format(today.day)
            )
        ):

            # Open a temporary file to download the Parquet object into
            with tempfile.NamedTemporaryFile(mode="w+b", dir=TMP_DIR, prefix="{}.".format(today.date().isoformat()), suffix=".parquet", delete=True) as tempFile:

                print("Opened temporary file for object download: '{}'.".format(tempFile.name))
                WAREHOUSE_BUCKET.download_fileobj(Key=i_obj.key, Fileobj=tempFile, Config=boto3.s3.transfer.TransferConfig(multipart_chunksize = 16*1024*1024))
                print("Downloaded '{}' [{:.2f}MiB] into '{}'.".format(
                    os.path.join(S3A_OPENINTEL_ENDPOINT, WAREHOUSE_BUCKET.name, i_obj.key),
                    os.path.getsize(tempFile.name) / (1024*1024),
                    tempFile.name
                ))
                # Use Pandas to read file into a DF and append to list
                self.pandas_df_list.append(
                    pd.read_parquet(tempFile.name, 
                        engine="fastparquet", 
                        columns=["query_name", "response_type", "ip4_address"])
                )


    def run(self):
        """Fetch the forward DNS data, populate a data frame, and process lines one by one"""
        attempt = 5
        self.pandas_df_list = [] # List of Parquet file-specific Pandas DataFrames

        while len(self.pandas_df_list) == 0 and attempt > 0:
            self.get_parquet()
            attempt -= 1

        # Concatenate Parquet file-specific DFs
        pandas_df = pd.concat(self.pandas_df_list)

        # Select registered domain name (SLD) to IPv4 address mappings from the measurement data
        df = pandas_df[
            # IPv4 record
            (pandas_df.response_type == "A") &
            # Filter out non-apex records
            (~pandas_df.query_name.str.startswith("www."))
        ][["query_name", "ip4_address"]].drop_duplicates()
        df.query_name = df.query_name.str[:-1] # Remove root '.'

        print("Read {} unique A records from {} Parquet file(s).".format(len(df), len(self.pandas_df_list)))

        # Write to CSV file
        #sld_a_mappings.to_csv(args.out_file, sep=",", header=True, index=False)
        #print("Written results to '{}' [{:.2f}MiB].".format(args.out_file, os.path.getsize(args.out_file) / (1024 * 1024)))

        domain_id = self.iyp.batch_get_nodes('DomainName', 'name', set(df['query_name']))
        ip_id = self.iyp.batch_get_nodes('IP', 'ip', set(df['ip4_address']))

        links = []
        for ind in df.index:
            domain_qid = domain_id[ df['query_name'][ind] ] 
            ip_qid =  ip_id[ df['ip4_address'][ind] ]
            
            links.append( { 'src_id':domain_qid, 'dst_id':ip_qid, 'props':[self.reference] } )

        # Push all links to IYP
        self.iyp.batch_add_links('RESOLVES_TO', links)


if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Start: %s" % sys.argv)

    crawler = Crawler(ORG, URL, NAME)
    if len(sys.argv) == 1 and sys.argv[1] == 'unit_test':
        crawler.unit_test(logging)
    else:
        crawler.run()
        crawler.close()

    logging.info("End: %s" % sys.argv)

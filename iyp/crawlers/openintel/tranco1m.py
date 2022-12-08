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

def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "not a valid ISO 8601 date: {0!r}".format(s)
        raise argparse.ArgumentTypeError(msg)

class Crawler(BaseCrawler):

    def run(self):
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
        req = requests.head(url)
        attempt = 3
        while req.status_code != 200 and attempt > 0:
            attempt -= 1
            today = today.shift(days=-1)
            url = URL.format(year=today.year, month=today.month, day=today.day)
            req = requests.head(url)

        today = today.shift(days=-1)
        pandas_df_list = [] # List of Parquet file-specific Pandas DataFrames

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

            print('toto')
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
                pandas_df_list.append(
                    pd.read_parquet(tempFile.name, 
                        engine="fastparquet", 
                        columns=["query_name", "response_type", "ip4_address"])
                )

        # Concatenate Parquet file-specific DFs
        pandas_df = pd.concat(pandas_df_list)

        # Select registered domain name (SLD) to IPv4 address mappings from the measurement data
        df = pandas_df[
            # IPv4 record
            (pandas_df.response_type == "A") &
            # Filter out non-apex records
            (~pandas_df.query_name.str.startswith("www."))
        ][["query_name", "ip4_address"]].drop_duplicates()
        df.query_name = df.query_name.str[:-1] # Remove root '.'

        print("Read {} unique A records from {} Parquet file(s).".format(len(df), len(pandas_df_list)))

        # Write to CSV file
        #sld_a_mappings.to_csv(args.out_file, sep=",", header=True, index=False)
        #print("Written results to '{}' [{:.2f}MiB].".format(args.out_file, os.path.getsize(args.out_file) / (1024 * 1024)))

        for i, ind in enumerate(df.index):
            self.update(df['query_name'][ind], df['ip4_address'][ind])
            sys.stderr.write(f'\rProcessed {i} lines')

            # commit every 1k lines
            if i % 10000 == 0:
                self.iyp.commit()

        sys.stderr.write('\n')

    def update(self, domain, ip):

        name_qid = self.iyp.get_node('DOMAIN_NAME', {'name': domain}, create=True)
        ip_qid = self.iyp.get_node('IP', {'ip': ip, 'af': 4}, create=True)
        statements = [ [ 'RESOLVES_TO', ip_qid, self.reference ] ] # Set AS name

        # Update domain name relationships
        self.iyp.add_links(name_qid, statements)


if __name__ == '__main__':

    scriptname = sys.argv[0].replace('/','_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, 
            filename='log/'+scriptname+'.log',
            level=logging.INFO, 
            datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.info("Started: %s" % sys.argv)

    crawler = Crawler(ORG, URL)
    crawler.run()
    crawler.close()


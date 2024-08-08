import datetime
import gzip
import json
import logging
import os
import shutil
from multiprocessing import Pool

import boto3
import botocore

# Global variable required for multiprocessing.
s3 = None

PARALLEL_DOWNLOADS = 4
if os.path.exists('config.json'):
    config = json.load(open('config.json', 'r'))
    PARALLEL_DOWNLOADS = config['ooni']['parallel_downloads']


def process(params: tuple):
    """Download and extract a single file.

    Args:
        params (tuple): Object key and output file path.
    """
    key, dest_file = params
    # Download the file
    try:
        s3.download_file(key, dest_file)
    except Exception as e:
        logging.error(f'Error downloading {key}: {e}')
        return

    # Extract the .gz file
    try:
        extracted_file = dest_file.rstrip('.gz')
        with gzip.open(dest_file, 'rb') as f_in, open(extracted_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        # Delete the .gz file
        os.remove(dest_file)
    except Exception as e:
        logging.error(f'Error extracting {key}: {e}')


def download_and_extract(repo: str, tmpdir: str, test_name: str):
    """Download the last 7 days of data for the specified test from an S3 bucket into a
    temporary directory.

    Args:
        repo (str): S3 bucket
        tmpdir (str): Output directory
        test_name (str): Test name
    """
    global s3
    # Create an anonymous session
    s3 = boto3.resource(
        's3',
        region_name='ap-northeast-1',
        config=botocore.client.Config(
            signature_version=botocore.UNSIGNED
        )
    ).Bucket(repo)

    # Get the dates for the last 7 days.
    dates = [
        (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=i)
        ).strftime('%Y%m%d')
        for i in range(7)
    ]

    dest_dir = os.path.join(tmpdir, test_name)
    files = list()

    logging.info('Fetching object list...')
    # For each day, grab the objects from the S3 bucket.
    for date in dates:
        date_objects = s3.objects.filter(Prefix=f'raw/{date}/').all()
        # Filter for objects from the requested test and only fetch JSONL files.
        for object_summary in date_objects:
            key = object_summary.key
            key_split = key.split('/')
            if len(key_split) != 6:
                logging.warning(f'Malformed key: {key}')
                continue
            test = key_split[4]
            object_name = key_split[5]
            if test != test_name or not object_name.endswith('.jsonl.gz'):
                continue
            dest_file = os.path.join(dest_dir, object_name)
            files.append((key, dest_file))

    logging.info(f'Fetching {len(files)} objects with {PARALLEL_DOWNLOADS} processes in parallel...')
    if files:
        # Ensure the destination directory exists.
        os.makedirs(dest_dir, exist_ok=True)
        # Download and extract the files.
        with Pool(PARALLEL_DOWNLOADS) as p:
            p.map(process, files)

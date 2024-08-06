import logging
import os
import boto3
import gzip
import shutil
import datetime
import botocore


# list objects in directory
def list_objects(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}
    ):
        for obj in page.get("Contents", []):
            yield obj


# list subdirectories in directory
def list_directories(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="/", PaginationConfig={"PageSize": 1000}
    ):
        for common_prefix in page.get("CommonPrefixes", []):
            yield common_prefix["Prefix"]


# download and extract all of the jsonl files for the last 7 days
def download_and_extract(repo, tmpdir, test_name):
    # Create an anonymous session
    s3 = boto3.client(
        "s3",
        config=botocore.client.Config(
            signature_version=botocore.UNSIGNED, region_name="eu-central-1"
        ),
    )

    # get the dates for the last 7 days
    dates = [
        (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=i)
        ).strftime("%Y%m%d")
        for i in range(7)
    ]

    # For each day, grab the data from the S3 bucket
    for date in dates:
        # Grab initial numbered pages
        root_pages = list_directories(s3, repo, f"raw/{date}/")
        # For each numbered root page, grab the countries:
        for root_page in root_pages:
            countries = list_directories(s3, repo, root_page)
            # For each country, grab the tests
            for country in countries:
                tests = list_directories(s3, repo, country)
                # filter for the test we want
                for test in tests:
                    if test_name == test.strip("/").split("/")[-1]:
                        objects = list_objects(s3, repo, test)
                        for obj in objects:
                            # Grab only the jsonl files, ignore the tar, download to tmpdir path named after the test
                            if obj["Key"].endswith(".jsonl.gz"):
                                test_name = test.strip("/").split("/")[-1]
                                dest_dir = os.path.join(tmpdir, test_name)
                                dest_file = os.path.join(
                                    dest_dir, os.path.basename(obj["Key"])
                                )

                                # Ensure the destination directory exists
                                os.makedirs(dest_dir, exist_ok=True)

                                # Download the file
                                try:
                                    s3.download_file(repo, obj["Key"], dest_file)
                                except Exception as e:
                                    logging.error(
                                        f"Error downloading {obj['Key']}: {e}"
                                    )
                                    continue

                                # Extract the .gz file
                                try:
                                    extracted_file = dest_file.rstrip(".gz")
                                    with gzip.open(dest_file, "rb") as f_in:
                                        with open(extracted_file, "wb") as f_out:
                                            shutil.copyfileobj(f_in, f_out)

                                    # Delete the .gz file
                                    os.remove(dest_file)
                                except Exception as e:
                                    logging.error(f"Error extracting {obj['Key']}: {e}")

#!/usr/bin/python3
import sys

import boto3

EVENT_LOCATION = "/Users/frazer/moj/repos/hmpps-digital-prison-reporting-glue-poc/tests/data/demo"
TARGET_BUCKET = "dpr-demo-development-20220916083016121000000001"
TARGET_KEY = "demo/events/json"


def flip_to_s3(source_file, bucket, key):
    """

    :param source_file:
    :param bucket:
    :param key:
    :return:
    """
    s3 = boto3.client('s3')
    with open(source_file, "rb") as f:
        s3.upload_fileobj(f, bucket, key)


def start():
    filename = sys.argv[1]
    print(TARGET_BUCKET)
    print(TARGET_KEY)
    print(filename)
    source_file = EVENT_LOCATION + "/" + filename

    key = TARGET_KEY + "/" + filename

    flip_to_s3(source_file=source_file, bucket=TARGET_BUCKET, key=key)


if __name__ == "__main__":
    # from pyspark.shell import spark

    start()

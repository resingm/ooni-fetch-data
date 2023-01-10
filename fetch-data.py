#/usr/bin/env python3

# 1st party imports
from datetime import date, datetime, timedelta
import gzip

# 3rd party imports
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd

S3_BUCKET = "ooni-data-eu-fra"
S3_PREFIX = "raw"

def build_prefix(
    timestamp: datetime,
    cc: str,
    test: str = "webconnectivity",
) -> str:
    """Build the S3 prefix"""
    yyyy = f"{timestamp.year:04d}"
    mm = f"{timestamp.month:02d}"
    dd = f"{timestamp.day:02d}"
    hh = f"{timestamp.hour:02d}"
    cc = cc.upper()
    test = test.lower()
    # return f"s3://{S3_BUCKET}/{S3_PREFIX}/{yyyy}{mm}{dd}/{hh}/{cc}/{test}/"
    return f"{S3_PREFIX}/{yyyy}{mm}{dd}/{hh}/{cc}/{test}/"


def main():
    ts = datetime(year=2023, month=1, day=5, hour=10)
    s3_prefix = build_prefix(ts, "ru")

    aws_config = Config(signature_version=UNSIGNED)
    # s3 = boto3.client('s3', config=aws_config)
    # objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)

    s3 = boto3.resource('s3', config=aws_config)
    s3_bucket = s3.Bucket(S3_BUCKET)
    objs = s3_bucket.objects.filter(Prefix=s3_prefix)
    objs = filter(lambda o: o.key.endswith(".jsonl.gz"), objs)

    for o in objs:
        df = pd.read_json(
            o.get()['Body'],
            lines=True,
            compression='gzip',
        )

        print(df)

        # lines = []
        # with gzip.open(o.get()['Body']) as f:
        #     df = pd.read_
        #     lines = f.readlines()

        # print(lines[:2])
        # print(len(lines))
        # jx = o.get()
        # buf = o.get()['Body'].read()
        # print(x)
        # print(o.key)

if __name__ == "__main__":
    main()

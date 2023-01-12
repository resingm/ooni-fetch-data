#/usr/bin/env python3

# 1st party imports
from datetime import date, datetime, timedelta
import gzip
import io

# 3rd party imports
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd

S3_BUCKET = "ooni-data-eu-fra"
S3_PREFIX = "raw"

COLS = [
    # default attributes
    "data_format_version",
    "input",
    "measurement_start_time",
    "probe_asn",
    "probe_network_name",
    "resolver_asn",
    "resolver_ip",
    "resolver_network_name",
    "software_name",
    "software_version",
    # extracted attributes
    "annotations_architecture",
    "annotations_engine_name",
    "annotations_engine_version",
    "annotations_flavor",
    "annotations_network_type",
    "annotations_origin",
    "annotations_platform",
    "test_keys_agent",
    "test_keys_client_resolver",
    "test_keys_dns_experiment_failure",
    "test_keys_control_failure",
    "test_keys_control",
    "test_keys_http_experiment_failure",
    "test_keys_body_length_match",
    "test_keys_body_proportion",
    "test_keys_status_code_match",
    "test_keys_headers_match",
    "test_keys_title_match",
    "test_keys_accessible",
    "test_keys_blocking",
    "test_keys_network_events_address",
    "test_keys_network_events_failure",
    "test_keys_network_events_operation",
    "test_keys_network_events_proto",
    "test_keys_network_events_t",
    "test_keys_network_events_tags",
    "test_keys_queries_answers",
    "test_keys_queries_engine",
    "test_keys_queries_failure",
    "test_keys_queries_hostname",
    "test_keys_queries_query_type",
    "test_keys_queries_resolver_hostname",
    "test_keys_queries_resolver_port",
    "test_keys_queries_resolver_address",
    "test_keys_tcp_connect_ip",
    "test_keys_tcp_connect_port",
    "test_keys_tcp_connect_status",
    "test_keys_requests_failure",
]

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


def explode_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    _df = df
    old_shape = _df.shape
    _df = _df.explode(col, ignore_index=True)
    new_shape = _df.shape
    print(f"Explode {col}: {old_shape}  --> {new_shape}")
    return _df


def map_col(r):
    print(f"{type(r)}")

    col = r.get("test_keys_network_events")

    print(f"{type(col)}") 
    print(col)
    return r

def flatten_column(df: pd.DataFrame, col: str, attr: str, default = pd.NA) -> pd.DataFrame:
    _df = df
    _df[f"{col}_{attr}"] = _df.loc[df[col].notna()].apply(
        lambda r: r[col].get(attr, default),
        axis=1,
    )
    return _df


def prepare_df(buf: io.BytesIO, compression: str = None) -> pd.DataFrame:
    df = pd.read_json(
        buf,
        lines=True,
        compression=compression
    )

    df = flatten_column(df, 'annotations', 'architecture')
    df = flatten_column(df, 'annotations', 'engine_name')
    df = flatten_column(df, 'annotations', 'engine_version')
    df = flatten_column(df, 'annotations', 'flavor')
    df = flatten_column(df, 'annotations', 'network_type')
    df = flatten_column(df, 'annotations', 'origin')
    df = flatten_column(df, 'annotations', 'platform')

    df = flatten_column(df, 'test_keys', 'agent')
    df = flatten_column(df, 'test_keys', 'client_resolver')
    df = flatten_column(df, 'test_keys', 'dns_experiment_failure')
    df = flatten_column(df, 'test_keys', 'control_failure')
    df = flatten_column(df, 'test_keys', 'control')
    df = flatten_column(df, 'test_keys', 'http_experiment_failure')
    df = flatten_column(df, 'test_keys', 'body_length_match')
    df = flatten_column(df, 'test_keys', 'body_proportion')
    df = flatten_column(df, 'test_keys', 'status_code_match')
    df = flatten_column(df, 'test_keys', 'headers_match')
    df = flatten_column(df, 'test_keys', 'title_match')
    df = flatten_column(df, 'test_keys', 'accessible')
    df = flatten_column(df, 'test_keys', 'blocking')

    # Flatten, explode and read test_keys.network_events
    df = flatten_column(df, 'test_keys', 'network_events')
    df = explode_column(df, 'test_keys_network_events')

    df = flatten_column(df, 'test_keys_network_events', 'address')
    df = flatten_column(df, 'test_keys_network_events', 'failure')
    df = flatten_column(df, 'test_keys_network_events', 'operation')
    df = flatten_column(df, 'test_keys_network_events', 'proto')
    df = flatten_column(df, 'test_keys_network_events', 't')
    df = flatten_column(df, 'test_keys_network_events', 'tags')

    # Flatten, explode and read test_keys.queries
    df = flatten_column(df, 'test_keys', 'queries')
    df = explode_column(df, 'test_keys_queries')

    df = flatten_column(df, 'test_keys_queries', 'answers')
    df = flatten_column(df, 'test_keys_queries', 'engine')
    df = flatten_column(df, 'test_keys_queries', 'failure')
    df = flatten_column(df, 'test_keys_queries', 'hostname')
    df = flatten_column(df, 'test_keys_queries', 'query_type')
    df = flatten_column(df, 'test_keys_queries', 'resolver_hostname')
    df = flatten_column(df, 'test_keys_queries', 'resolver_port')
    df = flatten_column(df, 'test_keys_queries', 'resolver_address')

    # Flatten, explode and read test_keys.tcp_connect
    df = flatten_column(df, 'test_keys', 'tcp_connect')
    df = explode_column(df, 'test_keys_tcp_connect')

    df = flatten_column(df, 'test_keys_tcp_connect', 'ip')
    df = flatten_column(df, 'test_keys_tcp_connect', 'port')
    df = flatten_column(df, 'test_keys_tcp_connect', 'status')

    df = flatten_column(df, 'test_keys_tcp_connect_status', 'blocked')
    df = flatten_column(df, 'test_keys_tcp_connect_status', 'failure')
    df = flatten_column(df, 'test_keys_tcp_connect_status', 'success')

    # Flatten, explode and read test_keys.requests
    df = flatten_column(df, 'test_keys', 'requests')
    df = explode_column(df, 'test_keys_requests')
    df = flatten_column(df, 'test_keys_requests', 'failure')

    df = df[COLS]

    return df



def main():
    ts = datetime(year=2023, month=1, day=5, hour=10)
    s3_prefix = build_prefix(ts, "ru")

    aws_config = Config(signature_version=UNSIGNED)
    # s3 = boto3.client('s3', config=aws_config)
    # objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)

    buf = None

    with open("./data/50rows.jsonl", "rb") as f:
        buf = io.BytesIO(f.read())

    df = prepare_df(buf, compression=None)

    df.to_csv(
        "./data/50rows.csv",
        # orient="index",
        # lines=True,
    )
    return

    df.to_parquet(
        "./data/50rows.parquet",
        engine="pyarrow",
        compression="gzip",
        index=False,
    )
    # print(df)

        # o = f.read()
        

    return 

    s3 = boto3.resource('s3', config=aws_config)
    s3_bucket = s3.Bucket(S3_BUCKET)
    objs = s3_bucket.objects.filter(Prefix=s3_prefix)
    objs = filter(lambda o: o.key.endswith(".jsonl.gz"), objs)

    for o in objs:
        buf = o.get()['Body']
        print(f'handling {o.key} ...')
        df= prepare_df(buf, compression="gzip")
        del buf
        
        key = o.key.split("/")[-1].replace(".jsonl.gz", ".csv")
        df.to_csv(f"./{key}")

        # df = pd.read_json(
        #     o.get()['Body'],
        #     lines=True,
        #     compression='gzip',
        # )


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

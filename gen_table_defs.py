#!/usr/bin/env python
import io
import os
import pandas as pd
import pyarrow.parquet as pq
import yaml

from S3.S3 import S3
from S3.S3Uri import S3Uri
from S3.Config import Config
from S3.Exceptions import S3Error, S3DownloadError
from S3.Utils import formatSize, formatDateTime

import base64
base64.encodestring = base64.encodebytes


S3_ENDPOINT = os.getenv("S3_ENDPOINT")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_BUCKET_PREFIX = os.getenv("S3_BUCKET_PREFIX")
TMPDIR = os.getenv("TMPDIR")
SCHEMA_NAME = os.getenv("SCHEMA_NAME")
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
DATATYPE_INDEX = {
    "object": "VARCHAR",
    "datetime64[ns, UTC]": "TIMESTAMP",
    "float64": "DOUBLE"
}


cfg = Config()
cfg.access_key=AWS_ACCESS_KEY
cfg.secret_key=AWS_SECRET_KEY
cfg.host_base=S3_ENDPOINT
cfg.host_bucket=f"{S3_ENDPOINT}/{S3_BUCKET}/"
s3 = S3(cfg)
uri = S3Uri(f"s3://{S3_BUCKET}/{S3_BUCKET_PREFIX}")
format_string = u"%(timestamp)16s %(size)s  %(uri)s"
bucket = uri.bucket()
prefix = uri.object()
dir_str = "DIR"
data_dict = {}
partitions_str = "partitions"
schema_def = {"schema": f"{SCHEMA_NAME}", "tables": []}


def download_file(s3, uri, destination):
    dst_stream = io.open(destination, mode='wb')
    dst_stream.stream_name = destination

    try:
        try:
            response = s3.object_get(uri, dst_stream, destination, start_position = 0)
        finally:
            dst_stream.close()
    except S3DownloadError as e:
        print(u"Download of '%s' failed (Reason: %s)" % (destination, e))
        print(u"object_get failed for '%s', deleting..." % (destination,))
        os.unlink(destination)
    except S3Error as e:
        print(u"Download of '%s' failed (Reason: %s)" % (destination, e))
        print(u"object_get failed for '%s', deleting..." % (destination,))
        os.unlink(destination)
        raise


def list_bucket(cfg, s3, bucket, prefix, data_dict, location=None):
    if prefix.endswith('*'):
        prefix = prefix[:-1]
    try:
        response = s3.bucket_list(bucket, prefix = prefix, limit = cfg.limit)
    except S3Error as e:
        if e.info["Code"] in S3.codes:
            print(S3.codes[e.info["Code"]] % bucket)
        raise

    table_name = location
    for cprefix in response['common_prefixes']:
        new_dir = cprefix["Prefix"].replace(prefix, "")
        first_partition = "=" not in prefix
        is_partition = "=" in new_dir

        if is_partition:
            partition = new_dir.split("=")
            if first_partition:
                table_name = prefix
                data_dict[table_name] = {partitions_str: []}
            if partition[0] not in data_dict[table_name][partitions_str]:
                data_dict[table_name][partitions_str].append(partition[0])

        list_bucket(cfg, s3, bucket, cprefix["Prefix"], data_dict, table_name)

    for object in response["list"]:
        data_dict[table_name]["file"] = object["Key"]
        break


list_bucket(cfg, s3, bucket, prefix, data_dict)

for table_location, tdata in data_dict.items():
    if table_location.endswith("/"):
        table_location = table_location[:-1]
    table_name = table_location.replace(S3_BUCKET_PREFIX, "").replace("/", "_")
    if table_name[0].isdigit():
        table_name = "DATA_" + table_name
    filepath = tdata["file"]
    file_name = filepath.split("/")[-1]
    destination = f"{TMPDIR}/{file_name}"
    file_uri = S3Uri(f"s3://{S3_BUCKET}/{filepath}")
    download_file(s3, file_uri, destination)
    parquet_data = pq.read_table(destination)
    df = parquet_data.to_pandas()
    table_dict = {"name": table_name, "location": f"s3a://{S3_BUCKET}/{table_location}", "format": "parquet", "columns": [], "partitions": tdata[partitions_str]}
    for col in df.columns:
        data_type = df.dtypes[col]
        table_dict["columns"].append(f"{col} {DATATYPE_INDEX.get(data_type, 'VARCHAR')}")
    for col in table_dict.get("partitions", []):
        table_dict["columns"].append(f"{col} 'VARCHAR'")
    schema_def["tables"].append(table_dict)

with open(f"./{OUTPUT_FILE}", "w") as file:
    documents = yaml.dump(schema_def, file)
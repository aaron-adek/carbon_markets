"""
Shared S3 utilities for Bronze → Silver processing scripts.
"""

import io
import logging
import os
import sys

import boto3
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ingestion.batch.utils import load_config  # noqa: E402

_cfg = load_config()

S3_BUCKET: str  = _cfg["aws"]["s3_bucket"]
AWS_REGION: str = _cfg["aws"]["region"]
LOG_LEVEL: str  = _cfg["logging"]["level"]
DRY_RUN: bool   = os.environ.get("DRY_RUN", "false").lower() == "true"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

s3 = boto3.client("s3", region_name=AWS_REGION)


def list_parquet_keys(prefix: str) -> list[str]:
    """Return all .parquet object keys under a given S3 prefix."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def read_all_bronze(prefix: str, log: logging.Logger) -> pd.DataFrame:
    """Read and union all Parquet files under a Bronze S3 prefix."""
    keys = list_parquet_keys(prefix)
    if not keys:
        log.warning("No parquet files found under s3://%s/%s", S3_BUCKET, prefix)
        return pd.DataFrame()
    log.info("Reading %d parquet file(s) from %s", len(keys), prefix)
    frames = []
    for key in keys:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(resp["Body"].read())))
    return pd.concat(frames, ignore_index=True)


def write_silver(df: pd.DataFrame, key: str, log: logging.Logger) -> None:
    """Write a DataFrame to S3 Silver as Parquet (snappy compressed)."""
    if DRY_RUN:
        log.info("[DRY_RUN] Would write %d rows to s3://%s/%s", len(df), S3_BUCKET, key)
        return
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("Wrote %d rows → s3://%s/%s", len(df), S3_BUCKET, key)

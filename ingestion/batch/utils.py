"""
Shared utilities for all batch ingestion jobs.

Provides:
  - fetch_bytes       — HTTP GET returning raw bytes (for xlsx/zip)
  - fetch_csv_stream  — HTTP GET with streaming for large CSV files
  - Cleaning helpers  — _clean_money, _clean_float, _clean_int, _clean_percent
  - S3 helpers        — upload_csv, upload_parquet
                        (both accept a `source` argument, e.g. "eex" or "eutl",
                        to build the correct Bronze S3 path)
"""

import io
import logging
import os
import re
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
import requests
import yaml

log = logging.getLogger("batch_utils")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; carbon-markets-batch-ingestor/1.0)"}
REQUEST_TIMEOUT_DEFAULT = 60
REQUEST_TIMEOUT_STREAM  = 120
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

# ── Config ─────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    """
    Load config.yaml from the project root and apply env var overrides.
    Environment variables always take precedence.
    """
    config_path = Path(__file__).resolve().parent.parent.parent / "config.yaml"
    cfg: dict = {}
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}

    aws = cfg.setdefault("aws", {})
    logging_cfg = cfg.setdefault("logging", {})

    if v := os.environ.get("S3_BUCKET"):
        aws["s3_bucket"] = v
    if v := os.environ.get("AWS_DEFAULT_REGION"):
        aws["region"] = v
    if v := os.environ.get("LOG_LEVEL"):
        logging_cfg["level"] = v.upper()

    aws.setdefault("region", "us-east-1")
    aws.setdefault("s3_bucket", "")
    logging_cfg.setdefault("level", "INFO")

    return cfg

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; carbon-markets-batch-ingestor/1.0)"}
REQUEST_TIMEOUT_DEFAULT = 60
REQUEST_TIMEOUT_STREAM  = 120
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


# ── HTTP fetch ─────────────────────────────────────────────────────────────────


def fetch_bytes(url: str) -> bytes:
    """HTTP GET — returns raw bytes. Suitable for xlsx / zip files."""
    log.info("Fetching %s", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_DEFAULT)
        r.raise_for_status()
        return r.content
    except requests.RequestException as exc:
        log.error("Failed to fetch %s: %s", url, exc)
        raise


def fetch_html(url: str) -> str:
    """HTTP GET — returns decoded text. Suitable for HTML pages."""
    log.info("Fetching %s", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_DEFAULT)
        r.raise_for_status()
        return r.text
    except requests.RequestException as exc:
        log.error("Failed to fetch %s: %s", url, exc)
        raise


def fetch_csv_stream(url: str) -> bytes:
    """
    Stream-download a potentially large CSV.
    Returns raw bytes — caller reads with pd.read_csv(io.BytesIO(content)).
    """
    log.info("Streaming %s", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_STREAM, stream=True)
        r.raise_for_status()
        chunks = []
        total = 0
        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                chunks.append(chunk)
                total += len(chunk)
        log.info("Downloaded %d bytes", total)
        return b"".join(chunks)
    except requests.RequestException as exc:
        log.error("Failed to stream %s: %s", url, exc)
        raise


# ── Cleaning helpers ───────────────────────────────────────────────────────────

_NULL_VALUES = frozenset(("-", "--", "", "nan", "n/a"))


def _is_null(value) -> bool:
    return pd.isna(value) or str(value).strip().lower() in _NULL_VALUES


def _clean_money(value) -> Optional[float]:
    if _is_null(value):
        return None
    try:
        return float(re.sub(r"[€$,\s]", "", str(value)))
    except ValueError:
        return None


def _clean_float(value) -> Optional[float]:
    if _is_null(value):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _clean_int(value) -> Optional[int]:
    if _is_null(value):
        return None
    try:
        return int(float(re.sub(r"[,\s]", "", str(value))))
    except ValueError:
        return None


def _clean_percent(value) -> Optional[float]:
    if _is_null(value):
        return None
    try:
        return float(re.sub(r"[%\s]", "", str(value)))
    except ValueError:
        return None


# ── S3 upload ──────────────────────────────────────────────────────────────────


def _s3_client(region: str):
    return boto3.client("s3", region_name=region)


def upload_csv(
    df: pd.DataFrame,
    table_name: str,
    bucket: str,
    ingest_date: str,
    region: str,
    dry_run: bool,
    source: str,
) -> str:
    """
    Upload a DataFrame as CSV to S3.

    S3 key: bronze/batch/<source>/raw/<table_name>/ingest_date=<date>/<table_name>.csv

    Args:
        source: identifies the ingestor, e.g. "eex" or "eutl". Used to build
                the S3 path so each source lands in its own Bronze prefix.
    """
    key = f"bronze/batch/{source}/raw/{table_name}/ingest_date={ingest_date}/{table_name}.csv"
    body = df.to_csv(index=False).encode("utf-8")
    if dry_run:
        log.info("[DRY RUN] CSV → s3://%s/%s  (%d bytes)", bucket, key, len(body))
        return key
    log.info("Uploading CSV → s3://%s/%s", bucket, key)
    _s3_client(region).put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv")
    return key


def upload_parquet(
    df: pd.DataFrame,
    table_name: str,
    bucket: str,
    ingest_date: str,
    region: str,
    dry_run: bool,
    source: str,
) -> str:
    """
    Upload a DataFrame as Parquet to S3.

    S3 key: bronze/batch/<source>/parquet/<table_name>/ingest_date=<date>/<table_name>.parquet
    """
    key = f"bronze/batch/{source}/parquet/{table_name}/ingest_date={ingest_date}/{table_name}.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    body = buf.getvalue()
    if dry_run:
        log.info("[DRY RUN] Parquet → s3://%s/%s  (%d bytes)", bucket, key, len(body))
        return key
    log.info("Uploading Parquet → s3://%s/%s", bucket, key)
    _s3_client(region).put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream"
    )
    return key

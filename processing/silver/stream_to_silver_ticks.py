"""
Stream Bronze → Silver Intraday Ticks Transform

Reads today's Bronze tick files and materialises per-symbol intraday tick
files into Silver, so the dashboard can do a single S3 GetObject per symbol
instead of listing and reading many Bronze files.

S3 paths:
  Input:  bronze/stream/carbon_prices_raw/event_date={today}/*.parquet
  Output: silver/stream/intraday_ticks/{symbol}/date={today}.parquet

Usage:
  python processing/silver/stream_to_silver_ticks.py

Environment variables:
  S3_BUCKET — overrides config.yaml
  DRY_RUN   — set to "true" to skip S3 write and log output only
"""

import io
import logging
import os
import sys
from datetime import date, datetime, timezone

import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from processing.utils import S3_BUCKET, DRY_RUN, list_parquet_keys, write_silver, s3  # noqa: E402

log = logging.getLogger("stream_to_silver_ticks")


def load_today_bronze(trade_date: str) -> pd.DataFrame:
    prefix = f"bronze/stream/carbon_prices_raw/event_date={trade_date}/"
    keys = list_parquet_keys(prefix)
    if not keys:
        log.warning("No Bronze tick files for %s", trade_date)
        return pd.DataFrame()
    log.info("Reading %d Bronze tick file(s) for %s", len(keys), trade_date)
    frames = []
    for key in keys:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(resp["Body"].read())))
    df = pd.concat(frames, ignore_index=True)
    df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], utc=True)
    return df.sort_values("event_time_utc").reset_index(drop=True)


def main():
    today = date.today().isoformat()
    log.info("Stream Silver → Gold intraday ticks  date=%s", today)
    try:
        ticks = load_today_bronze(today)
        if ticks.empty:
            log.warning("No tick data for %s — nothing to write to Gold", today)
            return

        ticks["gold_processed_utc"] = datetime.now(tz=timezone.utc).isoformat()

        for symbol, group in ticks.groupby("symbol"):
            key = f"silver/stream/intraday_ticks/{symbol}/date={today}.parquet"
            write_silver(group.reset_index(drop=True), key, log)
            log.info("Gold ticks: %s  %d rows", symbol, len(group))

    except (BotoCoreError, ClientError) as exc:
        log.error("Stream Silver ticks transform failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

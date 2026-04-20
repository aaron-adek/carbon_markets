"""
Stream Bronze → Silver Transform (Daily OHLCV)

Reads all Finnhub stream Bronze Parquet tick files from S3 and aggregates
them into daily OHLCV bars per symbol, writing a single Silver Parquet file.

S3 paths:
  Input:  bronze/stream/carbon_prices_raw/**/*.parquet
  Output: silver/carbon_prices_ohlcv/carbon_prices_ohlcv.parquet

Usage:
  python processing/stream_to_silver.py

Environment variables:
  S3_BUCKET          — overrides config.yaml
  AWS_DEFAULT_REGION — overrides config.yaml
  LOG_LEVEL          — DEBUG | INFO | WARNING (default: INFO)
  DRY_RUN            — set to "true" to skip S3 write and log output only
"""

import logging
import os
import sys
from datetime import date, datetime, timezone

import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import read_all_bronze, write_silver  # noqa: E402

log = logging.getLogger("stream_to_silver")

BRONZE_PREFIX = "bronze/stream/carbon_prices_raw/"
SILVER_KEY    = "silver/carbon_prices_ohlcv/carbon_prices_ohlcv.parquet"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # Convert ms epoch timestamp to UTC datetime and extract trade date
    df["trade_time_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["trade_date"] = df["trade_time_utc"].dt.date

    df = df.dropna(subset=["price", "trade_date"])

    # Sort by time so open/close are correct
    df = df.sort_values("trade_time_utc")

    # VWAP = sum(price * volume) / sum(volume) per symbol per day
    df["pv"] = df["price"] * df["volume"]
    vwap = (
        df.groupby(["symbol", "trade_date"])
        .apply(lambda g: g["pv"].sum() / g["volume"].sum() if g["volume"].sum() > 0 else g["price"].mean())
        .reset_index(name="vwap")
    )

    ohlcv = (
        df.groupby(["symbol", "trade_date"])
        .agg(
            name=("name", "first"),
            asset_class=("asset_class", "first"),
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume=("volume", "sum"),
            tick_count=("price", "count"),
        )
        .reset_index()
        .merge(vwap, on=["symbol", "trade_date"], how="left")
    )

    ohlcv["silver_processed_utc"] = datetime.now(tz=timezone.utc).isoformat()

    return ohlcv.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def main():
    log.info("Stream Bronze → Silver OHLCV  date=%s", date.today().isoformat())
    try:
        bronze = read_all_bronze(BRONZE_PREFIX, log)
        silver = transform(bronze)
        if not silver.empty:
            write_silver(silver, SILVER_KEY, log)
            log.info(
                "OHLCV silver: %d rows across %d symbol(s)",
                len(silver),
                silver["symbol"].nunique(),
            )
        else:
            log.warning("OHLCV silver: no stream data yet")
    except (BotoCoreError, ClientError) as exc:
        log.error("Stream OHLCV transform failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

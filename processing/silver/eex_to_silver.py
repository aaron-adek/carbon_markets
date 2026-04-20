"""
EEX Bronze → Silver Transform

Reads all EEX Bronze Parquet files from S3, applies type casting,
deduplication, and enrichment, then writes a single Silver Parquet file.

S3 paths:
  Input:  bronze/batch/eex/parquet/eex_auctions/**/*.parquet
  Output: silver/eex_auctions/eex_auctions.parquet

Usage:
  python processing/eex_to_silver.py

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

log = logging.getLogger("eex_to_silver")

BRONZE_PREFIX = "bronze/batch/eex/parquet/eex_auctions/"
SILVER_KEY    = "silver/eex_auctions/eex_auctions.parquet"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    df["auction_date"] = pd.to_datetime(df["auction_date"], errors="coerce").dt.date

    for col in ["clearing_price_eur", "bid_to_cover_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["volume_eua", "total_bidders", "successful_bidders", "proceeds_eur"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates(subset=["auction_date", "auction_name", "contract", "country"])
    df = df.dropna(subset=["auction_date", "clearing_price_eur"])

    df["silver_processed_utc"] = datetime.now(tz=timezone.utc).isoformat()

    return df.sort_values("auction_date").reset_index(drop=True)


def main():
    log.info("EEX Bronze → Silver  date=%s", date.today().isoformat())
    try:
        bronze = read_all_bronze(BRONZE_PREFIX, log)
        silver = transform(bronze)
        if not silver.empty:
            write_silver(silver, SILVER_KEY, log)
            log.info("EEX silver: %d rows", len(silver))
        else:
            log.warning("EEX silver: no data to write")
    except (BotoCoreError, ClientError) as exc:
        log.error("EEX transform failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

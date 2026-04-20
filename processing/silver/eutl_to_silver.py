"""
EUTL Bronze → Silver Transform

Reads all EUTL Bronze Parquet files from S3, applies type casting,
deduplication, and derives net_position_eua, then writes a single Silver
Parquet file.

S3 paths:
  Input:  bronze/batch/eutl/parquet/eutl_verified_emissions/**/*.parquet
  Output: silver/eutl_verified_emissions/eutl_verified_emissions.parquet

Usage:
  python processing/eutl_to_silver.py

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

log = logging.getLogger("eutl_to_silver")

BRONZE_PREFIX = "bronze/batch/eutl/parquet/eutl_verified_emissions/"
SILVER_KEY    = "silver/eutl_verified_emissions/eutl_verified_emissions.parquet"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    for col in ["verified_emissions_tco2", "free_allocations_eua", "surrendered_eua"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Net position: surplus when surrendered > verified emissions
    df["net_position_eua"] = df["surrendered_eua"] - df["verified_emissions_tco2"]

    df = df.drop_duplicates(subset=["installation_id", "year"])
    df = df.dropna(subset=["installation_id", "year"])

    df["silver_processed_utc"] = datetime.now(tz=timezone.utc).isoformat()

    return df.sort_values(["year", "installation_id"]).reset_index(drop=True)


def main():
    log.info("EUTL Bronze → Silver  date=%s", date.today().isoformat())
    try:
        bronze = read_all_bronze(BRONZE_PREFIX, log)
        silver = transform(bronze)
        if not silver.empty:
            write_silver(silver, SILVER_KEY, log)
            log.info("EUTL silver: %d rows", len(silver))
        else:
            log.warning("EUTL silver: no data to write")
    except (BotoCoreError, ClientError) as exc:
        log.error("EUTL transform failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

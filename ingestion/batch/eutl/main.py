"""
EUTL Batch Ingestion — EU ETS Verified Emissions → S3 Bronze Layer

Downloads the EU Transaction Log (EUTL) bulk verified emissions CSV from
the EEA Industry Portal:
  https://industry.eea.europa.eu/download?format=csv&datatypes=eutl&source=eutl

Covers all ~11,000 regulated EU ETS installations (stationary + aviation)
with annual verified emissions, free allocations, and surrendered allowances
from 2005 → present.

Writes to S3 as:
  CSV     → bronze/batch/eutl/raw/eutl_verified_emissions/ingest_date=<date>/eutl_verified_emissions.csv
  Parquet → bronze/batch/eutl/parquet/eutl_verified_emissions/ingest_date=<date>/eutl_verified_emissions.parquet

Environment variables:
  S3_BUCKET          — target bucket (required unless DRY_RUN=true)
  AWS_DEFAULT_REGION — default: us-east-1
  LOG_LEVEL          — DEBUG | INFO | WARNING  (default: INFO)
  DRY_RUN            — set to "true" to skip S3 and log output only
"""

import io
import logging
import os
import re
import sys
from datetime import date, datetime, timezone

import pandas as pd
import requests
from botocore.exceptions import BotoCoreError, ClientError

# Add the shared batch utils to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils import (  # noqa: E402
    _clean_float,
    _clean_int,
    fetch_csv_stream,
    upload_csv,
    upload_parquet,
    load_config,
)

_cfg = load_config()

# ── Configuration ──────────────────────────────────────────────────────────────

S3_BUCKET: str  = _cfg["aws"]["s3_bucket"]
AWS_REGION: str = _cfg["aws"]["region"]
LOG_LEVEL: str  = _cfg["logging"]["level"]
DRY_RUN: bool   = os.environ.get("DRY_RUN", "false").lower() == "true"

INGEST_DATE: str = date.today().isoformat()

# Primary: EEA SDI catalogue direct download (2005-2025, published Apr 2026)
# Fallback: industry portal (returns 500 intermittently)
EUTL_CSV_URL = (
    "https://sdi.eea.europa.eu/data/24567358-d375-4dff-87d7-d9943c68f5e0"
)
EUTL_CSV_URL_FALLBACK = (
    "https://industry.eea.europa.eu/download"
    "?format=csv&datatypes=eutl&source=eutl"
)

TABLE_NAME = "eutl_verified_emissions"

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("eutl_loader")

# ── Column schema ──────────────────────────────────────────────────────────────

# Known EUTL column name variants → canonical name
COLUMN_MAP = {
    # Installation identifiers
    "installation_identifier":     "installation_id",
    "installation_id":             "installation_id",
    "permit_identifier":           "installation_id",
    "national_id":                 "installation_id",

    # Installation name
    "installation_name":           "installation_name",
    "name":                        "installation_name",

    # Country
    "country":                     "country_code",
    "country_code":                "country_code",
    "member_state":                "country_code",

    # Activity type
    "main_activity_type_code":     "main_activity_type_code",
    "activity_type_code":          "main_activity_type_code",
    "main_activity_type":          "main_activity_type_desc",
    "activity_type":               "main_activity_type_desc",
    "main_activity_type_description": "main_activity_type_desc",

    # Year
    "year":                        "year",
    "reporting_year":              "year",
    "compliance_year":             "year",

    # Emissions
    "verified_emissions":          "verified_emissions_tco2",
    "verified_co2_emissions":      "verified_emissions_tco2",
    "total_verified_emissions":    "verified_emissions_tco2",
    "verified_emissions_tco2":     "verified_emissions_tco2",

    # Allocations
    "free_allocations":            "free_allocations_eua",
    "allocated_allowances":        "free_allocations_eua",
    "free_allocation":             "free_allocations_eua",
    "free_allocations_eua":        "free_allocations_eua",

    # Surrendered
    "surrendered_allowances":      "surrendered_eua",
    "surrendered":                 "surrendered_eua",
    "allowances_surrendered":      "surrendered_eua",
    "surrendered_eua":             "surrendered_eua",
}

OUTPUT_COLUMNS = [
    "installation_id",
    "installation_name",
    "country_code",
    "main_activity_type_code",
    "main_activity_type_desc",
    "year",
    "verified_emissions_tco2",
    "free_allocations_eua",
    "surrendered_eua",
    "net_position_eua",
    "source_system",
    "source_url",
    "ingest_timestamp_utc",
    "ingest_date",
]


# ── Parsing ────────────────────────────────────────────────────────────────────


def _snake(col: str) -> str:
    col = str(col).strip().lower()
    col = re.sub(r"[^\w\s]", "", col)
    col = re.sub(r"\s+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def fetch_eutl_csv() -> pd.DataFrame:
    """Download the EUTL bulk CSV and return a raw DataFrame."""
    try:
        content = fetch_csv_stream(EUTL_CSV_URL)
    except Exception as primary_err:
        log.warning("Primary URL failed (%s), trying fallback URL", primary_err)
        content = fetch_csv_stream(EUTL_CSV_URL_FALLBACK)
    log.info("Reading CSV into DataFrame")
    # Try comma, fall back to semicolon (EEA sometimes uses semicolon)
    try:
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        if df.shape[1] <= 1:
            raise ValueError("Single column — likely wrong delimiter")
    except (ValueError, pd.errors.ParserError):
        log.debug("Retrying with semicolon delimiter")
        df = pd.read_csv(io.BytesIO(content), sep=";", low_memory=False)
    log.info("Raw shape: %d rows × %d columns", *df.shape)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise raw EUTL DataFrame to OUTPUT_COLUMNS schema.

    Steps:
      1. Normalise column names to snake_case
      2. Map known variants to canonical names via COLUMN_MAP
      3. Coerce types
      4. Derive net_position_eua = surrendered_eua - free_allocations_eua
      5. Add metadata columns
    """
    df = df.copy()

    # 1. Snake-case all columns
    df.columns = [_snake(c) for c in df.columns]

    # 2. Rename to canonical names
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

    # Drop entirely empty rows
    df = df.dropna(how="all")

    # 3. Coerce types
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    for col in ("verified_emissions_tco2", "free_allocations_eua", "surrendered_eua"):
        if col in df.columns:
            df[col] = df[col].apply(_clean_int)
        else:
            df[col] = None

    # 4. Derived column — positive value means the installation is a net buyer
    #    (it surrendered more than it received for free → bought on the open market)
    surr = pd.to_numeric(df.get("surrendered_eua"), errors="coerce")
    free = pd.to_numeric(df.get("free_allocations_eua"), errors="coerce")
    df["net_position_eua"] = (surr - free).where(surr.notna() & free.notna(), other=None)
    df["net_position_eua"] = df["net_position_eua"].apply(
        lambda x: int(x) if pd.notna(x) else None
    )

    # 5. Metadata
    df["source_system"]        = "eutl_eea_portal"
    df["source_url"]           = EUTL_CSV_URL
    df["ingest_timestamp_utc"] = datetime.now(tz=timezone.utc).isoformat()
    df["ingest_date"]          = INGEST_DATE

    # Ensure all output columns exist (fill missing with None)
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[OUTPUT_COLUMNS]

    log.info("Transformed shape: %d rows × %d columns", *df.shape)
    return df.reset_index(drop=True)


# ── Orchestration ──────────────────────────────────────────────────────────────


def ingest_eutl() -> pd.DataFrame:
    df_raw = fetch_eutl_csv()
    return transform(df_raw)


# ── Entry point ────────────────────────────────────────────────────────────────


def main() -> None:
    if not S3_BUCKET and not DRY_RUN:
        log.error("S3_BUCKET environment variable is not set.")
        sys.exit(1)

    log.info(
        "Starting EUTL batch ingestion  ingest_date=%s  dry_run=%s",
        INGEST_DATE, DRY_RUN,
    )

    kwargs = dict(
        table_name=TABLE_NAME,
        bucket=S3_BUCKET,
        ingest_date=INGEST_DATE,
        region=AWS_REGION,
        dry_run=DRY_RUN,
        source="eutl",
    )

    try:
        result_df   = ingest_eutl()
        raw_key    = upload_csv(result_df, **kwargs)
        bronze_key = upload_parquet(result_df, **kwargs)
    except (BotoCoreError, ClientError) as exc:
        log.error("AWS error: %s", exc)
        sys.exit(1)
    except requests.RequestException as exc:
        log.error("HTTP error: %s", exc)
        sys.exit(1)

    log.info("Ingestion complete.")
    log.info(
        "  %-35s  %6d rows  raw=%s  bronze=%s",
        TABLE_NAME, len(result_df), raw_key, bronze_key,
    )


if __name__ == "__main__":
    main()

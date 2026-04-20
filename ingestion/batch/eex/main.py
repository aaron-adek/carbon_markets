"""
EEX Batch Ingestion — EUA Primary Auction Results → S3 Bronze Layer

Downloads per-year EEX auction report xlsx files (2020 → current year).
Pre-2020 files are not available via a public direct URL.

Each file URL follows the pattern:
  https://public.eex-group.com/eex/eua-auction-report/
  emission-spot-primary-market-auction-report-<YEAR>-data.xlsx

Confirmed available: 2020, 2021, 2022, 2023, 2024, 2025, 2026.

Unions all years into a single table and writes to S3 as:
  CSV     → bronze/batch/eex/raw/eex_auctions/ingest_date=<date>/eex_auctions.csv
  Parquet → bronze/batch/eex/parquet/eex_auctions/ingest_date=<date>/eex_auctions.parquet

EEX xlsx schema (confirmed from live file, header at row index 5):
  Date, Time, Auction Name, Contract, Status,
  Auction Price €/tCO2, Auction Volume tCO2, Cover Ratio,
  Total Number of Bidders, Number of Successful Bidders, Total Revenue €,
  Country, <per-country revenue columns ...>

Environment variables:
  S3_BUCKET          — target bucket (required unless DRY_RUN=true)
  AWS_DEFAULT_REGION — default: us-east-1
  LOG_LEVEL          — DEBUG | INFO | WARNING  (default: INFO)
  DRY_RUN            — set to "true" to skip S3 and log output only
"""

import io
import logging
import os
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
    _clean_money,
    fetch_bytes,
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
CURRENT_YEAR: int = date.today().year

# Per-year xlsx are available from 2020 → present
EEX_FIRST_YEAR = 2020
EEX_BASE_URL   = (
    "https://public.eex-group.com/eex/eua-auction-report/"
    "emission-spot-primary-market-auction-report-{year}-data.xlsx"
)

TABLE_NAME = "eex_auctions"

# EEX xlsx header row (0-based). Confirmed from live file inspection:
# rows 0-4 are metadata; row 5 is the column header row.
EEX_HEADER_ROW = 5

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("eex_loader")

# ── Column schema ──────────────────────────────────────────────────────────────

# Exact EEX column name → canonical output name
# Derived from live file: header row contains these exact strings.
EEX_COLUMN_MAP = {
    "Date":                          "auction_date",
    "Auction Name":                  "auction_name",
    "Contract":                      "contract",
    "Status":                        "status",
    "Auction Price €/tCO2":          "clearing_price_eur",
    "Auction Volume tCO2":           "volume_eua",
    "Cover Ratio":                   "bid_to_cover_ratio",
    "Total Number of Bidders":       "total_bidders",
    "Number of Successful Bidders":  "successful_bidders",
    "Total Revenue €":               "proceeds_eur",
    "Country":                       "country",
}

OUTPUT_COLUMNS = [
    "auction_date",
    "auction_name",
    "contract",
    "status",
    "clearing_price_eur",
    "volume_eua",
    "bid_to_cover_ratio",
    "total_bidders",
    "successful_bidders",
    "proceeds_eur",
    "country",
    "source_year",
    "source_system",
    "source_url",
    "ingest_timestamp_utc",
    "ingest_date",
]

# ── Parsing ────────────────────────────────────────────────────────────────────


def parse_eex_xlsx(content: bytes, source_url: str) -> pd.DataFrame:
    """
    Parse a single EEX auction report xlsx.

    Header is at row index EEX_HEADER_ROW (confirmed 5).
    Only the columns in EEX_COLUMN_MAP are retained; all per-country
    revenue columns are dropped.
    """
    log.info("Parsing xlsx from %s  (%d bytes)", source_url, len(content))

    df = pd.read_excel(
        io.BytesIO(content),
        header=EEX_HEADER_ROW,
        engine="openpyxl",
    )
    df = df.dropna(how="all")

    # Keep only the columns we care about (drop per-country revenue columns)
    keep = [c for c in df.columns if c in EEX_COLUMN_MAP]
    if not keep:
        log.warning("No recognised columns found in %s — skipping", source_url)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = df[keep].rename(columns=EEX_COLUMN_MAP)

    # ── Coerce types ─────────────────────────────────────────────────────────
    df["auction_date"] = pd.to_datetime(df["auction_date"], errors="coerce").dt.date
    df = df[df["auction_date"].notna()]

    for col, fn in [
        ("clearing_price_eur", _clean_money),
        ("proceeds_eur",       _clean_money),
        ("volume_eua",         _clean_int),
        ("total_bidders",      _clean_int),
        ("successful_bidders", _clean_int),
        ("bid_to_cover_ratio", _clean_float),
    ]:
        if col in df.columns:
            df[col] = df[col].apply(fn)

    # ── Metadata ─────────────────────────────────────────────────────────────
    df["source_year"]          = df["auction_date"].apply(lambda d: d.year if d else None)
    df["source_system"]        = "eex_auction_report"
    df["source_url"]           = source_url
    df["ingest_timestamp_utc"] = datetime.now(tz=timezone.utc).isoformat()
    df["ingest_date"]          = INGEST_DATE

    # Ensure all output columns present
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    log.info("Parsed %d rows from %s", len(df), source_url)
    return df[OUTPUT_COLUMNS].reset_index(drop=True)


# ── Fetch + parse orchestration ────────────────────────────────────────────────


def ingest_eex() -> pd.DataFrame:
    frames = []

    for year in range(EEX_FIRST_YEAR, CURRENT_YEAR + 1):
        url = EEX_BASE_URL.format(year=year)
        try:
            content = fetch_bytes(url)
        except requests.RequestException:
            log.warning("Skipping year %d — file not available", year)
            continue
        df = parse_eex_xlsx(content, source_url=url)
        if not df.empty:
            frames.append(df)

    if not frames:
        log.error("No data parsed from any EEX source — aborting")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)

    # Deduplicate — keep most recent ingest per auction_date + auction_name
    df = df.sort_values("ingest_timestamp_utc").drop_duplicates(
        subset=["auction_date", "auction_name"], keep="last"
    ).sort_values("auction_date").reset_index(drop=True)

    log.info("Total rows after deduplication: %d", len(df))

    return df


# ── Entry point ────────────────────────────────────────────────────────────────


def main() -> None:
    if not S3_BUCKET and not DRY_RUN:
        log.error("S3_BUCKET environment variable is not set.")
        sys.exit(1)

    log.info(
        "Starting EEX batch ingestion  ingest_date=%s  dry_run=%s",
        INGEST_DATE, DRY_RUN,
    )

    kwargs = dict(
        table_name=TABLE_NAME,
        bucket=S3_BUCKET,
        ingest_date=INGEST_DATE,
        region=AWS_REGION,
        dry_run=DRY_RUN,
        source="eex",
    )

    try:
        result_df = ingest_eex()
        raw_key = upload_csv(result_df, **kwargs)
        bronze_key = upload_parquet(result_df, **kwargs)
    except (BotoCoreError, ClientError) as exc:
        log.error("AWS error: %s", exc)
        sys.exit(1)
    except requests.RequestException as exc:
        log.error("HTTP error: %s", exc)
        sys.exit(1)

    log.info("Ingestion complete.")
    log.info(
        "  %-25s  %4d rows  raw=%s  bronze=%s",
        TABLE_NAME, len(result_df), raw_key, bronze_key,
    )


if __name__ == "__main__":
    main()

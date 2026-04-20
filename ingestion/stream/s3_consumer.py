"""
Kafka → S3 Consumer

Consumes trade ticks from the Kafka topic `carbon.prices.raw` and
micro-batches them to S3 Bronze as Parquet, flushing every FLUSH_INTERVAL_S
seconds (default: 60).

S3 path:
  bronze/stream/carbon_prices_raw/event_date=<YYYY-MM-DD>/
      carbon_prices_raw_<timestamp>.parquet

Configuration loaded from config.yaml (project root).
Environment variables override config.yaml values when set.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(__file__))
from utils import load_config  # noqa: E402

_cfg = load_config()

# ── Configuration ──────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP: str  = _cfg["kafka"]["bootstrap"]
KAFKA_TOPIC: str      = _cfg["kafka"]["topic"]
KAFKA_GROUP_ID: str   = _cfg["kafka"]["group_id"]
S3_BUCKET: str        = _cfg["aws"]["s3_bucket"]
AWS_REGION: str       = _cfg["aws"]["region"]
FLUSH_INTERVAL_S: int = _cfg["stream"]["flush_interval_s"]
LOG_LEVEL: str        = _cfg["logging"]["level"]

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("s3_consumer")

# ── S3 ─────────────────────────────────────────────────────────────────────────

s3 = boto3.client("s3", region_name=AWS_REGION)


def flush_to_s3(records: list[dict]) -> str:
    """Write a batch of tick records to S3 as Parquet. Returns the S3 key."""
    df = pd.DataFrame(records)

    # Partition by the event date of the first record in the batch
    event_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    if "event_time_utc" in df.columns and not df["event_time_utc"].isna().all():
        try:
            event_date = pd.to_datetime(
                df["event_time_utc"].iloc[0]
            ).strftime("%Y-%m-%d")
        except Exception:
            pass

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = (
        f"bronze/stream/carbon_prices_raw/"
        f"event_date={event_date}/"
        f"carbon_prices_raw_{ts}.parquet"
    )

    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("Flushed %d records → s3://%s/%s", len(records), S3_BUCKET, key)
    return key


# ── Graceful shutdown ──────────────────────────────────────────────────────────

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received — stopping after current flush")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ── Main loop ──────────────────────────────────────────────────────────────────


def main() -> None:
    if not S3_BUCKET:
        log.error("S3_BUCKET environment variable is not set.")
        sys.exit(1)

    log.info(
        "Starting S3 consumer  topic=%s  broker=%s  flush_interval=%ds  bucket=%s",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP, FLUSH_INTERVAL_S, S3_BUCKET,
    )

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=1000,  # allows flush loop to run even when idle
        )
    except KafkaError as exc:
        log.error("Failed to connect to Kafka: %s", exc)
        sys.exit(1)

    buffer: list[dict] = []
    last_flush = time.monotonic()

    try:
        while not _shutdown:
            # Poll for new messages (times out after consumer_timeout_ms)
            for message in consumer:
                buffer.append(message.value)
                log.debug(
                    "Buffered tick: %s @ %s  (buffer size: %d)",
                    message.value.get("symbol"),
                    message.value.get("price"),
                    len(buffer),
                )
                if _shutdown:
                    break

            # Flush if interval has elapsed and buffer is non-empty
            elapsed = time.monotonic() - last_flush
            if elapsed >= FLUSH_INTERVAL_S and buffer:
                try:
                    flush_to_s3(buffer)
                except (BotoCoreError, ClientError) as exc:
                    log.error("S3 flush failed: %s", exc)
                else:
                    buffer.clear()
                last_flush = time.monotonic()

    finally:
        # Final flush on shutdown
        if buffer:
            log.info("Flushing %d remaining records before exit", len(buffer))
            try:
                flush_to_s3(buffer)
            except Exception as exc:
                log.error("Final flush failed: %s", exc)
        consumer.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    main()

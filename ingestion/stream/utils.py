"""
Shared utilities for stream ingestion.

Provides:
  - load_config — loads config.yaml from project root with env var overrides
"""

import os
from pathlib import Path

import yaml


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
    kafka = cfg.setdefault("kafka", {})
    stream = cfg.setdefault("stream", {})
    secrets = cfg.setdefault("secrets", {})
    logging_cfg = cfg.setdefault("logging", {})

    if v := os.environ.get("S3_BUCKET"):
        aws["s3_bucket"] = v
    if v := os.environ.get("AWS_DEFAULT_REGION"):
        aws["region"] = v
    if v := os.environ.get("KAFKA_BOOTSTRAP"):
        kafka["bootstrap"] = v
    if v := os.environ.get("KAFKA_TOPIC"):
        kafka["topic"] = v
    if v := os.environ.get("KAFKA_GROUP_ID"):
        kafka["group_id"] = v
    if v := os.environ.get("FLUSH_INTERVAL_S"):
        stream["flush_interval_s"] = int(v)
    if v := os.environ.get("PROXY_SYMBOLS"):
        stream["symbols"] = [s.strip() for s in v.split(",") if s.strip()]
    if v := os.environ.get("FINNHUB_SECRET_NAME"):
        secrets["finnhub_secret_name"] = v
    if v := os.environ.get("LOG_LEVEL"):
        logging_cfg["level"] = v.upper()

    # Defaults
    aws.setdefault("region", "us-east-1")
    aws.setdefault("s3_bucket", "")
    kafka.setdefault("bootstrap", "localhost:9092")
    kafka.setdefault("topic", "carbon.prices.raw")
    kafka.setdefault("group_id", "s3-consumer")
    stream.setdefault("flush_interval_s", 60)
    stream.setdefault("symbols", ["KRBN", "KCCA", "ICLN"])
    secrets.setdefault("finnhub_secret_name", "carbon-markets/finnhub-api-key")
    logging_cfg.setdefault("level", "INFO")

    return cfg

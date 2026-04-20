"""
Finnhub WebSocket → Kafka Producer

Subscribes to Finnhub trade stream for carbon-linked ETF symbols and
publishes each tick as a JSON message to a Kafka topic.

Symbols (free tier):
  KRBN  — KraneShares Global Carbon Strategy ETF
  KCCA  — KraneShares California Carbon Allowance ETF
  ICLN  — iShares Global Clean Energy ETF

Market hours gate: 09:15–16:15 ET. Outside market hours the producer
sleeps and reconnects when the market opens.

Back-off: exponential 5s → 60s on connection errors.
Graceful shutdown: SIGINT / SIGTERM flushes the Kafka producer before exit.
Stdout fallback: if Kafka is unavailable the tick is logged to stdout so
the process can still run standalone for testing.

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
from zoneinfo import ZoneInfo

import boto3
import websocket

sys.path.insert(0, os.path.dirname(__file__))
from utils import load_config  # noqa: E402

_cfg = load_config()

# ── Configuration ──────────────────────────────────────────────────────────────

PROXY_SYMBOLS: list[str] = _cfg["stream"]["symbols"]
KAFKA_BOOTSTRAP: str     = _cfg["kafka"]["bootstrap"]
KAFKA_TOPIC: str         = _cfg["kafka"]["topic"]
FINNHUB_SECRET_NAME: str = _cfg["secrets"]["finnhub_secret_name"]
AWS_REGION: str          = _cfg["aws"]["region"]
LOG_LEVEL: str           = _cfg["logging"]["level"]


def _load_finnhub_api_key() -> str:
    """Fetch API key from AWS Secrets Manager."""
    try:
        client = boto3.client("secretsmanager", region_name=AWS_REGION)
        response = client.get_secret_value(SecretId=FINNHUB_SECRET_NAME)
        return response["SecretString"]
    except Exception as exc:
        log.error("Failed to fetch secret %s: %s", FINNHUB_SECRET_NAME, exc)
        sys.exit(1)

# Market hours gate (ET)
MARKET_OPEN  = (9, 15)   # 09:15 ET
MARKET_CLOSE = (16, 15)  # 16:15 ET
ET = ZoneInfo("America/New_York")

# Reconnect back-off
BACKOFF_MIN = 5
BACKOFF_MAX = 60

# Symbol metadata for enriching tick messages
SYMBOL_META = {
    "KRBN": {"name": "KraneShares Global Carbon Strategy ETF",        "asset_class": "carbon_etf"},
    "KCCA": {"name": "KraneShares California Carbon Allowance ETF",   "asset_class": "carbon_etf"},
    "ICLN": {"name": "iShares Global Clean Energy ETF",               "asset_class": "clean_energy_etf"},
    "KEUA": {"name": "KraneShares European Carbon Allowance ETF",     "asset_class": "carbon_etf"},
    "GRN":  {"name": "iPath Series B Carbon ETN",                     "asset_class": "carbon_etn"},
}

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("proxy_producer")

# ── Kafka ──────────────────────────────────────────────────────────────────────

_kafka_producer = None


def _get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is not None:
        return _kafka_producer
    try:
        from kafka import KafkaProducer
        _kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        log.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)
    except Exception as exc:
        log.warning("Kafka unavailable (%s) — stdout fallback active", exc)
        _kafka_producer = None
    return _kafka_producer


def _publish(message: dict) -> None:
    producer = _get_kafka_producer()
    if producer:
        producer.send(KAFKA_TOPIC, message)
    else:
        print(json.dumps(message), flush=True)


# ── Market hours ───────────────────────────────────────────────────────────────

def _is_market_open() -> bool:
    now_et = datetime.now(tz=ET)
    # Skip weekends
    if now_et.weekday() >= 5:
        return False
    t = (now_et.hour, now_et.minute)
    return MARKET_OPEN <= t < MARKET_CLOSE


def _seconds_until_open() -> float:
    """Seconds until next market open (ET). Returns 0 if already open."""
    now_et = datetime.now(tz=ET)
    # Build today's open time
    open_today = now_et.replace(
        hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0
    )
    if now_et < open_today and now_et.weekday() < 5:
        return (open_today - now_et).total_seconds()
    # Next weekday open
    days_ahead = 1
    while True:
        candidate = open_today.replace(
            day=(now_et.replace(day=now_et.day + days_ahead)).day
        )
        # Simple: just sleep 60s and recheck rather than computing exact delta
        return 60.0


# ── WebSocket handlers ─────────────────────────────────────────────────────────

def _on_open(ws):
    import time
    log.info("WebSocket connected — subscribing to %s", PROXY_SYMBOLS)
    for symbol in PROXY_SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        time.sleep(0.3)


def _on_message(ws, raw: str):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        log.debug("Non-JSON message: %s", raw)
        return

    msg_type = msg.get("type")

    if msg_type == "trade":
        for tick in msg.get("data", []):
            symbol = tick.get("s", "")
            meta = SYMBOL_META.get(symbol, {})
            enriched = {
                "symbol":          symbol,
                "name":            meta.get("name", ""),
                "asset_class":     meta.get("asset_class", ""),
                "price":           tick.get("p"),
                "volume":          tick.get("v"),
                "timestamp_ms":    tick.get("t"),
                "event_time_utc":  datetime.now(tz=timezone.utc).isoformat(),
                "source_system":   "finnhub_ws",
            }
            _publish(enriched)
            log.debug("Published tick: %s @ %.4f", symbol, tick.get("p", 0))

    elif msg_type == "ping":
        ws.send(json.dumps({"type": "pong"}))

    elif msg_type == "error":
        log.error("Finnhub error: %s", msg.get("msg"))


_rate_limit_reset_ts: float = 0.0


def _on_error(ws, error):
    global _rate_limit_reset_ts
    log.error("WebSocket error: %s", error)
    err_str = str(error)
    if "429" in err_str and "x-ratelimit-reset" in err_str:
        import re as _re
        m = _re.search(r"'x-ratelimit-reset':\s*'(\d+)'", err_str)
        if m:
            _rate_limit_reset_ts = float(m.group(1))
            log.warning("Rate limited by Finnhub — will wait until reset ts %s", _rate_limit_reset_ts)


def _on_close(ws, close_status_code, close_msg):
    log.info("WebSocket closed (%s): %s", close_status_code, close_msg)


# ── Graceful shutdown ──────────────────────────────────────────────────────────

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received — stopping")
    _shutdown = True
    if _kafka_producer:
        _kafka_producer.flush(timeout=10)
        _kafka_producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ── Main loop ──────────────────────────────────────────────────────────────────

def main() -> None:
    api_key = _load_finnhub_api_key()
    if not api_key:
        log.error("No Finnhub API key found.")
        sys.exit(1)

    finnhub_ws_url = f"wss://ws.finnhub.io?token={api_key}"

    log.info(
        "Starting proxy producer  symbols=%s  topic=%s  broker=%s",
        PROXY_SYMBOLS, KAFKA_TOPIC, KAFKA_BOOTSTRAP,
    )

    backoff = BACKOFF_MIN

    while not _shutdown:
        if not _is_market_open():
            log.info("Market closed — sleeping 60s")
            time.sleep(60)
            continue

        log.info("Connecting to Finnhub WebSocket")
        ws = websocket.WebSocketApp(
            finnhub_ws_url,
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )

        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            log.error("Unexpected error: %s", exc)

        if _shutdown:
            break

        # Honour Finnhub rate-limit reset before reconnecting
        if _rate_limit_reset_ts:
            wait = max(0.0, _rate_limit_reset_ts - time.time()) + 2.0
            log.info("Rate limit cooldown — waiting %.0fs", wait)
            time.sleep(wait)
            backoff = BACKOFF_MIN
        else:
            log.info("Reconnecting in %ds", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)


if __name__ == "__main__":
    main()

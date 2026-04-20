#!/usr/bin/env bash
# start_stream.sh
#
# Sets up and starts the full carbon markets stream pipeline on the Pi:
#   1. Starts Kafka (KRaft mode) if not already running
#   2. Creates the Kafka topic if it doesn't exist
#   3. Starts the Finnhub → Kafka producer in the background
#   4. Starts the Kafka → S3 consumer in the background
#
# Usage:
#   chmod +x pi/start_stream.sh
#   ./pi/start_stream.sh
#
# Logs are written to:
#   ~/carbon_markets/logs/producer.log
#   ~/carbon_markets/logs/consumer.log
#   ~/carbon_markets/logs/kafka.log

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
KAFKA_DIR="$HOME/kafka"
KAFKA_TOPIC="carbon.prices.raw"
LOG_DIR="$REPO_DIR/logs"
VENV="$REPO_DIR/.venv"

mkdir -p "$LOG_DIR"

# ── Helpers ────────────────────────────────────────────────────────────────────

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

require() {
    if ! command -v "$1" &>/dev/null; then
        echo "ERROR: '$1' not found. $2"
        exit 1
    fi
}

# ── Checks ─────────────────────────────────────────────────────────────────────

require java "Install with: sudo apt install -y openjdk-21-jre-headless"
require python3 "Install with: sudo apt install -y python3"

if [[ ! -d "$KAFKA_DIR" ]]; then
    echo "ERROR: Kafka not found at $KAFKA_DIR"
    echo "Run the Pi setup steps in the README first."
    exit 1
fi

if [[ ! -d "$VENV" ]]; then
    log "Creating Python virtual environment..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q --upgrade pip
fi

log "Installing/updating Python dependencies..."
"$VENV/bin/pip" install -q -r "$REPO_DIR/ingestion/stream/requirements.txt"
"$VENV/bin/pip" install -q -r "$REPO_DIR/ingestion/batch/requirements.txt"
"$VENV/bin/pip" install -q -r "$REPO_DIR/processing/requirements.txt"

# ── Kafka ──────────────────────────────────────────────────────────────────────

if ! "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list &>/dev/null 2>&1; then
    log "Starting Kafka..."
    export KAFKA_HEAP_OPTS="-Xmx1G -Xms512M"
    "$KAFKA_DIR/bin/kafka-server-start.sh" \
        "$KAFKA_DIR/config/kraft/server.properties" \
        >> "$LOG_DIR/kafka.log" 2>&1 &
    KAFKA_PID=$!
    log "Kafka PID: $KAFKA_PID"

    # Wait for Kafka to be ready
    log "Waiting for Kafka to be ready..."
    for i in $(seq 1 30); do
        if "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list &>/dev/null 2>&1; then
            break
        fi
        sleep 2
    done
else
    log "Kafka already running"
fi

# ── Create topic if missing ────────────────────────────────────────────────────

if ! "$KAFKA_DIR/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep -q "^${KAFKA_TOPIC}$"; then

    log "Creating topic: $KAFKA_TOPIC"
    "$KAFKA_DIR/bin/kafka-topics.sh" \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "$KAFKA_TOPIC" \
        --partitions 1 \
        --replication-factor 1
else
    log "Topic '$KAFKA_TOPIC' already exists"
fi

# ── Producer ───────────────────────────────────────────────────────────────────

log "Starting Finnhub producer..."
"$VENV/bin/python" "$REPO_DIR/ingestion/stream/proxy_producer.py" \
    >> "$LOG_DIR/producer.log" 2>&1 &
PRODUCER_PID=$!
log "Producer PID: $PRODUCER_PID"

# ── Consumer ───────────────────────────────────────────────────────────────────

log "Starting S3 consumer..."
"$VENV/bin/python" "$REPO_DIR/ingestion/stream/s3_consumer.py" \
    >> "$LOG_DIR/consumer.log" 2>&1 &
CONSUMER_PID=$!
log "Consumer PID: $CONSUMER_PID"

# ── Done ───────────────────────────────────────────────────────────────────────

log "Stream pipeline started."
log "  Producer → $LOG_DIR/producer.log  (PID $PRODUCER_PID)"
log "  Consumer → $LOG_DIR/consumer.log  (PID $CONSUMER_PID)"
log "  Kafka    → $LOG_DIR/kafka.log"
log ""
log "To follow logs:"
log "  tail -f $LOG_DIR/producer.log"
log "  tail -f $LOG_DIR/consumer.log"
log ""
log "To stop everything:"
log "  kill $PRODUCER_PID $CONSUMER_PID"

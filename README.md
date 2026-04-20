# Carbon Markets Data Pipeline

An end-to-end data engineering pipeline ingesting EU carbon markets data from two authoritative primary sources, enriched with a real-time ETF proxy stream. Built as a Bloomberg interview project demonstrating multi-source ingestion, schema conformation, cloud storage on AWS S3, and a live Streamlit dashboard.

---

## Data Sources

| Source | Type | Cadence | What it provides |
|---|---|---|---|
| **EEX EUA Auctions** | Batch (xlsx download) | ~Twice weekly | Official EU Allowance clearing prices & volumes from 2020 → present |
| **EUTL Verified Emissions** | Batch (CSV download) | Annual | Per-installation EU ETS verified emissions, free allocations, surrendered allowances |
| **Finnhub WebSocket** | Stream (WebSocket) | Real-time (market hours) | Intraday price signal via KRBN / KCCA / ICLN / KEUA / GRN ETFs — proxy for EUA futures |

EEX provides the authoritative EU carbon price. EUTL provides demand-side fundamentals — the emissions data that drives that price. The Finnhub stream provides an intraday signal between quarterly auctions.

---

## Architecture

```
EEX   (xlsx download) ─────┐
EUTL  (CSV download)  ─────┼──► S3 Bronze (raw) ──► S3 Silver (conformed) ──► Streamlit Dashboard
                           │                                                   (Pi + Cloudflare Tunnel)
Finnhub WebSocket ─► Kafka ─┘
(KRBN / KCCA / ICLN / KEUA / GRN)
```

### Batch — GitHub Actions (scheduled)
EEX and EUTL ingestors run on GitHub Actions. No always-on infrastructure required for batch.

### Stream — Raspberry Pi 8GB (always-on)
Apache Kafka (KRaft mode, v3.9.2) and the Finnhub WebSocket producer run as systemd services on a Raspberry Pi. The producer gates on US market hours (09:15–16:15 ET) and reconnects automatically with exponential back-off. The S3 consumer micro-batches ticks to S3 Bronze every 60 seconds. A systemd timer runs the Silver transform every 60 seconds.

### Dashboard
A Bloomberg-themed Streamlit dashboard reads directly from S3 Silver and is hosted on the Pi, exposed publicly via a Cloudflare Tunnel (HTTPS). It auto-refreshes every 60 seconds.

---

## Repository Structure

```
ingestion/
  batch/
    eex/               EEX EUA auction ingestor
      main.py
    eutl/              EUTL verified emissions ingestor
      main.py
    utils.py           Shared: HTTP fetch, cleaning helpers, S3 upload, config loader
    requirements.txt
  stream/
    proxy_producer.py  Finnhub WebSocket → Kafka producer
    s3_consumer.py     Kafka → S3 Bronze consumer (60s micro-batch)
    utils.py
    requirements.txt
infrastructure/
  iam/                 Terraform — least-privilege IAM user + policy
    main.tf
    variables.tf
    outputs.tf
  pi/                  systemd service units + setup script for the Raspberry Pi
    kafka.service
    producer.service
    consumer.service
    stream_to_silver.service
    stream_to_silver.timer
    stream_to_silver_ticks.service
    stream_to_silver_ticks.timer
    dashboard.service
    cloudflared.service
    setup.sh           One-shot bootstrap: venv, dependencies, systemd install
    start_stream.sh
processing/
  utils.py             Shared S3 helpers for Silver transforms
  silver/
    eex_to_silver.py         EEX Bronze → Silver
    eutl_to_silver.py        EUTL Bronze → Silver
    stream_to_silver.py      Stream ticks → daily OHLCV + VWAP Silver
    stream_to_silver_ticks.py  Stream ticks → per-symbol intraday tick files (Silver)
  requirements.txt
dashboard/
  app.py               Streamlit dashboard (Bloomberg dark theme)
  requirements.txt
.github/
  workflows/
    batch_ingest.yml   Scheduled GitHub Actions for EEX + EUTL
config.yaml            Project-wide configuration (region, bucket, Kafka, symbols)
```

---

## S3 Layer Structure

```
bronze/
  batch/
    eex/
      raw/eex_auctions/ingest_date=<date>/eex_auctions.csv
      parquet/eex_auctions/ingest_date=<date>/eex_auctions.parquet
    eutl/
      raw/eutl_verified_emissions/ingest_date=<date>/eutl_verified_emissions.csv
      parquet/eutl_verified_emissions/ingest_date=<date>/eutl_verified_emissions.parquet
  stream/
    carbon_prices_raw/event_date=<date>/carbon_prices_raw_<timestamp>.parquet
silver/
  eex_auctions/eex_auctions.parquet
  carbon_prices_ohlcv/carbon_prices_ohlcv.parquet          (daily OHLCV + VWAP per symbol)
  stream/intraday_ticks/<symbol>/date=<date>.parquet       (per-symbol intraday tick file, 1 object/symbol/day)
  eutl_verified_emissions/eutl_verified_emissions.parquet
```

---

## Configuration

All configuration lives in `config.yaml` at the project root.

```yaml
aws:
  profile: "carbon markets ingestor"
  region: us-east-1
  s3_bucket: carbon-markets-bucket

kafka:
  bootstrap: localhost:9092
  topic: carbon.prices.raw
  group_id: s3-consumer

stream:
  flush_interval_s: 60
  symbols: [KRBN, KCCA, ICLN, KEUA, GRN]

secrets:
  finnhub_secret_name: carbon-markets/finnhub-api-key

logging:
  level: INFO
```

The Finnhub API key is stored in **AWS Secrets Manager** under `carbon-markets/finnhub-api-key` and fetched at runtime — never stored in config files or environment variables.

---

## Quick Start

### Batch ingestors (local dry run)

```bash
pip install -r ingestion/batch/requirements.txt

DRY_RUN=true python ingestion/batch/eex/main.py
DRY_RUN=true python ingestion/batch/eutl/main.py
```

### Silver transforms (run once after batch ingest)

```bash
pip install -r processing/requirements.txt

python processing/silver/eex_to_silver.py
python processing/silver/stream_to_silver.py
python processing/silver/stream_to_silver_ticks.py   # intraday ticks, run during/after market hours
```

### Dashboard (local)

```bash
pip install -r dashboard/requirements.txt
cd dashboard && streamlit run app.py
```

---

## Infrastructure

IAM is managed via Terraform. The `carbon-markets-ingestor` user has least-privilege access:
- `s3:PutObject` / `s3:GetObject` on `bronze/*` and `silver/*`
- `secretsmanager:GetSecretValue` on `carbon-markets/finnhub-api-key`
- `secretsmanager:GetSecretValue` on `carbon-markets/cloudflare-tunnel-token`

```bash
cd infrastructure/iam
terraform init
terraform apply

terraform output access_key_id
terraform output -raw secret_access_key
```

---

## Scheduling

| Job | Runner | Trigger |
|---|---|---|
| `ingestion/batch/eex/main.py` | GitHub Actions | Cron: 07:00 UTC Tue + Thu |
| `ingestion/batch/eutl/main.py` | GitHub Actions | Cron: 06:00 UTC 1 April annually |
| `ingestion/stream/proxy_producer.py` | Pi systemd | On boot, always-on |
| `ingestion/stream/s3_consumer.py` | Pi systemd | On boot, always-on |
| `processing/silver/stream_to_silver.py` | Pi systemd timer | Every 60 seconds |
| `processing/silver/stream_to_silver_ticks.py` | Pi systemd timer | Every 60 seconds |
| `dashboard/app.py` | Pi systemd | On boot, always-on |

GitHub Actions secrets required: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `S3_BUCKET`.

---

## Raspberry Pi Setup

All service unit files and the bootstrap script live in `infrastructure/pi/`. Assumes a fresh Raspberry Pi OS (64-bit) with network access.

#### 1. Install system dependencies

```bash
sudo apt update && sudo apt install -y openjdk-21-jre-headless python3 python3-venv git unzip curl
```

#### 2. Install Kafka

```bash
wget https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz
tar -xzf kafka_2.13-3.9.2.tgz && mv kafka_2.13-3.9.2 ~/kafka
rm kafka_2.13-3.9.2.tgz

echo 'export KAFKA_HEAP_OPTS="-Xmx1G -Xms512M"' >> ~/.bashrc && source ~/.bashrc
```

#### 3. Initialise KRaft storage (once only)

```bash
UUID=$(~/kafka/bin/kafka-storage.sh random-uuid)
~/kafka/bin/kafka-storage.sh format -t $UUID -c ~/kafka/config/kraft/server.properties
```

#### 4. Install the AWS CLI

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o awscliv2.zip
unzip awscliv2.zip && sudo ./aws/install
rm -rf awscliv2.zip aws/
```

#### 5. Install cloudflared

```bash
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64 \
  -o cloudflared
chmod +x cloudflared && sudo mv cloudflared /usr/local/bin/cloudflared
```

#### 6. Clone the repository

```bash
git clone https://github.com/<your-username>/carbon_markets.git ~/carbon_markets
```

#### 7. Configure AWS credentials

```bash
mkdir -p ~/.aws
# Paste credentials from: terraform output access_key_id / secret_access_key
nano ~/.aws/credentials   # [default] aws_access_key_id / aws_secret_access_key
nano ~/.aws/config        # [default] region = us-east-1
```

#### 8. Run setup.sh

```bash
cd ~/carbon_markets
bash infrastructure/pi/setup.sh
```

This will:
- Create the Python virtual environment at `.venv`
- Install all dependencies from all `requirements.txt` files
- Copy all systemd service/timer units to `/etc/systemd/system/`
- Enable and start all services (Kafka, producer, consumer, stream_to_silver, dashboard, cloudflared)
- Print the public dashboard URL once the Cloudflare tunnel is up

#### 9. Verify

```bash
sudo systemctl status kafka producer consumer stream_to_silver stream_to_silver_ticks dashboard cloudflared

journalctl -u dashboard -f
journalctl -u cloudflared -n 20
```

---

## Key Design Decisions

**Why KRBN/KCCA/ICLN/KEUA/GRN as a stream proxy?**
No free real-time EUA price feed exists. KRBN directly tracks EUA + CCUS carbon futures and is available free on Finnhub's WebSocket tier.

**Why VWAP in Silver?**
Volume-Weighted Average Price is a standard execution quality metric. Computing it at the Silver layer (price × volume at tick level, then summed per day) avoids losing the information once ticks are aggregated to OHLCV.

**Why AWS Secrets Manager for the Finnhub key?**
The key is fetched at runtime by the Pi using the `carbon-markets-ingestor` IAM user. It never touches the filesystem, environment variables, or config files — eliminating the risk of accidental exposure in logs or git history.

**Why Raspberry Pi for streaming?**
Running Kafka and a WebSocket producer 24/7 on an EC2 instance costs ~$15–30/month. The Pi 8GB handles it at zero marginal cost once owned.

**Why GitHub Actions for batch?**
EEX auctions twice a week, EUTL once a year. Total runtime under 5 minutes each. GitHub Actions free tier (2,000 min/month) handles this with zero infrastructure overhead and a visible run history.

**Why Cloudflare Tunnel for the dashboard?**
Exposes the Pi-hosted Streamlit app over public HTTPS with no open ports, no domain required (quick tunnel), and no cost. The tunnel token is stored in AWS Secrets Manager — not in the service file.

**Why pre-compute `proceeds_eur` in Silver?**
Auction proceeds (`clearing_price_eur × volume_eua`) are a stable derived fact, not a display choice. Computing them once in `eex_to_silver.py` means the dashboard reads a column and plots — no arithmetic at render time, and the value is available to any downstream consumer of the Silver file.

**Why consolidate intraday ticks in Silver (`stream_to_silver_ticks.py`)?**
Each 60-second consumer flush writes a separate Bronze Parquet file. By the end of a trading day that's ~95 objects per symbol. The tick transform merges them into a single Silver object per symbol per day, reducing dashboard S3 read calls from O(N) to O(1) and making the tick chart load in a single `GetObject`.

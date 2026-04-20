#!/usr/bin/env bash
# setup.sh — Bootstrap or update carbon_markets on a Raspberry Pi
# Usage: bash setup.sh
# Re-run after every git pull to sync dependencies and reload services.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
PYTHON="${VENV_DIR}/bin/python"
PIP="${VENV_DIR}/bin/pip"

echo "==> Project: $PROJECT_DIR"

# ── 1. Create venv if it doesn't exist ──────────────────────────────────────
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo "==> Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

echo "==> Upgrading pip..."
"$PIP" install --quiet --upgrade pip

# ── 2. Install all requirements ──────────────────────────────────────────────
REQUIREMENTS=(
    ingestion/stream/requirements.txt
    ingestion/batch/requirements.txt
    processing/requirements.txt
    dashboard/requirements.txt
)

for req in "${REQUIREMENTS[@]}"; do
    if [ -f "$PROJECT_DIR/$req" ]; then
        echo "==> Installing $req..."
        "$PIP" install --quiet -r "$PROJECT_DIR/$req"
    fi
done

echo "==> All Python dependencies installed."

# ── 3. Install cloudflared if missing ───────────────────────────────────────
if ! command -v cloudflared &>/dev/null; then
    echo "==> Installing cloudflared..."
    curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg \
        | sudo tee /usr/share/keyrings/cloudflare-main.gpg > /dev/null
    echo 'deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main' \
        | sudo tee /etc/apt/sources.list.d/cloudflared.list
    sudo apt-get update -qq && sudo apt-get install -y -q cloudflared
    echo "==> cloudflared installed."
else
    echo "==> cloudflared already installed: $(cloudflared --version 2>&1 | head -1)"
fi

# ── 4. Install / reload systemd services (only if running as a user with sudo) ─
SERVICES_DIR="$PROJECT_DIR/infrastructure/pi"
CF_PID="0"
if [ -d "$SERVICES_DIR" ] && command -v systemctl &>/dev/null; then
    echo "==> Installing systemd services from $SERVICES_DIR..."
    for unit in "$SERVICES_DIR"/*.service "$SERVICES_DIR"/*.timer; do
        [ -f "$unit" ] || continue
        name="$(basename "$unit")"
        dest="/etc/systemd/system/$name"
        if [ ! -f "$dest" ] || ! diff -q "$unit" "$dest" &>/dev/null; then
            echo "    Copying $name -> $dest"
            sudo cp "$unit" "$dest"
        fi
    done

    sudo systemctl daemon-reload

    # Remove any stale systemctl edit overrides that could interfere
    for svc in kafka producer consumer stream_to_silver dashboard cloudflared; do
        override_dir="/etc/systemd/system/${svc}.service.d"
        if [ -d "$override_dir" ]; then
            echo "    Removing stale overrides for $svc..."
            sudo rm -rf "$override_dir"
        fi
    done

    sudo systemctl daemon-reload

    CF_PID=""

    # Enable and (re)start services that should always run
    for svc in kafka producer consumer stream_to_silver dashboard cloudflared; do
        service_file="/etc/systemd/system/${svc}.service"
        [ -f "$service_file" ] || continue
        sudo systemctl enable --quiet "$svc"
        if sudo systemctl is-active --quiet "$svc"; then
            sudo systemctl restart "$svc"
            echo "    Restarted $svc"
        else
            sudo systemctl start "$svc"
            echo "    Started $svc"
        fi
        if [ "$svc" = "cloudflared" ]; then
            sleep 2
            CF_PID=$(sudo systemctl show cloudflared --property=MainPID --value 2>/dev/null || echo "0")
        fi
    done

    # Enable timers separately
    for timer in "$SERVICES_DIR"/*.timer; do
        [ -f "$timer" ] || continue
        name="$(basename "$timer")"
        sudo systemctl enable --quiet "$name"
        sudo systemctl start "$name" 2>/dev/null || true
        echo "    Timer enabled: $name"
    done
fi

echo ""
echo "==> Setup complete."
echo "    Activate venv manually with:  source .venv/bin/activate"
echo "    Or run scripts directly with: .venv/bin/python <script.py>"

# ── Extract and display the Cloudflare tunnel URL ─────────────────────────────
if command -v systemctl &>/dev/null; then
    echo ""
    echo "==> Waiting for Cloudflare tunnel URL..."
    TUNNEL_URL=""
    for i in $(seq 1 20); do
        if [ -n "$CF_PID" ] && [ "$CF_PID" != "0" ]; then
            TUNNEL_URL=$(sudo journalctl -u cloudflared -n 200 --no-pager -l 2>/dev/null \
                | grep "cloudflared\[$CF_PID\]" \
                | grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' | tail -1 || true)
        fi
        [ -n "$TUNNEL_URL" ] && break
        sleep 2
    done

    echo ""
    if [ -n "$TUNNEL_URL" ]; then
        echo "╔══════════════════════════════════════════════════════════╗"
        echo "║  DASHBOARD URL (share this with your team):              ║"
        printf "║  %-56s  ║\n" "$TUNNEL_URL"
        echo "╚══════════════════════════════════════════════════════════╝"
    else
        echo "    Could not detect tunnel URL yet. Run:"
        echo "    sudo journalctl -u cloudflared -n 50 | grep trycloudflare"
    fi
fi

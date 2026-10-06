#!/usr/bin/env sh
set -e

echo "[INFO] Starting Zigma Aerio 300 Bridge..."

if [ -f "/data/options.json" ]; then
    echo "[INFO] Running as Home Assistant Add-on (/data/options.json found)"
fi

exec python3 /app/zigma_bridge.py

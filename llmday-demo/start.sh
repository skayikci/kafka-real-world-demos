#!/bin/bash
set -eo pipefail
# start.sh — run the demo

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🐳 Ensuring Docker stack is up..."
docker compose up -d 2>&1 | grep -v "^$" || true

echo "🔌 Registering Debezium connector..."
bash "$SCRIPT_DIR/register-connector.sh"

echo "🚀 Starting demo..."
exec "$SCRIPT_DIR/.venv/bin/python" -u consumer.py

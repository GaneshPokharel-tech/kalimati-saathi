#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CRON_FILE="$ROOT_DIR/ops/kalimati_crontab.txt"

if [ ! -f "$CRON_FILE" ]; then
  echo "Crontab file not found: $CRON_FILE"
  exit 1
fi

echo "Installing crontab from: $CRON_FILE"
crontab "$CRON_FILE"

echo
echo "Current crontab:"
crontab -l

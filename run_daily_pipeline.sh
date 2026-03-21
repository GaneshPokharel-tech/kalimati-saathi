#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

mkdir -p logs
LOG_FILE="logs/kalimati_pipeline_$(date +%F).log"
BRIEF_FILE="data/processed/kalimati_market_brief.md"

{
  echo "============================================================"
  echo "Pipeline started at $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  .venv/bin/python run_daily_pipeline.py

  echo
  echo "-------------------- MARKET BRIEF SNAPSHOT --------------------"
  if [ -f "$BRIEF_FILE" ]; then
    sed -n '1,80p' "$BRIEF_FILE"
  else
    echo "Market brief file not found: $BRIEF_FILE"
  fi

  echo
  echo "Pipeline finished at $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
} | tee -a "$LOG_FILE"

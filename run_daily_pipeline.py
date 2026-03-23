"""
Daily pipeline orchestrator.

Step order:
  1. scraper/fetch_kalimati_prices.py   – scrape & persist prices
  2. analysis.anomaly_report            – anomaly detection
  3. analysis.forecast_baseline         – multi-model forecasting
  4. analysis.generate_market_brief     – markdown brief
  5. analysis.commodity_normalization_audit
  6. analysis.data_quality_audit
  7. analysis.generate_policy_flags     – row-depth & price-quality flags
  8. build_status()                     – write pipeline_status.json
  9. storage/load_history_to_sqlite.py  – load all outputs to SQLite
 10. ops/smoke_test_pipeline.py         – assert all outputs are sane
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
STATUS_PATH = ROOT / "data/processed/kalimati_pipeline_status.json"
HISTORY_PATH = ROOT / "data/processed/kalimati_price_history.csv"

PRE_STATUS_STEPS = [
    ["scraper/fetch_kalimati_prices.py"],
    ["-m", "analysis.anomaly_report"],
    ["-m", "analysis.forecast_baseline"],
    ["-m", "analysis.generate_market_brief"],
    ["-m", "analysis.commodity_normalization_audit"],
    ["-m", "analysis.data_quality_audit"],
    ["-m", "analysis.generate_policy_flags"],   # ← generates policy flag CSVs
]

POST_STATUS_STEPS = [
    ["storage/load_history_to_sqlite.py"],
    ["ops/smoke_test_pipeline.py"],
]


def run_step(step: list[str]) -> None:
    cmd = [sys.executable, *step]
    print("=" * 80, flush=True)
    print("Running:", " ".join(cmd), flush=True)
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def build_status() -> None:
    status: dict = {
        "pipeline_ran_at_utc": datetime.now(timezone.utc).isoformat(),
        "history_exists": HISTORY_PATH.exists(),
        "history_rows": 0,
        "latest_history_bs_date": None,
        "latest_history_ad_date": None,
    }

    if HISTORY_PATH.exists():
        df = pd.read_csv(HISTORY_PATH).copy()
        status["history_rows"] = int(len(df))

        if len(df) > 0:
            df["requested_date_ad_dt"] = pd.to_datetime(
                df["requested_date_ad"], errors="coerce"
            )
            df["fetched_at_utc_dt"] = pd.to_datetime(
                df["fetched_at_utc"], errors="coerce", utc=True
            ).dt.tz_convert(None)
            df["sort_date"] = df["requested_date_ad_dt"].fillna(df["fetched_at_utc_dt"])

            latest_idx = df["sort_date"].idxmax()
            latest_row = df.loc[latest_idx]

            status["latest_history_bs_date"] = (
                None
                if pd.isna(latest_row["scrape_date_bs"])
                else str(latest_row["scrape_date_bs"])
            )

            requested_ad = (
                None
                if pd.isna(latest_row["requested_date_ad"])
                else str(latest_row["requested_date_ad"]).strip()
            )
            if requested_ad:
                status["latest_history_ad_date"] = requested_ad
            else:
                latest_sort_date = latest_row["sort_date"]
                status["latest_history_ad_date"] = (
                    None
                    if pd.isna(latest_sort_date)
                    else pd.Timestamp(latest_sort_date).strftime("%Y-%m-%d")
                )

    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(
        json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> None:
    for step in PRE_STATUS_STEPS:
        run_step(step)

    build_status()
    print(f"Saved pipeline status: {STATUS_PATH}", flush=True)

    for step in POST_STATUS_STEPS:
        run_step(step)

    print()
    print("Daily pipeline completed successfully.", flush=True)


if __name__ == "__main__":
    main()

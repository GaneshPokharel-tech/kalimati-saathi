"""
Advanced anomaly detection for Kalimati vegetable prices.

Detection methods (all run, worst-case severity reported):
  1. Rolling-median z-score  – deviation from 7-day median in std units
  2. IQR spike              – price vs inter-quartile range of lookback window
  3. Consecutive-day change – day-over-day % change threshold

Severity tiers:
  low      |z| in [1.5, 2.5)  or  pct_change in [15%, 30%)
  medium   |z| in [2.5, 3.5)  or  pct_change in [30%, 50%)
  high     |z| in [3.5, 5.0)  or  pct_change in [50%, 75%)
  critical |z| >= 5.0          or  pct_change >= 75%
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
HISTORY_CSV = ROOT / "data/processed/kalimati_price_history.csv"
OUTPUT_DIR = ROOT / "data/processed"
OUTPUT_PATH = OUTPUT_DIR / "kalimati_anomaly_report.csv"

from analysis.history_confidence import add_history_confidence

LOOKBACK = 7
MIN_HISTORY = 8

# Z-score severity bands
_Z_LOW = 1.5
_Z_MED = 2.5
_Z_HIGH = 3.5
_Z_CRIT = 5.0

# Percent-change severity bands
_PCT_LOW = 15.0
_PCT_MED = 30.0
_PCT_HIGH = 50.0
_PCT_CRIT = 75.0


def _severity_from_z(z: float) -> str:
    az = abs(z)
    if az >= _Z_CRIT:
        return "critical"
    if az >= _Z_HIGH:
        return "high"
    if az >= _Z_MED:
        return "medium"
    if az >= _Z_LOW:
        return "low"
    return "normal"


def _severity_from_pct(pct: float) -> str:
    ap = abs(pct)
    if ap >= _PCT_CRIT:
        return "critical"
    if ap >= _PCT_HIGH:
        return "high"
    if ap >= _PCT_MED:
        return "medium"
    if ap >= _PCT_LOW:
        return "low"
    return "normal"


_SEVERITY_ORDER = {"normal": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}


def _worst_severity(*sevs: str) -> str:
    return max(sevs, key=lambda s: _SEVERITY_ORDER.get(s, 0))


def load_history() -> pd.DataFrame:
    df = pd.read_csv(HISTORY_CSV).copy()
    df = add_history_confidence(df)
    df = df.dropna(subset=["commodity", "unit", "avg_price", "sort_date"]).copy()
    df = df.sort_values(["commodity", "unit", "sort_date"]).reset_index(drop=True)
    return df


def build_anomaly_report(df: pd.DataFrame, lookback: int = LOOKBACK, min_history: int = MIN_HISTORY) -> pd.DataFrame:
    rows = []

    for (commodity, unit), group in df.groupby(["commodity", "unit"], sort=True):
        group = group.sort_values("sort_date").reset_index(drop=True)
        if len(group) < min_history:
            continue

        latest_row = group.iloc[-1]
        history_window = group.iloc[-(lookback + 1):-1].copy()
        if len(history_window) < lookback:
            continue

        prices = history_window["avg_price"].values.astype(float)
        current_price = float(latest_row["avg_price"])

        baseline_median = float(np.median(prices))
        baseline_mean = float(np.mean(prices))
        baseline_std = float(np.std(prices))

        if baseline_median == 0 or np.isnan(baseline_median):
            continue

        # ── Method 1: pct change vs median ──────────────────────────────────
        abs_change = current_price - baseline_median
        pct_change = (abs_change / baseline_median) * 100

        # ── Method 2: z-score ───────────────────────────────────────────────
        if baseline_std > 0:
            z_score = (current_price - baseline_mean) / baseline_std
        else:
            z_score = 0.0

        # ── Method 3: IQR ratio ─────────────────────────────────────────────
        q1, q3 = float(np.percentile(prices, 25)), float(np.percentile(prices, 75))
        iqr = q3 - q1
        if iqr > 0:
            iqr_ratio = (current_price - baseline_median) / iqr
        else:
            iqr_ratio = 0.0

        # ── Composite severity ───────────────────────────────────────────────
        sev_pct = _severity_from_pct(pct_change)
        sev_z = _severity_from_z(z_score)
        anomaly_severity = _worst_severity(sev_pct, sev_z)

        rows.append({
            "commodity": commodity,
            "unit": unit,
            "latest_date": latest_row["sort_date"],
            "latest_bs_date": latest_row["scrape_date_bs"],
            "current_avg_price": round(current_price, 2),
            "baseline_median_7": round(baseline_median, 2),
            "baseline_mean_7": round(baseline_mean, 2),
            "baseline_std_7": round(baseline_std, 2),
            "abs_change_vs_median": round(abs_change, 2),
            "pct_change_vs_median": round(pct_change, 2),
            "z_score": round(z_score, 3),
            "iqr_ratio": round(iqr_ratio, 3),
            "anomaly_severity": anomaly_severity,
            "history_points": len(group),
            "latest_history_confidence_band": latest_row["history_confidence_band"],
            "latest_history_confidence_rank": int(latest_row["history_confidence_rank"]),
            "latest_is_default_model_window": bool(latest_row["is_default_model_window"]),
        })

    if not rows:
        return pd.DataFrame()

    report_df = pd.DataFrame(rows)
    report_df["abs_pct_change"] = report_df["pct_change_vs_median"].abs()
    report_df = report_df.sort_values(
        ["abs_pct_change", "commodity"], ascending=[False, True]
    ).reset_index(drop=True)
    return report_df


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_history()
    report_df = build_anomaly_report(df)

    print(f"Total history rows: {len(df):,}")
    if not df.empty and "scrape_date_bs" in df.columns:
        valid_dates = df["scrape_date_bs"].dropna()
        if not valid_dates.empty:
            print(f"Latest market date in data: {valid_dates.iloc[-1]}")
    print(f"Series evaluated: {len(report_df):,}")
    print()

    if report_df.empty:
        print("No anomaly candidates found.")
        return

    report_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"Saved anomaly report: {OUTPUT_PATH}")
    print()

    print("Severity breakdown:")
    print(report_df["anomaly_severity"].value_counts().to_string())
    print()

    display_cols = [
        "commodity", "unit", "latest_bs_date", "current_avg_price",
        "baseline_median_7", "pct_change_vs_median", "z_score",
        "anomaly_severity", "latest_history_confidence_band",
    ]
    print("Top 20 anomaly candidates:")
    print(report_df[display_cols].head(20).to_string(index=False))

    print()
    print("Top positive spikes:")
    print(
        report_df.sort_values("pct_change_vs_median", ascending=False)[display_cols]
        .head(10)
        .to_string(index=False)
    )

    print()
    print("Top negative drops:")
    print(
        report_df.sort_values("pct_change_vs_median", ascending=True)[display_cols]
        .head(10)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()

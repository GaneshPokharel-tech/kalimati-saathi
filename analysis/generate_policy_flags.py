"""
Generate data-driven policy flag CSV files consumed by the dashboard, smoke tests,
and SQLite loader.

Outputs:
  data/processed/row_depth_policy_flags.csv   – per-date row-depth quality flags
  data/processed/price_quality_policy_flags.csv – per-row price anomaly flags

Row-depth severity thresholds:
  critical_low  < 10 rows  → exclude + manual_review
  low           < 30 rows  → manual_review (not excluded)
  normal        >= 30 rows → include

Price quality issue types:
  invalid_price_logic   – min > max, or avg outside [min, max]
  zero_or_negative_price – avg_price <= 0
  statistical_outlier   – IQR-based outlier per commodity+unit (>= 10 history points)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
HISTORY_CSV = ROOT / "data/processed/kalimati_price_history.csv"
DB_PATH = ROOT / "data/processed/kalimati.db"
OUT_DIR = ROOT / "data/processed"
ROW_DEPTH_OUT = OUT_DIR / "row_depth_policy_flags.csv"
PRICE_QUALITY_OUT = OUT_DIR / "price_quality_policy_flags.csv"

ROW_DEPTH_CRITICAL_THRESHOLD = 10
ROW_DEPTH_LOW_THRESHOLD = 30
IQR_MULTIPLIER = 3.0
MIN_SERIES_FOR_IQR = 10


def load_history() -> pd.DataFrame:
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        try:
            df = pd.read_sql_query("SELECT * FROM price_history", conn)
            if not df.empty:
                return df
        except Exception:
            pass
        finally:
            conn.close()

    if HISTORY_CSV.exists():
        return pd.read_csv(HISTORY_CSV).copy()

    raise FileNotFoundError("No history data found. Run the scraper first.")


def _policy_action(exclude: bool, review: bool) -> str:
    if exclude:
        return "exclude"
    if review:
        return "manual_review"
    return "include"


def build_row_depth_flags(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (requested_date_ad, scrape_date_bs) date."""
    from analysis.history_confidence import add_history_confidence

    df = add_history_confidence(df)

    agg = (
        df.groupby(["requested_date_ad", "scrape_date_bs"], dropna=False)
        .agg(
            row_count=("commodity", "count"),
            history_confidence_band=("history_confidence_band", "first"),
        )
        .reset_index()
    )

    def _severity(n: int) -> str:
        if n < ROW_DEPTH_CRITICAL_THRESHOLD:
            return "critical_low"
        if n < ROW_DEPTH_LOW_THRESHOLD:
            return "low"
        return "normal"

    agg["row_depth_severity"] = agg["row_count"].apply(_severity)
    agg["exclude_from_default_model_window"] = agg["row_depth_severity"] == "critical_low"
    agg["manual_review"] = agg["row_depth_severity"].isin(["critical_low", "low"])
    agg["policy_action"] = agg.apply(
        lambda r: _policy_action(
            r["exclude_from_default_model_window"], r["manual_review"]
        ),
        axis=1,
    )

    return agg[
        [
            "requested_date_ad",
            "scrape_date_bs",
            "row_count",
            "history_confidence_band",
            "row_depth_severity",
            "exclude_from_default_model_window",
            "manual_review",
            "policy_action",
        ]
    ].reset_index(drop=True)


def build_price_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """One row per flagged (date, commodity, unit)."""
    keep_cols = [
        "requested_date_ad",
        "scrape_date_bs",
        "commodity",
        "unit",
        "min_price",
        "max_price",
        "avg_price",
    ]
    base = df[keep_cols].copy()

    flagged_parts: list[pd.DataFrame] = []

    # --- invalid logic ---
    invalid_mask = (
        (base["min_price"].notna() & base["max_price"].notna() & (base["min_price"] > base["max_price"]))
        | (base["avg_price"].notna() & base["min_price"].notna() & (base["avg_price"] < base["min_price"]))
        | (base["avg_price"].notna() & base["max_price"].notna() & (base["avg_price"] > base["max_price"]))
    )
    if invalid_mask.any():
        part = base[invalid_mask].copy()
        part["price_issue_type"] = "invalid_price_logic"
        flagged_parts.append(part)

    # --- zero / negative avg ---
    zero_mask = base["avg_price"].notna() & (base["avg_price"] <= 0)
    if zero_mask.any():
        part = base[zero_mask].copy()
        part["price_issue_type"] = "zero_or_negative_price"
        flagged_parts.append(part)

    # --- IQR outliers per commodity+unit ---
    outlier_parts: list[pd.DataFrame] = []
    for (commodity, unit), grp in base.groupby(["commodity", "unit"], sort=False):
        valid = grp["avg_price"].dropna()
        if len(valid) < MIN_SERIES_FOR_IQR:
            continue
        q1 = float(np.percentile(valid, 25))
        q3 = float(np.percentile(valid, 75))
        iqr = q3 - q1
        if iqr == 0:
            continue
        lower = q1 - IQR_MULTIPLIER * iqr
        upper = q3 + IQR_MULTIPLIER * iqr
        outlier_idx = grp.index[
            grp["avg_price"].notna()
            & ((grp["avg_price"] < lower) | (grp["avg_price"] > upper))
        ]
        if len(outlier_idx) == 0:
            continue
        part = base.loc[outlier_idx].copy()
        part["price_issue_type"] = "statistical_outlier"
        outlier_parts.append(part)

    if outlier_parts:
        flagged_parts.append(pd.concat(outlier_parts, ignore_index=True))

    if not flagged_parts:
        empty_cols = keep_cols + [
            "price_issue_type",
            "exclude_from_default_model_window",
            "manual_review",
            "policy_action",
        ]
        return pd.DataFrame(columns=empty_cols)

    combined = pd.concat(flagged_parts, ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["scrape_date_bs", "commodity", "unit"], keep="first"
    )

    combined["exclude_from_default_model_window"] = combined["price_issue_type"].isin(
        ["invalid_price_logic", "zero_or_negative_price"]
    )
    combined["manual_review"] = combined["price_issue_type"] == "statistical_outlier"
    combined["policy_action"] = combined.apply(
        lambda r: _policy_action(
            r["exclude_from_default_model_window"], r["manual_review"]
        ),
        axis=1,
    )

    return combined[
        keep_cols
        + [
            "price_issue_type",
            "exclude_from_default_model_window",
            "manual_review",
            "policy_action",
        ]
    ].reset_index(drop=True)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_history()
    print(f"Loaded {len(df):,} history rows")

    row_depth_df = build_row_depth_flags(df)
    row_depth_df.to_csv(ROW_DEPTH_OUT, index=False, encoding="utf-8-sig")
    print(f"Saved row_depth_policy_flags: {len(row_depth_df):,} date entries → {ROW_DEPTH_OUT}")

    price_quality_df = build_price_quality_flags(df)
    price_quality_df.to_csv(PRICE_QUALITY_OUT, index=False, encoding="utf-8-sig")
    print(
        f"Saved price_quality_policy_flags: {len(price_quality_df):,} flagged rows → {PRICE_QUALITY_OUT}"
    )

    print()
    if not row_depth_df.empty:
        print("Row-depth policy summary:")
        print(row_depth_df["policy_action"].value_counts().to_string())
        print(row_depth_df["row_depth_severity"].value_counts().to_string())

    print()
    if not price_quality_df.empty:
        print("Price-quality flag summary:")
        print(price_quality_df["price_issue_type"].value_counts().to_string())
        print(price_quality_df["policy_action"].value_counts().to_string())
    else:
        print("No price-quality flags found (data looks clean).")


if __name__ == "__main__":
    main()

"""
Generate per-date row-depth policy flags and per-commodity price-quality policy flags.

Outputs
-------
  data/processed/row_depth_policy_flags.csv
  data/processed/price_quality_policy_flags.csv
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
HISTORY_PATH = ROOT / "data/processed/kalimati_price_history.csv"
OUTPUT_DIR = ROOT / "data/processed"
ROW_DEPTH_OUTPUT = OUTPUT_DIR / "row_depth_policy_flags.csv"
PRICE_QUALITY_OUTPUT = OUTPUT_DIR / "price_quality_policy_flags.csv"


# Thresholds
CRITICAL_LOW_THRESHOLD = 10
LOW_THRESHOLD = 30
IQR_MULTIPLIER = 3.0


def load_history() -> pd.DataFrame:
    df = pd.read_csv(HISTORY_PATH).copy()
    df["requested_date_ad_dt"] = pd.to_datetime(df.get("requested_date_ad"), errors="coerce")
    df["fetched_at_utc_dt"] = pd.to_datetime(
        df.get("fetched_at_utc"), errors="coerce", utc=True
    ).dt.tz_convert(None)
    df["sort_date"] = df["requested_date_ad_dt"].fillna(df["fetched_at_utc_dt"])
    return df


def _row_depth_severity(n: int) -> str:
    if n < CRITICAL_LOW_THRESHOLD:
        return "critical_low"
    if n < LOW_THRESHOLD:
        return "low"
    return "normal"


def build_row_depth_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Per scrape-date: count commodity rows and classify data-depth severity."""
    date_groups = (
        df.groupby(["requested_date_ad", "scrape_date_bs"], dropna=False)
        .agg(
            row_count=("avg_price", "count"),
            history_confidence_band=("history_confidence_band", "first")
            if "history_confidence_band" in df.columns
            else ("avg_price", lambda _: "unknown"),
        )
        .reset_index()
    )

    if "history_confidence_band" not in date_groups.columns:
        date_groups["history_confidence_band"] = "unknown"

    date_groups["row_depth_severity"] = date_groups["row_count"].apply(_row_depth_severity)
    date_groups["exclude_from_default_model_window"] = date_groups["row_depth_severity"].isin(
        ["critical_low"]
    )
    date_groups["manual_review"] = date_groups["row_depth_severity"].isin(
        ["critical_low", "low"]
    )
    date_groups["policy_action"] = date_groups.apply(
        lambda r: "exclude"
        if r["row_depth_severity"] == "critical_low"
        else ("flag_for_review" if r["row_depth_severity"] == "low" else "accept"),
        axis=1,
    )

    ordered_cols = [
        "requested_date_ad",
        "scrape_date_bs",
        "row_count",
        "history_confidence_band",
        "row_depth_severity",
        "exclude_from_default_model_window",
        "manual_review",
        "policy_action",
    ]
    return (
        date_groups[ordered_cols]
        .sort_values(["requested_date_ad", "scrape_date_bs"])
        .reset_index(drop=True)
    )


def build_price_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Per commodity+date: flag invalid prices and statistical outliers."""
    rows = []

    for (date_ad, date_bs), group in df.groupby(
        ["requested_date_ad", "scrape_date_bs"], dropna=False
    ):
        group = group.copy()
        prices = group["avg_price"].dropna()

        # IQR outlier fences for this date's cross-section
        if len(prices) >= 4:
            q1 = prices.quantile(0.25)
            q3 = prices.quantile(0.75)
            iqr = q3 - q1
            lower_fence = q1 - IQR_MULTIPLIER * iqr
            upper_fence = q3 + IQR_MULTIPLIER * iqr
        else:
            lower_fence = -np.inf
            upper_fence = np.inf

        for _, row in group.iterrows():
            min_p = row.get("min_price")
            max_p = row.get("max_price")
            avg_p = row.get("avg_price")

            issues = []

            if pd.notna(min_p) and pd.notna(max_p) and float(min_p) > float(max_p):
                issues.append("invalid_price_logic")

            if pd.notna(avg_p) and float(avg_p) <= 0:
                issues.append("zero_or_negative_price")

            if pd.notna(avg_p) and (float(avg_p) < lower_fence or float(avg_p) > upper_fence):
                issues.append("statistical_outlier")

            for issue in issues:
                is_hard_exclude = issue in ("invalid_price_logic", "zero_or_negative_price")
                rows.append(
                    {
                        "requested_date_ad": date_ad,
                        "scrape_date_bs": date_bs,
                        "commodity": row.get("commodity"),
                        "unit": row.get("unit"),
                        "min_price": min_p,
                        "max_price": max_p,
                        "avg_price": avg_p,
                        "price_issue_type": issue,
                        "exclude_from_default_model_window": True,
                        "manual_review": True,
                        "policy_action": "exclude" if is_hard_exclude else "flag_for_review",
                    }
                )

    if not rows:
        return pd.DataFrame(
            columns=[
                "requested_date_ad",
                "scrape_date_bs",
                "commodity",
                "unit",
                "min_price",
                "max_price",
                "avg_price",
                "price_issue_type",
                "exclude_from_default_model_window",
                "manual_review",
                "policy_action",
            ]
        )

    return (
        pd.DataFrame(rows)
        .sort_values(["requested_date_ad", "commodity"])
        .reset_index(drop=True)
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_history()
    print(f"History rows loaded: {len(df):,}")

    # Add confidence band if not already present
    if "history_confidence_band" not in df.columns:
        try:
            from analysis.history_confidence import add_history_confidence
            df = add_history_confidence(df)
        except ImportError:
            df["history_confidence_band"] = "unknown"

    row_depth_df = build_row_depth_flags(df)
    row_depth_df.to_csv(ROW_DEPTH_OUTPUT, index=False, encoding="utf-8-sig")
    print(f"Row-depth policy flags: {len(row_depth_df):,} rows  →  {ROW_DEPTH_OUTPUT}")

    price_quality_df = build_price_quality_flags(df)
    price_quality_df.to_csv(PRICE_QUALITY_OUTPUT, index=False, encoding="utf-8-sig")
    print(f"Price-quality policy flags: {len(price_quality_df):,} rows  →  {PRICE_QUALITY_OUTPUT}")

    # Summary
    if not row_depth_df.empty:
        sev_counts = row_depth_df["row_depth_severity"].value_counts().to_dict()
        print(f"  Row-depth severity breakdown: {sev_counts}")

    if not price_quality_df.empty:
        issue_counts = price_quality_df["price_issue_type"].value_counts().to_dict()
        print(f"  Price-quality issue breakdown: {issue_counts}")


if __name__ == "__main__":
    main()

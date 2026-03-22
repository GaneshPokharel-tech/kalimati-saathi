import sqlite3
from pathlib import Path

import pandas as pd

from analysis.history_confidence import add_history_confidence, confidence_band_summary

DB_PATH = Path("data/processed/kalimati.db")
HISTORY_CSV = Path("data/processed/kalimati_price_history.csv")
OUT_PATH = Path("data/processed/data_quality_audit_summary.txt")


def load_history():
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        try:
            df = pd.read_sql_query("SELECT * FROM price_history", conn)
            if not df.empty:
                return df
        finally:
            conn.close()

    if HISTORY_CSV.exists():
        return pd.read_csv(HISTORY_CSV).copy()

    raise FileNotFoundError("No history source found")


def main():
    df = load_history().copy()
    df = add_history_confidence(df)

    duplicate_key = ["scrape_date_bs", "commodity", "unit"]
    duplicate_rows = df[df.duplicated(subset=duplicate_key, keep=False)].copy()

    latest_idx = df["sort_date"].idxmax()
    latest_bs_date = df.loc[latest_idx, "scrape_date_bs"]
    latest_df = df[df["scrape_date_bs"] == latest_bs_date].copy()

    null_summary = df[[
        "fetched_at_utc", "requested_date_ad", "scrape_date_bs",
        "commodity", "unit", "min_price", "max_price", "avg_price"
    ]].isna().sum().sort_values(ascending=False)

    invalid_price_rows = df[
        (df["min_price"].notna() & df["max_price"].notna() & (df["min_price"] > df["max_price"])) |
        (df["avg_price"].notna() & df["min_price"].notna() & (df["avg_price"] < df["min_price"])) |
        (df["avg_price"].notna() & df["max_price"].notna() & (df["avg_price"] > df["max_price"]))
    ].copy()

    zero_or_negative_rows = df[
        (df["min_price"].fillna(0) <= 0) |
        (df["max_price"].fillna(0) <= 0) |
        (df["avg_price"].fillna(0) <= 0)
    ].copy()

    unit_counts = (
        df.groupby("unit", dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", "unit"], ascending=[False, True])
        .reset_index(drop=True)
    )

    latest_unit_counts = (
        latest_df.groupby("unit", dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", "unit"], ascending=[False, True])
        .reset_index(drop=True)
    )

    confidence_summary_df = confidence_band_summary(df)

    lines = []
    lines.append("Kalimati Saathi Data Quality Audit")
    lines.append("=" * 60)
    lines.append(f"Total rows: {len(df)}")
    lines.append(f"Unique BS dates: {df['scrape_date_bs'].nunique()}")
    lines.append(f"Latest BS date by sort_date: {latest_bs_date}")
    lines.append(f"Rows on latest BS date: {len(latest_df)}")
    lines.append("")

    lines.append("History confidence summary")
    lines.append("-" * 60)
    if len(confidence_summary_df) > 0:
        lines.append(confidence_summary_df.to_string(index=False))
    else:
        lines.append("None")
    lines.append("")

    lines.append("Null summary")
    lines.append("-" * 60)
    for col, val in null_summary.items():
        lines.append(f"{col}: {int(val)}")
    lines.append("")

    lines.append("Duplicate audit")
    lines.append("-" * 60)
    lines.append(f"Duplicate rows on key {duplicate_key}: {len(duplicate_rows)}")
    lines.append("")

    lines.append("Invalid price logic audit")
    lines.append("-" * 60)
    lines.append(f"Rows with min/max/avg inconsistency: {len(invalid_price_rows)}")
    lines.append("")

    lines.append("Zero or negative price audit")
    lines.append("-" * 60)
    lines.append(f"Rows with zero/negative min|max|avg: {len(zero_or_negative_rows)}")
    lines.append("")

    lines.append("Unit distribution (all history)")
    lines.append("-" * 60)
    if len(unit_counts) > 0:
        lines.append(unit_counts.to_string(index=False))
    else:
        lines.append("None")
    lines.append("")

    lines.append("Unit distribution (latest date)")
    lines.append("-" * 60)
    if len(latest_unit_counts) > 0:
        lines.append(latest_unit_counts.to_string(index=False))
    else:
        lines.append("None")
    lines.append("")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print("Data quality audit completed")
    print("Output:", OUT_PATH)
    print("Total rows:", len(df))
    print("Latest BS date:", latest_bs_date)
    print("Duplicate rows:", len(duplicate_rows))
    print("Invalid price rows:", len(invalid_price_rows))
    print("Zero/negative price rows:", len(zero_or_negative_rows))
    print()
    print("Confidence summary:")
    if len(confidence_summary_df) > 0:
        print(confidence_summary_df.to_string(index=False))
    else:
        print("None")
    print()
    print("Null summary:")
    print(null_summary.to_string())


if __name__ == "__main__":
    main()

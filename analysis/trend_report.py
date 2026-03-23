"""Quick trend report comparing latest two available market dates."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
HISTORY_PATH = ROOT / "data/processed/kalimati_price_history.csv"


def main() -> None:
    df = pd.read_csv(HISTORY_PATH)

    print("=== KALIMATI TREND REPORT ===")
    print(f"Total history rows: {len(df)}")
    print(f"Unique scrape dates: {df['scrape_date_bs'].nunique()}")
    print()

    requested_date = pd.to_datetime(df["requested_date_ad"], errors="coerce")
    fetched_date = pd.to_datetime(df["fetched_at_utc"], errors="coerce").dt.tz_convert(None)
    df["sort_date"] = requested_date.fillna(fetched_date)

    date_order_df = (
        df.groupby("scrape_date_bs", as_index=False)["sort_date"]
        .max()
        .sort_values("sort_date")
        .reset_index(drop=True)
    )

    latest_date = date_order_df.iloc[-1]["scrape_date_bs"]
    latest_df = df[df["scrape_date_bs"] == latest_date].copy()
    latest_df["price_spread"] = latest_df["max_price"] - latest_df["min_price"]

    print(f"Latest scrape date: {latest_date}")
    print(f"Items on latest date: {len(latest_df)}")
    print()

    print("Top 10 expensive items on latest date:")
    print(
        latest_df.sort_values("avg_price", ascending=False)[
            ["commodity", "unit", "avg_price"]
        ]
        .head(10)
        .to_string(index=False)
    )
    print()

    if len(date_order_df) < 2:
        print("Trend comparison not available yet (only one date in history).")
        return

    current_date = date_order_df.iloc[-1]["scrape_date_bs"]
    previous_date = date_order_df.iloc[-2]["scrape_date_bs"]

    current_df = df[df["scrape_date_bs"] == current_date][
        ["commodity", "avg_price"]
    ].rename(columns={"avg_price": "current_avg_price"})
    previous_df = df[df["scrape_date_bs"] == previous_date][
        ["commodity", "avg_price"]
    ].rename(columns={"avg_price": "previous_avg_price"})

    compare_df = current_df.merge(previous_df, on="commodity", how="inner")
    compare_df["price_change"] = (
        compare_df["current_avg_price"] - compare_df["previous_avg_price"]
    )

    print(f"Comparing {current_date} vs {previous_date}")
    print()

    print("Top 10 price increases:")
    print(
        compare_df.sort_values("price_change", ascending=False)[
            ["commodity", "previous_avg_price", "current_avg_price", "price_change"]
        ]
        .head(10)
        .to_string(index=False)
    )
    print()

    print("Top 10 price decreases:")
    print(
        compare_df.sort_values("price_change", ascending=True)[
            ["commodity", "previous_avg_price", "current_avg_price", "price_change"]
        ]
        .head(10)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()

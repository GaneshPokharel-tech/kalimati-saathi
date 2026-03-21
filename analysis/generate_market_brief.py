import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path("data/processed/kalimati.db")
HISTORY_CSV = Path("data/processed/kalimati_price_history.csv")
ANOMALY_CSV = Path("data/processed/kalimati_anomaly_report.csv")
FORECAST_CSV = Path("data/processed/kalimati_forecast_baseline.csv")
OUTPUT_PATH = Path("data/processed/kalimati_market_brief.md")


def load_sqlite_table(table_name):
    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_history():
    df = load_sqlite_table("price_history")
    if not df.empty:
        return df
    if HISTORY_CSV.exists():
        return pd.read_csv(HISTORY_CSV).copy()
    return pd.DataFrame()


def load_anomaly():
    df = load_sqlite_table("anomaly_report")
    if not df.empty:
        return df
    if ANOMALY_CSV.exists():
        return pd.read_csv(ANOMALY_CSV).copy()
    return pd.DataFrame()


def load_forecast():
    df = load_sqlite_table("forecast_baseline")
    if not df.empty:
        return df
    if FORECAST_CSV.exists():
        return pd.read_csv(FORECAST_CSV).copy()
    return pd.DataFrame()


def main():
    history_df = load_history()
    anomaly_df = load_anomaly()
    forecast_df = load_forecast()

    if history_df.empty:
        raise ValueError("No history data found")

    history_df["requested_date_ad_dt"] = pd.to_datetime(history_df["requested_date_ad"], errors="coerce")
    history_df["fetched_at_utc_dt"] = pd.to_datetime(history_df["fetched_at_utc"], errors="coerce", utc=True).dt.tz_convert(None)
    history_df["sort_date"] = history_df["requested_date_ad_dt"].fillna(history_df["fetched_at_utc_dt"])
    history_df["price_spread"] = history_df["max_price"] - history_df["min_price"]

    latest_idx = history_df["sort_date"].idxmax()
    latest_bs_date = str(history_df.loc[latest_idx, "scrape_date_bs"])
    latest_sort_date = pd.Timestamp(history_df.loc[latest_idx, "sort_date"]).strftime("%Y-%m-%d")

    latest_df = history_df[history_df["scrape_date_bs"] == latest_bs_date].copy()
    latest_df = latest_df.sort_values("commodity").reset_index(drop=True)

    lines = []
    lines.append("# Kalimati Market Brief")
    lines.append("")
    lines.append(f"- Latest saved BS date: **{latest_bs_date}**")
    lines.append(f"- Latest saved AD date: **{latest_sort_date}**")
    lines.append(f"- Total items: **{len(latest_df)}**")
    lines.append(f"- Average market price: **Rs. {latest_df['avg_price'].mean():.2f}**")
    lines.append("")

    if not latest_df.empty:
        most_expensive = latest_df.sort_values("avg_price", ascending=False).iloc[0]
        cheapest = latest_df.sort_values("avg_price", ascending=True).iloc[0]
        widest_spread = latest_df.sort_values("price_spread", ascending=False).iloc[0]

        lines.append("## Daily highlights")
        lines.append("")
        lines.append(f"- Most expensive: **{most_expensive['commodity']}** ({most_expensive['unit']}) at **Rs. {most_expensive['avg_price']:.2f}**")
        lines.append(f"- Cheapest: **{cheapest['commodity']}** ({cheapest['unit']}) at **Rs. {cheapest['avg_price']:.2f}**")
        lines.append(f"- Widest spread: **{widest_spread['commodity']}** ({widest_spread['unit']}) with spread **Rs. {widest_spread['price_spread']:.2f}**")
        lines.append("")

    lines.append("## Top 10 expensive items")
    lines.append("")
    for _, row in latest_df.sort_values("avg_price", ascending=False).head(10).iterrows():
        lines.append(f"- {row['commodity']} ({row['unit']}): Rs. {row['avg_price']:.2f}")
    lines.append("")

    if not anomaly_df.empty:
        lines.append("## Top anomaly watchlist")
        lines.append("")
        for _, row in anomaly_df.sort_values("pct_change_vs_median", ascending=False).head(5).iterrows():
            lines.append(
                f"- Spike: {row['commodity']} ({row['unit']}) | current Rs. {row['current_avg_price']:.2f} | "
                f"7-day median Rs. {row['baseline_median_7']:.2f} | change {row['pct_change_vs_median']:.2f}%"
            )
        for _, row in anomaly_df.sort_values("pct_change_vs_median", ascending=True).head(5).iterrows():
            lines.append(
                f"- Drop: {row['commodity']} ({row['unit']}) | current Rs. {row['current_avg_price']:.2f} | "
                f"7-day median Rs. {row['baseline_median_7']:.2f} | change {row['pct_change_vs_median']:.2f}%"
            )
        lines.append("")

    if not forecast_df.empty:
        lines.append("## Top forecast watchlist")
        lines.append("")
        for _, row in forecast_df.sort_values("forecast_delta_vs_latest", ascending=False).head(5).iterrows():
            lines.append(
                f"- Upward reversion: {row['commodity']} ({row['unit']}) | latest Rs. {row['latest_avg_price']:.2f} | "
                f"baseline forecast Rs. {row['next_day_forecast']:.2f} | delta {row['forecast_delta_vs_latest']:.2f}"
            )
        for _, row in forecast_df.sort_values("forecast_delta_vs_latest", ascending=True).head(5).iterrows():
            lines.append(
                f"- Downward reversion: {row['commodity']} ({row['unit']}) | latest Rs. {row['latest_avg_price']:.2f} | "
                f"baseline forecast Rs. {row['next_day_forecast']:.2f} | delta {row['forecast_delta_vs_latest']:.2f}"
            )
        lines.append("")

    OUTPUT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print("Market brief generated")
    print("Output:", OUTPUT_PATH)
    print("Latest BS date:", latest_bs_date)
    print("Latest AD date:", latest_sort_date)


if __name__ == "__main__":
    main()

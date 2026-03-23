"""Generate a Markdown market brief from the latest pipeline outputs."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from analysis.history_confidence import add_history_confidence, confidence_band_summary

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data/processed/kalimati.db"
HISTORY_CSV = ROOT / "data/processed/kalimati_price_history.csv"
ANOMALY_CSV = ROOT / "data/processed/kalimati_anomaly_report.csv"
FORECAST_CSV = ROOT / "data/processed/kalimati_forecast_baseline.csv"
OUTPUT_PATH = ROOT / "data/processed/kalimati_market_brief.md"

_ALLOWED_TABLES = {
    "price_history", "anomaly_report", "forecast_baseline",
    "pipeline_status", "scrape_status", "market_brief",
}


def _load_sqlite(table_name: str) -> pd.DataFrame:
    if table_name not in _ALLOWED_TABLES:
        return pd.DataFrame()
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_history() -> pd.DataFrame:
    df = _load_sqlite("price_history")
    if not df.empty:
        return df
    if HISTORY_CSV.exists():
        return pd.read_csv(HISTORY_CSV).copy()
    return pd.DataFrame()


def load_anomaly() -> pd.DataFrame:
    df = _load_sqlite("anomaly_report")
    if not df.empty:
        return df
    if ANOMALY_CSV.exists():
        return pd.read_csv(ANOMALY_CSV).copy()
    return pd.DataFrame()


def load_forecast() -> pd.DataFrame:
    df = _load_sqlite("forecast_baseline")
    if not df.empty:
        return df
    if FORECAST_CSV.exists():
        return pd.read_csv(FORECAST_CSV).copy()
    return pd.DataFrame()


def main() -> None:
    history_df = load_history()
    anomaly_df = load_anomaly()
    forecast_df = load_forecast()

    if history_df.empty:
        raise ValueError("No history data found")

    history_df = add_history_confidence(history_df)
    history_df["price_spread"] = history_df["max_price"] - history_df["min_price"]

    latest_idx = history_df["sort_date"].idxmax()
    latest_bs_date = str(history_df.loc[latest_idx, "scrape_date_bs"])
    latest_sort_date = pd.Timestamp(history_df.loc[latest_idx, "sort_date"]).strftime("%Y-%m-%d")
    latest_confidence_band = str(history_df.loc[latest_idx, "history_confidence_band"])

    latest_df = history_df[history_df["scrape_date_bs"] == latest_bs_date].copy()
    latest_df = latest_df.sort_values("commodity").reset_index(drop=True)

    band_summary_df = confidence_band_summary(history_df)

    lines = []
    lines.append("# Kalimati Market Brief")
    lines.append("")
    lines.append(f"- Latest saved BS date: **{latest_bs_date}**")
    lines.append(f"- Latest saved AD date: **{latest_sort_date}**")
    lines.append(f"- Latest date confidence band: **{latest_confidence_band}**")
    lines.append(f"- Total items: **{len(latest_df)}**")
    lines.append(f"- Average market price: **Rs. {latest_df['avg_price'].mean():.2f}**")
    lines.append("")

    lines.append("## Historical confidence notes")
    lines.append("")
    lines.append("- Default stronger modeling window starts at **2015-05-01**.")
    lines.append("- Last 90 days of data are classified as **current_live** (slides automatically).")
    lines.append("- **2014-11-01 to 2015-04-30** is medium-confidence historical data.")
    lines.append("- **2013-11-01 to 2014-10-31** is low-confidence historical data — use carefully.")
    lines.append("")

    if not band_summary_df.empty:
        lines.append("### History rows by confidence band")
        lines.append("")
        for _, row in band_summary_df.iterrows():
            lines.append(f"- {row['history_confidence_band']}: **{int(row['rows'])}** rows")
        lines.append("")

    if not latest_df.empty:
        most_expensive = latest_df.sort_values("avg_price", ascending=False).iloc[0]
        cheapest = latest_df.sort_values("avg_price", ascending=True).iloc[0]
        widest_spread = latest_df.sort_values("price_spread", ascending=False).iloc[0]

        lines.append("## Daily highlights")
        lines.append("")
        lines.append(
            f"- Most expensive: **{most_expensive['commodity']}** ({most_expensive['unit']}) "
            f"at **Rs. {most_expensive['avg_price']:.2f}**"
        )
        lines.append(
            f"- Cheapest: **{cheapest['commodity']}** ({cheapest['unit']}) "
            f"at **Rs. {cheapest['avg_price']:.2f}**"
        )
        lines.append(
            f"- Widest spread: **{widest_spread['commodity']}** ({widest_spread['unit']}) "
            f"spread **Rs. {widest_spread['price_spread']:.2f}**"
        )
        lines.append("")

    lines.append("## Top 10 expensive items")
    lines.append("")
    for _, row in latest_df.sort_values("avg_price", ascending=False).head(10).iterrows():
        lines.append(f"- {row['commodity']} ({row['unit']}): Rs. {row['avg_price']:.2f}")
    lines.append("")

    if not anomaly_df.empty:
        lines.append("## Top anomaly watchlist")
        lines.append("")
        sev_col = "anomaly_severity" if "anomaly_severity" in anomaly_df.columns else None
        for _, row in anomaly_df.sort_values("pct_change_vs_median", ascending=False).head(5).iterrows():
            sev = f" | severity {row[sev_col]}" if sev_col else ""
            lines.append(
                f"- Spike: {row['commodity']} ({row['unit']}) | current Rs. {row['current_avg_price']:.2f} | "
                f"7-day median Rs. {row['baseline_median_7']:.2f} | change {row['pct_change_vs_median']:.2f}%"
                f"{sev}"
            )
        for _, row in anomaly_df.sort_values("pct_change_vs_median", ascending=True).head(5).iterrows():
            sev = f" | severity {row[sev_col]}" if sev_col else ""
            lines.append(
                f"- Drop: {row['commodity']} ({row['unit']}) | current Rs. {row['current_avg_price']:.2f} | "
                f"7-day median Rs. {row['baseline_median_7']:.2f} | change {row['pct_change_vs_median']:.2f}%"
                f"{sev}"
            )
        lines.append("")

    if not forecast_df.empty:
        lines.append("## Top forecast watchlist")
        lines.append("")
        f1d_col = "forecast_1d" if "forecast_1d" in forecast_df.columns else "next_day_forecast"
        f7d_col = "forecast_7d" if "forecast_7d" in forecast_df.columns else None
        model_col = "model_used" if "model_used" in forecast_df.columns else None
        trend_col = "trend_direction" if "trend_direction" in forecast_df.columns else None

        for _, row in forecast_df.sort_values("forecast_delta_vs_latest", ascending=False).head(5).iterrows():
            extras = []
            if f7d_col:
                extras.append(f"7d Rs. {row[f7d_col]:.2f}")
            if trend_col:
                extras.append(f"trend {row[trend_col]}")
            if model_col:
                extras.append(f"model {row[model_col]}")
            extra_str = " | " + " | ".join(extras) if extras else ""
            lines.append(
                f"- Upward: {row['commodity']} ({row['unit']}) | latest Rs. {row['latest_avg_price']:.2f} | "
                f"forecast Rs. {row[f1d_col]:.2f} | delta {row['forecast_delta_vs_latest']:.2f}"
                f"{extra_str}"
            )
        for _, row in forecast_df.sort_values("forecast_delta_vs_latest", ascending=True).head(5).iterrows():
            extras = []
            if f7d_col:
                extras.append(f"7d Rs. {row[f7d_col]:.2f}")
            if trend_col:
                extras.append(f"trend {row[trend_col]}")
            if model_col:
                extras.append(f"model {row[model_col]}")
            extra_str = " | " + " | ".join(extras) if extras else ""
            lines.append(
                f"- Downward: {row['commodity']} ({row['unit']}) | latest Rs. {row['latest_avg_price']:.2f} | "
                f"forecast Rs. {row[f1d_col]:.2f} | delta {row['forecast_delta_vs_latest']:.2f}"
                f"{extra_str}"
            )
        lines.append("")

    OUTPUT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print("Market brief generated")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Latest BS date: {latest_bs_date}")
    print(f"Latest AD date: {latest_sort_date}")
    print(f"Latest date confidence band: {latest_confidence_band}")


if __name__ == "__main__":
    main()

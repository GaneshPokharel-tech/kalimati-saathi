import json
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Kalimati Saathi", layout="wide")

st.title("Kalimati Saathi")
st.caption("Kalimati vegetable price intelligence prototype")

history_csv_path = Path("data/processed/kalimati_price_history.csv")
anomaly_csv_path = Path("data/processed/kalimati_anomaly_report.csv")
forecast_csv_path = Path("data/processed/kalimati_forecast_baseline.csv")
market_brief_path = Path("data/processed/kalimati_market_brief.md")
sqlite_db_path = Path("data/processed/kalimati.db")

pipeline_status_path = Path("data/processed/kalimati_pipeline_status.json")
scrape_status_path = Path("data/processed/kalimati_last_scrape_status.json")

pipeline_status = {}
scrape_status = {}


def load_table_from_sqlite(table_name):
    if not sqlite_db_path.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(sqlite_db_path)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_single_row_json_table(table_name):
    df = load_table_from_sqlite(table_name)
    if not df.empty:
        return df.iloc[0].to_dict(), "SQLite"
    return {}, "None"


def load_history():
    df = load_table_from_sqlite("price_history")
    if not df.empty:
        return df, "SQLite"

    if history_csv_path.exists():
        return pd.read_csv(history_csv_path).copy(), "CSV"

    return pd.DataFrame(), "None"


def load_anomaly():
    df = load_table_from_sqlite("anomaly_report")
    if not df.empty:
        return df, "SQLite"

    if anomaly_csv_path.exists():
        return pd.read_csv(anomaly_csv_path).copy(), "CSV"

    return pd.DataFrame(), "None"


def load_forecast():
    df = load_table_from_sqlite("forecast_baseline")
    if not df.empty:
        return df, "SQLite"

    if forecast_csv_path.exists():
        return pd.read_csv(forecast_csv_path).copy(), "CSV"

    return pd.DataFrame(), "None"


def load_market_brief():
    df = load_table_from_sqlite("market_brief")
    if not df.empty and "brief_markdown" in df.columns:
        return str(df.iloc[0]["brief_markdown"]), "SQLite"

    if market_brief_path.exists():
        return market_brief_path.read_text(encoding="utf-8"), "Markdown"

    return "", "None"


def to_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8-sig")


pipeline_status, pipeline_status_source = load_single_row_json_table("pipeline_status")
scrape_status, scrape_status_source = load_single_row_json_table("scrape_status")

if not pipeline_status and pipeline_status_path.exists():
    pipeline_status = json.loads(pipeline_status_path.read_text(encoding="utf-8"))
    pipeline_status_source = "JSON"

if not scrape_status and scrape_status_path.exists():
    scrape_status = json.loads(scrape_status_path.read_text(encoding="utf-8"))
    scrape_status_source = "JSON"

history_df, history_source = load_history()
anomaly_df, anomaly_source = load_anomaly()
forecast_df, forecast_source = load_forecast()
market_brief_content, market_brief_source = load_market_brief()

if history_df.empty:
    st.error("No history data found. Run the daily pipeline first.")
    st.stop()

history_df["price_spread"] = history_df["max_price"] - history_df["min_price"]
history_df["requested_date_ad_dt"] = pd.to_datetime(history_df["requested_date_ad"], errors="coerce")
history_df["fetched_at_utc_dt"] = pd.to_datetime(history_df["fetched_at_utc"], errors="coerce").dt.tz_convert(None)
history_df["sort_date"] = history_df["requested_date_ad_dt"].fillna(history_df["fetched_at_utc_dt"])

if scrape_status:
    st.subheader("Pipeline Status")

    status_label_map = {
        "saved": "Saved",
        "no_data": "No data returned",
    }
    display_status = status_label_map.get(scrape_status.get("status"), scrape_status.get("status", "unknown"))

    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Last scrape status", display_status)
    s2.metric("Last returned BS date", scrape_status.get("returned_bs_date") or "N/A")
    s3.metric("Last scrape row count", scrape_status.get("row_count", 0))
    s4.metric("History rows", pipeline_status.get("history_rows", len(history_df)))
    s5.metric("History source", history_source)

    st.caption(
        f"Last scrape ran at: {scrape_status.get('scrape_ran_at_utc', 'N/A')} | "
        f"Requested mode: {scrape_status.get('requested_mode', 'N/A')} | "
        f"Latest saved history BS date: {pipeline_status.get('latest_history_bs_date', 'N/A')} | "
        f"Latest saved history AD date: {pipeline_status.get('latest_history_ad_date', 'N/A')} | "
        f"Anomaly source: {anomaly_source} | Forecast source: {forecast_source} | "
        f"Pipeline status source: {pipeline_status_source} | Scrape status source: {scrape_status_source}"
    )

    if scrape_status.get("status") == "no_data":
        st.warning(
            "Latest scrape returned no table rows. Dashboard charts below are based on the most recent saved history date, not necessarily the latest page label."
        )
    elif scrape_status.get("status") == "saved":
        st.success("Latest scrape saved successfully.")
else:
    st.info("Scrape status file not found yet. Run the daily pipeline first.")

if market_brief_content:
    st.subheader("Latest Market Brief")
    st.caption(f"Market brief source: {market_brief_source}")
    with st.expander("Open latest market brief", expanded=False):
        st.markdown(market_brief_content)
else:
    st.info("Market brief not found. Run the daily pipeline first.")

date_order_df = (
    history_df.groupby("scrape_date_bs", as_index=False)["sort_date"]
    .max()
    .sort_values("sort_date")
    .reset_index(drop=True)
)

available_dates = date_order_df["scrape_date_bs"].tolist()
selected_date = st.selectbox("Select market date", available_dates, index=len(available_dates) - 1)
latest_saved_bs_date = pipeline_status.get("latest_history_bs_date") or available_dates[-1]

selected_df = history_df[history_df["scrape_date_bs"] == selected_date].copy()
selected_df = selected_df.sort_values("commodity").reset_index(drop=True)

st.subheader("Market Overview")

col1, col2, col3 = st.columns(3)
col1.metric("Total Items", len(selected_df))
col2.metric("Average Price", round(selected_df["avg_price"].mean(), 2))
col3.metric("Highest Avg Price", round(selected_df["avg_price"].max(), 2))

st.subheader("Filters")

units = ["All"] + sorted(selected_df["unit"].dropna().unique().tolist())
selected_unit = st.selectbox("Select unit", units)

search_text = st.text_input("Search commodity", "")

filtered_df = selected_df.copy()

if selected_unit != "All":
    filtered_df = filtered_df[filtered_df["unit"] == selected_unit]

if search_text.strip():
    filtered_df = filtered_df[
        filtered_df["commodity"].str.contains(search_text.strip(), case=False, na=False)
    ]

filtered_df = filtered_df.sort_values("commodity").reset_index(drop=True)

st.subheader("Usable Daily Insights")

if len(filtered_df) > 0:
    most_expensive = filtered_df.sort_values("avg_price", ascending=False).iloc[0]
    cheapest = filtered_df.sort_values("avg_price", ascending=True).iloc[0]
    widest_spread = filtered_df.sort_values("price_spread", ascending=False).iloc[0]

    st.write(f"Most expensive item: **{most_expensive['commodity']}** ({most_expensive['unit']}) at average price **Rs. {most_expensive['avg_price']:.2f}**")
    st.write(f"Cheapest item: **{cheapest['commodity']}** ({cheapest['unit']}) at average price **Rs. {cheapest['avg_price']:.2f}**")
    st.write(f"Highest price spread: **{widest_spread['commodity']}** with spread **Rs. {widest_spread['price_spread']:.2f}**")
else:
    st.warning("No rows matched your filter.")

if len(date_order_df) >= 2:
    selected_idx = date_order_df.index[date_order_df["scrape_date_bs"] == selected_date][0]

    if selected_idx > 0:
        previous_date = date_order_df.iloc[selected_idx - 1]["scrape_date_bs"]
        previous_df = history_df[history_df["scrape_date_bs"] == previous_date][["commodity", "avg_price"]].rename(
            columns={"avg_price": "previous_avg_price"}
        )
        current_df = selected_df[["commodity", "avg_price"]].rename(
            columns={"avg_price": "current_avg_price"}
        )

        compare_df = current_df.merge(previous_df, on="commodity", how="inner")
        compare_df["price_change"] = compare_df["current_avg_price"] - compare_df["previous_avg_price"]

        st.subheader(f"Trend vs Previous Date ({previous_date})")

        c1, c2 = st.columns(2)

        with c1:
            st.write("Top Price Increases")
            top_up_df = compare_df.sort_values("price_change", ascending=False).head(10)
            st.dataframe(top_up_df, width="stretch")

        with c2:
            st.write("Top Price Decreases")
            top_down_df = compare_df.sort_values("price_change", ascending=True).head(10)
            st.dataframe(top_down_df, width="stretch")

st.subheader("Filtered Daily Price Table")

if len(filtered_df) > 0:
    st.download_button(
        "Download filtered daily CSV",
        data=to_csv_bytes(filtered_df),
        file_name=f"kalimati_filtered_daily_{selected_date}.csv",
        mime="text/csv",
    )

st.dataframe(filtered_df, width="stretch")

if len(filtered_df) > 0:
    st.subheader("Top 15 by Average Price")
    chart_df = filtered_df.sort_values("avg_price", ascending=False).head(15)[["commodity", "avg_price"]]
    st.bar_chart(chart_df.set_index("commodity"))

    st.subheader("Top 10 Widest Price Spread")
    spread_df = filtered_df.sort_values("price_spread", ascending=False)[
        ["commodity", "unit", "min_price", "max_price", "price_spread"]
    ].head(10)
    st.dataframe(spread_df, width="stretch")

    st.subheader("Commodity Price Trend")

    trend_options = filtered_df["commodity"].dropna().sort_values().unique().tolist()
    selected_trend_commodity = st.selectbox("Select commodity for trend", trend_options)

    trend_df = history_df[history_df["commodity"] == selected_trend_commodity].copy()
    if selected_unit != "All":
        trend_df = trend_df[trend_df["unit"] == selected_unit]

    trend_df = (
        trend_df.sort_values("sort_date")[["sort_date", "avg_price", "commodity", "unit", "scrape_date_bs"]]
        .dropna(subset=["sort_date", "avg_price"])
        .reset_index(drop=True)
    )

    if len(trend_df) > 0:
        st.caption(f"Trend history for {selected_trend_commodity}")
        st.line_chart(trend_df.set_index("sort_date")["avg_price"])

        st.download_button(
            "Download trend CSV",
            data=to_csv_bytes(trend_df.rename(columns={"sort_date": "date"})),
            file_name=f"kalimati_trend_{selected_trend_commodity}.csv",
            mime="text/csv",
        )

        st.dataframe(
            trend_df.rename(columns={"sort_date": "date"})[["date", "scrape_date_bs", "commodity", "unit", "avg_price"]],
            width="stretch"
        )
    else:
        st.info("No trend data available for the selected commodity.")

if selected_date == latest_saved_bs_date:
    if not anomaly_df.empty:
        st.subheader("Anomaly Watchlist")

        anomaly_display_df = anomaly_df.copy()
        if selected_unit != "All":
            anomaly_display_df = anomaly_display_df[anomaly_display_df["unit"] == selected_unit]

        if search_text.strip():
            anomaly_display_df = anomaly_display_df[
                anomaly_display_df["commodity"].str.contains(search_text.strip(), case=False, na=False)
            ]

        if len(anomaly_display_df) > 0:
            st.download_button(
                "Download anomaly CSV",
                data=to_csv_bytes(anomaly_display_df),
                file_name=f"kalimati_anomaly_watchlist_{selected_date}.csv",
                mime="text/csv",
            )

        c1, c2 = st.columns(2)

        with c1:
            st.write("Top Positive Spikes")
            st.dataframe(
                anomaly_display_df.sort_values("pct_change_vs_median", ascending=False)[
                    [
                        "commodity",
                        "unit",
                        "latest_bs_date",
                        "current_avg_price",
                        "baseline_median_7",
                        "pct_change_vs_median",
                    ]
                ].head(10),
                width="stretch"
            )

        with c2:
            st.write("Top Negative Drops")
            st.dataframe(
                anomaly_display_df.sort_values("pct_change_vs_median", ascending=True)[
                    [
                        "commodity",
                        "unit",
                        "latest_bs_date",
                        "current_avg_price",
                        "baseline_median_7",
                        "pct_change_vs_median",
                    ]
                ].head(10),
                width="stretch"
            )
    else:
        st.info("Anomaly report not found. Run analysis/anomaly_report.py first.")

    if not forecast_df.empty:
        st.subheader("Forecast Watchlist")

        forecast_display_df = forecast_df.copy()
        if selected_unit != "All":
            forecast_display_df = forecast_display_df[forecast_display_df["unit"] == selected_unit]

        if search_text.strip():
            forecast_display_df = forecast_display_df[
                forecast_display_df["commodity"].str.contains(search_text.strip(), case=False, na=False)
            ]

        if len(forecast_display_df) > 0:
            st.download_button(
                "Download forecast CSV",
                data=to_csv_bytes(forecast_display_df),
                file_name=f"kalimati_forecast_watchlist_{selected_date}.csv",
                mime="text/csv",
            )

        c1, c2 = st.columns(2)

        with c1:
            st.write("Expected Upward Reversion")
            st.dataframe(
                forecast_display_df.sort_values("forecast_delta_vs_latest", ascending=False)[
                    [
                        "commodity",
                        "unit",
                        "latest_bs_date",
                        "latest_avg_price",
                        "rolling_median_7",
                        "next_day_forecast",
                        "forecast_delta_vs_latest",
                    ]
                ].head(10),
                width="stretch"
            )

        with c2:
            st.write("Expected Downward Reversion")
            st.dataframe(
                forecast_display_df.sort_values("forecast_delta_vs_latest", ascending=True)[
                    [
                        "commodity",
                        "unit",
                        "latest_bs_date",
                        "latest_avg_price",
                        "rolling_median_7",
                        "next_day_forecast",
                        "forecast_delta_vs_latest",
                    ]
                ].head(10),
                width="stretch"
            )
    else:
        st.info("Forecast report not found. Run analysis/forecast_baseline.py first.")
else:
    st.info(f"Anomaly and forecast watchlists are available only for the latest saved history date: {latest_saved_bs_date}")

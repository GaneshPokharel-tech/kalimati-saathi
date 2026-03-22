import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from analysis.history_confidence import add_history_confidence

st.set_page_config(page_title="Kalimati Saathi", layout="wide")

st.title("Kalimati Saathi")
st.caption("Kalimati vegetable price intelligence prototype")

history_csv_path = Path("data/processed/kalimati_price_history.csv")
anomaly_csv_path = Path("data/processed/kalimati_anomaly_report.csv")
forecast_csv_path = Path("data/processed/kalimati_forecast_baseline.csv")
market_brief_path = Path("data/processed/kalimati_market_brief.md")
row_depth_policy_csv_path = Path("data/processed/row_depth_policy_flags.csv")
price_quality_policy_csv_path = Path("data/processed/price_quality_policy_flags.csv")
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



def load_table_with_csv_fallback(table_name, csv_path):
    df = load_table_from_sqlite(table_name)
    if not df.empty:
        return df, "SQLite"

    if csv_path.exists():
        return pd.read_csv(csv_path).copy(), "CSV"

    return pd.DataFrame(), "None"


def load_single_row_json_table(table_name):
    df = load_table_from_sqlite(table_name)
    if not df.empty:
        return df.iloc[0].to_dict(), "SQLite"
    return {}, "None"


def load_history():
    return load_table_with_csv_fallback("price_history", history_csv_path)


def load_anomaly():
    return load_table_with_csv_fallback("anomaly_report", anomaly_csv_path)


def load_forecast():
    return load_table_with_csv_fallback("forecast_baseline", forecast_csv_path)


def load_row_depth_policy():
    return load_table_with_csv_fallback("row_depth_policy_flags", row_depth_policy_csv_path)


def load_price_quality_policy():
    return load_table_with_csv_fallback("price_quality_policy_flags", price_quality_policy_csv_path)


def load_market_brief():
    df = load_table_from_sqlite("market_brief")
    if not df.empty and "brief_markdown" in df.columns:
        return str(df.iloc[0]["brief_markdown"]), "SQLite"

    if market_brief_path.exists():
        return market_brief_path.read_text(encoding="utf-8"), "Markdown"

    return "", "None"


def to_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8-sig")


def normalize_bool_series(series):
    if getattr(series, "dtype", None) == bool:
        return series.fillna(False)
    normalized = series.astype("string").fillna("").str.strip().str.lower()
    return normalized.isin(["true", "1", "yes"])


def enrich_watchlist_df(watchlist_df, row_depth_policy_df, price_quality_policy_df):
    watchlist_df = watchlist_df.copy()
    if watchlist_df.empty:
        return watchlist_df

    if "latest_is_default_model_window" in watchlist_df.columns:
        watchlist_df["latest_is_default_model_window"] = normalize_bool_series(
            watchlist_df["latest_is_default_model_window"]
        )
    else:
        watchlist_df["latest_is_default_model_window"] = False

    if not row_depth_policy_df.empty:
        row_depth_watch = row_depth_policy_df[
            [
                "scrape_date_bs",
                "row_depth_severity",
                "exclude_from_default_model_window",
                "manual_review",
                "policy_action",
            ]
        ].drop_duplicates().rename(
            columns={
                "scrape_date_bs": "latest_bs_date",
                "row_depth_severity": "latest_row_depth_flag",
                "exclude_from_default_model_window": "latest_row_depth_excluded",
                "manual_review": "latest_row_depth_manual_review",
                "policy_action": "latest_row_depth_policy_action",
            }
        )
        watchlist_df = watchlist_df.merge(row_depth_watch, on="latest_bs_date", how="left")
    else:
        watchlist_df["latest_row_depth_flag"] = pd.NA
        watchlist_df["latest_row_depth_excluded"] = False
        watchlist_df["latest_row_depth_manual_review"] = False
        watchlist_df["latest_row_depth_policy_action"] = pd.NA

    if not price_quality_policy_df.empty:
        price_quality_watch = price_quality_policy_df[
            [
                "scrape_date_bs",
                "commodity",
                "unit",
                "price_issue_type",
                "exclude_from_default_model_window",
                "manual_review",
                "policy_action",
            ]
        ].drop_duplicates().rename(
            columns={
                "scrape_date_bs": "latest_bs_date",
                "price_issue_type": "latest_price_quality_flag",
                "exclude_from_default_model_window": "latest_price_quality_excluded",
                "manual_review": "latest_price_quality_manual_review",
                "policy_action": "latest_price_quality_policy_action",
            }
        )
        watchlist_df = watchlist_df.merge(
            price_quality_watch,
            on=["latest_bs_date", "commodity", "unit"],
            how="left",
        )
    else:
        watchlist_df["latest_price_quality_flag"] = pd.NA
        watchlist_df["latest_price_quality_excluded"] = False
        watchlist_df["latest_price_quality_manual_review"] = False
        watchlist_df["latest_price_quality_policy_action"] = pd.NA

    watchlist_df["latest_row_depth_excluded"] = normalize_bool_series(
        watchlist_df["latest_row_depth_excluded"]
    )
    watchlist_df["latest_row_depth_manual_review"] = normalize_bool_series(
        watchlist_df["latest_row_depth_manual_review"]
    )
    watchlist_df["latest_price_quality_excluded"] = normalize_bool_series(
        watchlist_df["latest_price_quality_excluded"]
    )
    watchlist_df["latest_price_quality_manual_review"] = normalize_bool_series(
        watchlist_df["latest_price_quality_manual_review"]
    )

    watchlist_df["latest_policy_manual_review"] = (
        watchlist_df["latest_row_depth_manual_review"]
        | watchlist_df["latest_price_quality_manual_review"]
    )
    watchlist_df["latest_is_policy_excluded_from_default_model_window"] = (
        watchlist_df["latest_row_depth_excluded"]
        | watchlist_df["latest_price_quality_excluded"]
    )
    watchlist_df["latest_is_safe_default_row"] = (
        watchlist_df["latest_is_default_model_window"]
        & ~watchlist_df["latest_is_policy_excluded_from_default_model_window"]
    )

    return watchlist_df


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
row_depth_policy_df, row_depth_policy_source = load_row_depth_policy()
price_quality_policy_df, price_quality_policy_source = load_price_quality_policy()
market_brief_content, market_brief_source = load_market_brief()

if history_df.empty:
    st.error("No history data found. Run the daily pipeline first.")
    st.stop()

history_df["price_spread"] = history_df["max_price"] - history_df["min_price"]
history_df["requested_date_ad_dt"] = pd.to_datetime(history_df["requested_date_ad"], errors="coerce")
history_df["fetched_at_utc_dt"] = pd.to_datetime(history_df["fetched_at_utc"], errors="coerce").dt.tz_convert(None)
history_df["sort_date"] = history_df["requested_date_ad_dt"].fillna(history_df["fetched_at_utc_dt"])

if "history_confidence_band" not in history_df.columns:
    history_df = add_history_confidence(history_df)

if not row_depth_policy_df.empty:
    row_depth_join_df = row_depth_policy_df[
        [
            "requested_date_ad",
            "scrape_date_bs",
            "row_count",
            "row_depth_severity",
            "exclude_from_default_model_window",
            "manual_review",
            "policy_action",
        ]
    ].drop_duplicates().rename(
        columns={
            "row_count": "row_depth_row_count",
            "row_depth_severity": "row_depth_flag",
            "exclude_from_default_model_window": "row_depth_excluded",
            "manual_review": "row_depth_manual_review",
            "policy_action": "row_depth_policy_action",
        }
    )
    history_df = history_df.merge(
        row_depth_join_df,
        on=["requested_date_ad", "scrape_date_bs"],
        how="left",
    )
else:
    history_df["row_depth_row_count"] = pd.NA
    history_df["row_depth_flag"] = pd.NA
    history_df["row_depth_excluded"] = False
    history_df["row_depth_manual_review"] = False
    history_df["row_depth_policy_action"] = pd.NA

if not price_quality_policy_df.empty:
    price_quality_join_df = price_quality_policy_df[
        [
            "requested_date_ad",
            "scrape_date_bs",
            "commodity",
            "unit",
            "price_issue_type",
            "exclude_from_default_model_window",
            "manual_review",
            "policy_action",
        ]
    ].drop_duplicates().rename(
        columns={
            "price_issue_type": "price_quality_flag",
            "exclude_from_default_model_window": "price_quality_excluded",
            "manual_review": "price_quality_manual_review",
            "policy_action": "price_quality_policy_action",
        }
    )
    history_df = history_df.merge(
        price_quality_join_df,
        on=["requested_date_ad", "scrape_date_bs", "commodity", "unit"],
        how="left",
    )
else:
    history_df["price_quality_flag"] = pd.NA
    history_df["price_quality_excluded"] = False
    history_df["price_quality_manual_review"] = False
    history_df["price_quality_policy_action"] = pd.NA

history_df["row_depth_excluded"] = normalize_bool_series(history_df["row_depth_excluded"])
history_df["row_depth_manual_review"] = normalize_bool_series(history_df["row_depth_manual_review"])
history_df["price_quality_excluded"] = normalize_bool_series(history_df["price_quality_excluded"])
history_df["price_quality_manual_review"] = normalize_bool_series(history_df["price_quality_manual_review"])

history_df["policy_manual_review"] = (
    history_df["row_depth_manual_review"] | history_df["price_quality_manual_review"]
)
history_df["is_policy_excluded_from_default_model_window"] = (
    history_df["row_depth_excluded"] | history_df["price_quality_excluded"]
)
history_df["is_safe_default_row"] = (
    normalize_bool_series(history_df["is_default_model_window"])
    & ~history_df["is_policy_excluded_from_default_model_window"]
)

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


st.caption(
    f"Trust layer sources: confidence=helper | "
    f"row-depth policy={row_depth_policy_source} ({len(row_depth_policy_df):,} rows) | "
    f"price-quality policy={price_quality_policy_source} ({len(price_quality_policy_df):,} rows)"
)

date_order_df = (
    history_df.groupby("scrape_date_bs", as_index=False)["sort_date"]
    .max()
    .sort_values("sort_date")
    .reset_index(drop=True)
)

available_dates = date_order_df["scrape_date_bs"].tolist()
st.header("Selected Date Explorer")
st.caption(
    "यो भाग selected market date मा आधारित छ. Trend vs Previous Date ले calendar previous day होइन, selected date भन्दा अघिल्लो available data date सँग compare गर्छ."
)

selected_date = st.selectbox("Select market date", available_dates, index=len(available_dates) - 1)
latest_saved_bs_date = pipeline_status.get("latest_history_bs_date") or available_dates[-1]

view_mode = st.radio(
    "History view",
    ["Safe default window", "Full raw history"],
    horizontal=True,
    help="Safe default window prefers the default model window and excludes policy-flagged rows. Full raw history keeps all rows visible for audit.",
)

selected_raw_df = history_df[history_df["scrape_date_bs"] == selected_date].copy()

if view_mode == "Safe default window":
    selected_df = selected_raw_df[selected_raw_df["is_safe_default_row"]].copy()
else:
    selected_df = selected_raw_df.copy()

selected_df = selected_df.sort_values(["commodity", "unit"]).reset_index(drop=True)

st.caption(
    f"Selected date view: showing {len(selected_df):,} of {len(selected_raw_df):,} rows | "
    f"policy-excluded rows on this date: {int(selected_raw_df['is_policy_excluded_from_default_model_window'].sum())} | "
    f"manual-review rows on this date: {int(selected_raw_df['policy_manual_review'].sum())}"
)

if selected_df.empty and len(selected_raw_df) > 0 and view_mode == "Safe default window":
    st.warning("No rows remain in the safe default window for this date. Switch to Full raw history to inspect all rows.")

if len(selected_raw_df) > 0:
    st.subheader("Trust Summary")
    confidence_counts = (
        selected_raw_df["history_confidence_band"]
        .fillna("unknown")
        .value_counts()
        .to_dict()
    )
    confidence_summary = ", ".join([f"{k}: {v}" for k, v in confidence_counts.items()]) if confidence_counts else "none"

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Visible rows", len(selected_df))
    t2.metric("Default window rows", int(normalize_bool_series(selected_raw_df["is_default_model_window"]).sum()))
    t3.metric("Policy-excluded rows", int(selected_raw_df["is_policy_excluded_from_default_model_window"].sum()))
    t4.metric("Manual-review rows", int(selected_raw_df["policy_manual_review"].sum()))

    st.caption(f"Confidence bands on selected date: {confidence_summary}")

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

        previous_df = (
            history_df[history_df["scrape_date_bs"] == previous_date][["commodity", "unit", "avg_price"]]
            .rename(columns={"avg_price": "previous_avg_price"})
            .copy()
        )

        current_df = (
            selected_df[["commodity", "unit", "avg_price"]]
            .rename(columns={"avg_price": "current_avg_price"})
            .copy()
        )

        compare_df = current_df.merge(
            previous_df,
            on=["commodity", "unit"],
            how="inner",
        )

        compare_df["price_change"] = (
            compare_df["current_avg_price"] - compare_df["previous_avg_price"]
        )

        st.subheader(f"Trend vs Previous Date ({previous_date})")
        st.caption("यो compare selected date भन्दा अघिल्लो available data date सँग हो, calendar previous day सँग होइन.")
        c1, c2 = st.columns(2)

        display_cols = [
            "commodity",
            "unit",
            "previous_avg_price",
            "current_avg_price",
            "price_change",
        ]

        with c1:
            st.write("Top Price Increases")
            top_up_df = compare_df.sort_values("price_change", ascending=False).head(10)
            st.dataframe(top_up_df[display_cols], width="stretch")

        with c2:
            st.write("Top Price Decreases")
            top_down_df = compare_df.sort_values("price_change", ascending=True).head(10)
            st.dataframe(top_down_df[display_cols], width="stretch")

st.subheader("Filtered Daily Price Table")

if len(filtered_df) > 0:
    st.download_button(
        "Download filtered daily CSV",
        data=to_csv_bytes(filtered_df),
        file_name=f"kalimati_filtered_daily_{selected_date}.csv",
        mime="text/csv",
    )

table_display_cols = [
    "commodity",
    "unit",
    "min_price",
    "max_price",
    "avg_price",
    "price_spread",
    "history_confidence_band",
    "is_default_model_window",
    "is_policy_excluded_from_default_model_window",
    "row_depth_flag",
    "price_quality_flag",
    "policy_manual_review",
]

table_display_cols = [col for col in table_display_cols if col in filtered_df.columns]

st.dataframe(filtered_df[table_display_cols], width="stretch")

if len(filtered_df) > 0:
    st.subheader("Top 15 by Average Price")
    chart_df = filtered_df.sort_values("avg_price", ascending=False).head(15).copy()
    chart_df["item_label"] = (
        chart_df["commodity"].fillna("").astype(str)
        + " ("
        + chart_df["unit"].fillna("N/A").astype(str)
        + ")"
    )
    st.bar_chart(chart_df.set_index("item_label")["avg_price"])

    st.subheader("Top 10 Widest Price Spread")
    spread_df = filtered_df.sort_values("price_spread", ascending=False)[
        ["commodity", "unit", "min_price", "max_price", "price_spread"]
    ].head(10)
    st.dataframe(spread_df, width="stretch")

    st.header("Commodity History")
    st.caption("यो भाग commodity + unit specific history का लागि हो. Trend chart selected commodity/unit को पुरानो usable history देखाउँछ.")
    st.subheader("Commodity Price Trend")

trend_source_df = filtered_df.copy()
trend_source_df = trend_source_df.dropna(subset=["commodity"])

if len(trend_source_df) > 0:
    trend_source_df["commodity_key"] = (
        trend_source_df["commodity"].fillna("").astype(str)
        + " ("
        + trend_source_df["unit"].fillna("N/A").astype(str)
        + ")"
    )

    trend_options = sorted(trend_source_df["commodity_key"].unique().tolist())
    selected_trend_key = st.selectbox("Select commodity for trend", trend_options)

    selected_trend_row = trend_source_df[
        trend_source_df["commodity_key"] == selected_trend_key
    ].iloc[0]

    selected_trend_commodity = selected_trend_row["commodity"]
    selected_trend_unit = selected_trend_row["unit"]
    selected_trend_unit_display = "N/A" if pd.isna(selected_trend_unit) else str(selected_trend_unit)

    if pd.isna(selected_trend_unit):
        trend_df = history_df[
            (history_df["commodity"] == selected_trend_commodity)
            & (history_df["unit"].isna())
        ].copy()
    else:
        trend_df = history_df[
            (history_df["commodity"] == selected_trend_commodity)
            & (history_df["unit"] == selected_trend_unit)
        ].copy()

    trend_df = (
        trend_df.sort_values("sort_date")[["sort_date", "avg_price", "commodity", "unit", "scrape_date_bs"]]
        .dropna(subset=["sort_date", "avg_price"])
        .reset_index(drop=True)
    )

    if len(trend_df) > 0:
        st.caption(f"Trend history for {selected_trend_commodity} ({selected_trend_unit_display})")
        st.line_chart(trend_df.set_index("sort_date")["avg_price"])

        safe_commodity = str(selected_trend_commodity).replace("/", "-").replace(" ", "_")
        safe_unit = selected_trend_unit_display.replace("/", "-").replace(" ", "_")

        st.download_button(
            "Download trend CSV",
            data=to_csv_bytes(trend_df.rename(columns={"sort_date": "date"})),
            file_name=f"kalimati_trend_{safe_commodity}_{safe_unit}.csv",
            mime="text/csv",
        )

        st.dataframe(
            trend_df.rename(columns={"sort_date": "date"})[
                ["date", "scrape_date_bs", "commodity", "unit", "avg_price"]
            ],
            width="stretch",
        )
    else:
        st.info("No trend data available for the selected commodity.")
else:
    st.info("No trend data available for the selected commodity.")

st.header("Latest Snapshot")
st.caption(
    f"यो भाग selected date मा होइन, latest saved history date मा मात्र आधारित छ: {latest_saved_bs_date}"
)

if selected_date == latest_saved_bs_date:
    if not anomaly_df.empty:
        st.subheader("Anomaly Watchlist")

        anomaly_display_df = anomaly_df.copy()
        anomaly_display_df = anomaly_display_df[
            anomaly_display_df["latest_bs_date"] == latest_saved_bs_date
        ].copy()
        anomaly_display_df = enrich_watchlist_df(
            anomaly_display_df,
            row_depth_policy_df,
            price_quality_policy_df,
        )

        if view_mode == "Safe default window":
            anomaly_display_df = anomaly_display_df[
                anomaly_display_df["latest_is_safe_default_row"]
            ].copy()

        if selected_unit != "All":
            anomaly_display_df = anomaly_display_df[
                anomaly_display_df["unit"] == selected_unit
            ]

        if search_text.strip():
            anomaly_display_df = anomaly_display_df[
                anomaly_display_df["commodity"].str.contains(
                    search_text.strip(), case=False, na=False
                )
            ]

        if len(anomaly_display_df) > 0:
            st.caption(
                f"Anomaly watchlist scoped to latest saved date {latest_saved_bs_date} | "
                f"rows: {len(anomaly_display_df):,} | "
                f"manual-review rows: {int(anomaly_display_df['latest_policy_manual_review'].sum())} | "
                f"policy-excluded rows: {int(anomaly_display_df['latest_is_policy_excluded_from_default_model_window'].sum())}"
            )

            st.download_button(
                "Download anomaly CSV",
                data=to_csv_bytes(anomaly_display_df),
                file_name=f"kalimati_anomaly_watchlist_{selected_date}.csv",
                mime="text/csv",
            )

            anomaly_cols = [
                "commodity",
                "unit",
                "latest_bs_date",
                "current_avg_price",
                "baseline_median_7",
                "pct_change_vs_median",
                "latest_history_confidence_band",
                "latest_is_default_model_window",
                "latest_is_policy_excluded_from_default_model_window",
                "latest_price_quality_flag",
                "latest_policy_manual_review",
            ]
            anomaly_cols = [col for col in anomaly_cols if col in anomaly_display_df.columns]

            c1, c2 = st.columns(2)

            with c1:
                st.write("Top Positive Spikes")
                st.dataframe(
                    anomaly_display_df.sort_values(
                        "pct_change_vs_median", ascending=False
                    )[anomaly_cols].head(10),
                    width="stretch",
                )

            with c2:
                st.write("Top Negative Drops")
                st.dataframe(
                    anomaly_display_df.sort_values(
                        "pct_change_vs_median", ascending=True
                    )[anomaly_cols].head(10),
                    width="stretch",
                )
        else:
            st.info(
                "No anomaly rows remain after latest-date scoping and current trust/filter selection."
            )
    else:
        st.info("Anomaly report not found. Run analysis/anomaly_report.py first.")

    if not forecast_df.empty:
        st.subheader("Forecast Watchlist")

        forecast_display_df = forecast_df.copy()
        forecast_display_df = forecast_display_df[
            forecast_display_df["latest_bs_date"] == latest_saved_bs_date
        ].copy()
        forecast_display_df = enrich_watchlist_df(
            forecast_display_df,
            row_depth_policy_df,
            price_quality_policy_df,
        )

        if view_mode == "Safe default window":
            forecast_display_df = forecast_display_df[
                forecast_display_df["latest_is_safe_default_row"]
            ].copy()

        if selected_unit != "All":
            forecast_display_df = forecast_display_df[
                forecast_display_df["unit"] == selected_unit
            ]

        if search_text.strip():
            forecast_display_df = forecast_display_df[
                forecast_display_df["commodity"].str.contains(
                    search_text.strip(), case=False, na=False
                )
            ]

        if len(forecast_display_df) > 0:
            st.caption(
                f"Forecast watchlist scoped to latest saved date {latest_saved_bs_date} | "
                f"rows: {len(forecast_display_df):,} | "
                f"manual-review rows: {int(forecast_display_df['latest_policy_manual_review'].sum())} | "
                f"policy-excluded rows: {int(forecast_display_df['latest_is_policy_excluded_from_default_model_window'].sum())}"
            )

            st.download_button(
                "Download forecast CSV",
                data=to_csv_bytes(forecast_display_df),
                file_name=f"kalimati_forecast_watchlist_{selected_date}.csv",
                mime="text/csv",
            )

            forecast_cols = [
                "commodity",
                "unit",
                "latest_bs_date",
                "latest_avg_price",
                "rolling_median_7",
                "next_day_forecast",
                "forecast_delta_vs_latest",
                "latest_history_confidence_band",
                "latest_is_default_model_window",
                "latest_is_policy_excluded_from_default_model_window",
                "latest_price_quality_flag",
                "latest_policy_manual_review",
            ]
            forecast_cols = [col for col in forecast_cols if col in forecast_display_df.columns]

            c1, c2 = st.columns(2)

            with c1:
                st.write("Expected Upward Reversion")
                st.dataframe(
                    forecast_display_df.sort_values(
                        "forecast_delta_vs_latest", ascending=False
                    )[forecast_cols].head(10),
                    width="stretch",
                )

            with c2:
                st.write("Expected Downward Reversion")
                st.dataframe(
                    forecast_display_df.sort_values(
                        "forecast_delta_vs_latest", ascending=True
                    )[forecast_cols].head(10),
                    width="stretch",
                )
        else:
            st.info(
                "No forecast rows remain after latest-date scoping and current trust/filter selection."
            )
    else:
        st.info("Forecast report not found. Run analysis/forecast_baseline.py first.")
else:
    st.info(
        f"Anomaly and forecast watchlists are shown only for the latest saved history date: {latest_saved_bs_date}"
    )

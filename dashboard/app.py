"""
Kalimati Saathi — Vegetable price intelligence dashboard for Kalimati market, Nepal.
"""
import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from analysis.history_confidence import add_history_confidence

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kalimati Saathi",
    page_icon="🥦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
}
.stApp { background-color: #0f172a; color: #e2e8f0; }
.block-container { padding: 1.5rem 2rem; max-width: 1400px; }

[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1e293b 0%, #1a2744 100%);
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 1rem 1.2rem !important;
    box-shadow: 0 4px 6px rgba(0,0,0,0.3);
}
[data-testid="metric-container"] [data-testid="metric-label"] {
    color: #94a3b8 !important;
    font-size: 0.75rem !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
}
[data-testid="metric-container"] [data-testid="metric-value"] {
    color: #f1f5f9 !important;
    font-size: 1.5rem !important;
    font-weight: 700 !important;
}

.stTabs [data-baseweb="tab-list"] {
    background-color: #1e293b;
    border-radius: 10px;
    padding: 4px;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    background-color: transparent;
    border-radius: 8px;
    color: #94a3b8;
    font-weight: 500;
    padding: 8px 16px;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #3b82f6, #6366f1) !important;
    color: #ffffff !important;
}
.stTabs [data-baseweb="tab-panel"] { padding-top: 1.5rem; }

h1, h2, h3 { color: #f1f5f9 !important; }

[data-testid="stExpander"] {
    background-color: #1e293b;
    border: 1px solid #334155;
    border-radius: 10px;
}

[data-testid="stSidebar"] {
    background-color: #1e293b;
    border-right: 1px solid #334155;
}
hr { border-color: #334155; }

[data-testid="stDownloadButton"] button {
    background-color: #1e293b;
    border: 1px solid #334155;
    color: #94a3b8;
    border-radius: 8px;
}
[data-testid="stDownloadButton"] button:hover {
    border-color: #3b82f6;
    color: #3b82f6;
}
</style>
""",
    unsafe_allow_html=True,
)

# ── Paths ─────────────────────────────────────────────────────────────────────
HISTORY_CSV = PROJECT_ROOT / "data/processed/kalimati_price_history.csv"
ANOMALY_CSV = PROJECT_ROOT / "data/processed/kalimati_anomaly_report.csv"
FORECAST_CSV = PROJECT_ROOT / "data/processed/kalimati_forecast_baseline.csv"
MARKET_BRIEF_MD = PROJECT_ROOT / "data/processed/kalimati_market_brief.md"
ROW_DEPTH_CSV = PROJECT_ROOT / "data/processed/row_depth_policy_flags.csv"
PRICE_QUALITY_CSV = PROJECT_ROOT / "data/processed/price_quality_policy_flags.csv"
SQLITE_DB = PROJECT_ROOT / "data/processed/kalimati.db"
PIPELINE_STATUS_JSON = PROJECT_ROOT / "data/processed/kalimati_pipeline_status.json"
SCRAPE_STATUS_JSON = PROJECT_ROOT / "data/processed/kalimati_last_scrape_status.json"

_ALLOWED_TABLES = {
    "price_history",
    "anomaly_report",
    "forecast_baseline",
    "market_brief",
    "pipeline_status",
    "scrape_status",
    "row_depth_policy_flags",
    "price_quality_policy_flags",
}

# ── Data helpers ──────────────────────────────────────────────────────────────


def _sqlite_table(table_name: str) -> pd.DataFrame:
    if table_name not in _ALLOWED_TABLES or not SQLITE_DB.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(SQLITE_DB)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)  # noqa: S608
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def _load_with_fallback(table_name: str, csv_path: Path) -> pd.DataFrame:
    df = _sqlite_table(table_name)
    if not df.empty:
        return df
    if csv_path.exists():
        try:
            return pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            pass
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_history() -> pd.DataFrame:
    return _load_with_fallback("price_history", HISTORY_CSV)


@st.cache_data(ttl=300)
def load_anomaly() -> pd.DataFrame:
    return _load_with_fallback("anomaly_report", ANOMALY_CSV)


@st.cache_data(ttl=300)
def load_forecast() -> pd.DataFrame:
    return _load_with_fallback("forecast_baseline", FORECAST_CSV)


@st.cache_data(ttl=300)
def load_row_depth() -> pd.DataFrame:
    return _load_with_fallback("row_depth_policy_flags", ROW_DEPTH_CSV)


@st.cache_data(ttl=300)
def load_price_quality() -> pd.DataFrame:
    return _load_with_fallback("price_quality_policy_flags", PRICE_QUALITY_CSV)


@st.cache_data(ttl=300)
def load_market_brief() -> str:
    df = _sqlite_table("market_brief")
    if not df.empty and "brief_markdown" in df.columns:
        return str(df.iloc[0]["brief_markdown"])
    if MARKET_BRIEF_MD.exists():
        return MARKET_BRIEF_MD.read_text(encoding="utf-8")
    return ""


@st.cache_data(ttl=300)
def load_pipeline_status() -> dict:
    df = _sqlite_table("pipeline_status")
    if not df.empty:
        return df.iloc[0].to_dict()
    if PIPELINE_STATUS_JSON.exists():
        return json.loads(PIPELINE_STATUS_JSON.read_text(encoding="utf-8"))
    return {}


@st.cache_data(ttl=300)
def load_scrape_status() -> dict:
    df = _sqlite_table("scrape_status")
    if not df.empty:
        return df.iloc[0].to_dict()
    if SCRAPE_STATUS_JSON.exists():
        return json.loads(SCRAPE_STATUS_JSON.read_text(encoding="utf-8"))
    return {}


def normalize_bool(series: pd.Series) -> pd.Series:
    if getattr(series, "dtype", None) == bool:
        return series.fillna(False)
    return (
        series.astype("string").fillna("").str.strip().str.lower().isin(["true", "1", "yes"])
    )


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def kpi_card(label: str, value: str, color: str = "#3b82f6") -> str:
    return f"""
    <div style="background:linear-gradient(135deg,#1e293b,#1a2744);
                border:1px solid #334155;border-radius:12px;padding:1.2rem 1.4rem;
                border-top:3px solid {color};margin-bottom:0.5rem;">
        <div style="color:#94a3b8;font-size:0.7rem;text-transform:uppercase;
                    letter-spacing:0.08em;font-weight:600;margin-bottom:0.4rem;">{label}</div>
        <div style="color:#f1f5f9;font-size:1.6rem;font-weight:700;line-height:1.2;">{value}</div>
    </div>"""


_CHART_CFG = dict(
    gridColor="#334155",
    labelColor="#94a3b8",
    titleColor="#94a3b8",
    domainColor="#334155",
)

# ── Load all data ─────────────────────────────────────────────────────────────
raw_history = load_history()
if raw_history.empty:
    st.error("No price history found. Run the daily pipeline first.")
    st.stop()

pipeline_status = load_pipeline_status()
scrape_status = load_scrape_status()
anomaly_df = load_anomaly()
forecast_df = load_forecast()
row_depth_df = load_row_depth()
price_quality_df = load_price_quality()
market_brief = load_market_brief()

# ── Enrich history ────────────────────────────────────────────────────────────
history_df = raw_history.copy()
history_df["requested_date_ad_dt"] = pd.to_datetime(
    history_df.get("requested_date_ad"), errors="coerce"
)
if "fetched_at_utc" in history_df.columns:
    history_df["fetched_at_utc_dt"] = pd.to_datetime(
        history_df["fetched_at_utc"], errors="coerce", utc=True
    ).dt.tz_convert(None)
else:
    history_df["fetched_at_utc_dt"] = pd.NaT
history_df["sort_date"] = history_df["requested_date_ad_dt"].fillna(
    history_df["fetched_at_utc_dt"]
)
history_df["price_spread"] = history_df["max_price"] - history_df["min_price"]

if "history_confidence_band" not in history_df.columns:
    history_df = add_history_confidence(history_df)

# Merge row-depth policy flags
if not row_depth_df.empty:
    rd_join = (
        row_depth_df[
            [
                "requested_date_ad",
                "scrape_date_bs",
                "row_count",
                "row_depth_severity",
                "exclude_from_default_model_window",
                "manual_review",
                "policy_action",
            ]
        ]
        .drop_duplicates()
        .rename(
            columns={
                "row_count": "row_depth_row_count",
                "row_depth_severity": "row_depth_flag",
                "exclude_from_default_model_window": "row_depth_excluded",
                "manual_review": "row_depth_manual_review",
                "policy_action": "row_depth_policy_action",
            }
        )
    )
    history_df = history_df.merge(
        rd_join, on=["requested_date_ad", "scrape_date_bs"], how="left"
    )
else:
    for c in [
        "row_depth_row_count",
        "row_depth_flag",
        "row_depth_excluded",
        "row_depth_manual_review",
        "row_depth_policy_action",
    ]:
        history_df[c] = pd.NA

# Merge price-quality policy flags
if not price_quality_df.empty:
    pq_join = (
        price_quality_df[
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
        ]
        .drop_duplicates()
        .rename(
            columns={
                "price_issue_type": "price_quality_flag",
                "exclude_from_default_model_window": "price_quality_excluded",
                "manual_review": "price_quality_manual_review",
                "policy_action": "price_quality_policy_action",
            }
        )
    )
    history_df = history_df.merge(
        pq_join,
        on=["requested_date_ad", "scrape_date_bs", "commodity", "unit"],
        how="left",
    )
else:
    for c in [
        "price_quality_flag",
        "price_quality_excluded",
        "price_quality_manual_review",
        "price_quality_policy_action",
    ]:
        history_df[c] = pd.NA

for col in [
    "row_depth_excluded",
    "row_depth_manual_review",
    "price_quality_excluded",
    "price_quality_manual_review",
]:
    history_df[col] = normalize_bool(history_df[col])

history_df["policy_manual_review"] = (
    history_df["row_depth_manual_review"] | history_df["price_quality_manual_review"]
)
history_df["is_policy_excluded"] = (
    history_df["row_depth_excluded"] | history_df["price_quality_excluded"]
)
history_df["is_safe_default_row"] = normalize_bool(
    history_df.get("is_default_model_window", pd.Series(False, index=history_df.index))
) & ~history_df["is_policy_excluded"]

# Date ordering
date_order = (
    history_df.groupby("scrape_date_bs", as_index=False)["sort_date"]
    .max()
    .sort_values("sort_date")
    .reset_index(drop=True)
)
available_dates = date_order["scrape_date_bs"].tolist()
latest_bs = pipeline_status.get("latest_history_bs_date") or (
    available_dates[-1] if available_dates else ""
)

# ── Hero header ───────────────────────────────────────────────────────────────
_status = scrape_status.get("status", "unknown")
_status_dot = "🟢" if _status == "saved" else "🟡"
_scrape_rows = scrape_status.get("row_count", 0)

st.markdown(
    f"""
<div style="background:linear-gradient(135deg,#1e293b 0%,#0f172a 100%);
            border:1px solid #334155;border-radius:16px;padding:1.5rem 2rem;
            margin-bottom:1.5rem;display:flex;justify-content:space-between;align-items:center;">
    <div>
        <div style="font-size:1.8rem;font-weight:800;color:#f1f5f9;letter-spacing:-0.03em;">
            🥦 Kalimati Saathi
        </div>
        <div style="color:#94a3b8;font-size:0.9rem;margin-top:4px;">
            Kalimati vegetable market · Price intelligence for Nepal
        </div>
    </div>
    <div style="text-align:right;">
        <div style="color:#94a3b8;font-size:0.72rem;text-transform:uppercase;
                    letter-spacing:0.08em;">Latest Market Date</div>
        <div style="color:#38bdf8;font-size:1.4rem;font-weight:700;">{latest_bs}</div>
        <div style="color:#64748b;font-size:0.75rem;margin-top:4px;">
            {_status_dot} {_scrape_rows} items · {_status}
        </div>
    </div>
</div>
""",
    unsafe_allow_html=True,
)

if _status == "no_data":
    st.warning(
        f"Latest scrape returned no data. Dashboard shows last saved date: **{latest_bs}**"
    )

# ── KPI row ───────────────────────────────────────────────────────────────────
_latest_snap = history_df[history_df["scrape_date_bs"] == latest_bs]
_kc = st.columns(5)
_kc[0].markdown(
    kpi_card("History Rows", f"{len(history_df):,}", "#3b82f6"), unsafe_allow_html=True
)
_kc[1].markdown(kpi_card("Latest Date", str(latest_bs), "#8b5cf6"), unsafe_allow_html=True)
_kc[2].markdown(
    kpi_card("Items (Latest Date)", f"{len(_latest_snap):,}", "#06b6d4"),
    unsafe_allow_html=True,
)
_kc[3].markdown(
    kpi_card("Forecast Series", f"{len(forecast_df):,}", "#f59e0b"), unsafe_allow_html=True
)
_kc[4].markdown(
    kpi_card("Anomaly Signals", f"{len(anomaly_df):,}", "#ef4444"), unsafe_allow_html=True
)

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_explorer, tab_anomaly, tab_forecast, tab_trends, tab_quality = st.tabs(
    [
        "📊 Overview",
        "🔍 Price Explorer",
        "⚠️ Anomaly Watchlist",
        "📈 Forecast",
        "📉 Commodity Trends",
        "🛡 Data Quality",
    ]
)

# =============================================================================
# TAB 1 · OVERVIEW
# =============================================================================
with tab_overview:
    if market_brief:
        with st.expander("📄 Latest Market Brief", expanded=True):
            st.markdown(market_brief)

    st.subheader("Top 10 Commodities by Average Price")
    top10 = _latest_snap.dropna(subset=["avg_price"]).nlargest(10, "avg_price").copy()
    if not top10.empty:
        top10["label"] = (
            top10["commodity"].astype(str)
            + " ("
            + top10["unit"].fillna("").astype(str)
            + ")"
        )
        chart_top10 = (
            alt.Chart(top10)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("avg_price:Q", title="Average Price (Rs.)"),
                y=alt.Y("label:N", sort="-x", title=""),
                color=alt.Color(
                    "avg_price:Q", scale=alt.Scale(scheme="blues"), legend=None
                ),
                tooltip=[
                    "commodity:N",
                    "unit:N",
                    "min_price:Q",
                    "max_price:Q",
                    "avg_price:Q",
                ],
            )
            .properties(height=300)
            .configure_view(strokeWidth=0)
            .configure_axis(**_CHART_CFG)
        )
        st.altair_chart(chart_top10, use_container_width=True)
    else:
        st.info("No price data for the latest date.")

    st.subheader("Pipeline Status")
    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("Last Scrape Status", (_status or "N/A").upper())
    sc2.metric("Returned BS Date", scrape_status.get("returned_bs_date") or "N/A")
    sc3.metric(
        "History Rows", f"{pipeline_status.get('history_rows', len(history_df)):,}"
    )
    sc4.metric("Latest AD Date", pipeline_status.get("latest_history_ad_date") or "N/A")
    st.caption(
        f"Pipeline ran: {pipeline_status.get('pipeline_ran_at_utc', 'N/A')} · "
        f"Scrape ran: {scrape_status.get('scrape_ran_at_utc', 'N/A')}"
    )

# =============================================================================
# TAB 2 · PRICE EXPLORER
# =============================================================================
with tab_explorer:
    col_filter, col_main = st.columns([1, 3])

    with col_filter:
        st.markdown("**Filters**")
        selected_date = st.selectbox(
            "Market date", available_dates, index=len(available_dates) - 1
        )
        view_mode = st.radio(
            "View mode",
            ["Safe default window", "Full raw history"],
            help="Safe mode excludes policy-flagged rows.",
        )
        units = ["All"] + sorted(history_df["unit"].dropna().unique().tolist())
        selected_unit = st.selectbox("Unit filter", units)
        search_text = st.text_input("Search commodity", "")

    with col_main:
        date_df = history_df[history_df["scrape_date_bs"] == selected_date].copy()
        view_df = (
            date_df[date_df["is_safe_default_row"]].copy()
            if view_mode == "Safe default window"
            else date_df.copy()
        )
        if selected_unit != "All":
            view_df = view_df[view_df["unit"] == selected_unit]
        if search_text.strip():
            view_df = view_df[
                view_df["commodity"].str.contains(search_text.strip(), case=False, na=False)
            ]
        view_df = view_df.sort_values("commodity").reset_index(drop=True)

        st.caption(
            f"Showing **{len(view_df):,}** of **{len(date_df):,}** rows for `{selected_date}` · "
            f"Policy-excluded: {int(date_df['is_policy_excluded'].sum())} · "
            f"Manual-review: {int(date_df['policy_manual_review'].sum())}"
        )

        if len(view_df) > 0:
            most_exp = view_df.nlargest(1, "avg_price").iloc[0]
            cheapest_row = view_df.nsmallest(1, "avg_price").iloc[0]
            ic1, ic2, ic3 = st.columns(3)
            ic1.metric("Items shown", len(view_df))
            ic2.metric(
                "Most expensive",
                f"Rs. {most_exp['avg_price']:.0f}",
                most_exp["commodity"],
            )
            ic3.metric(
                "Cheapest",
                f"Rs. {cheapest_row['avg_price']:.0f}",
                cheapest_row["commodity"],
            )

            chart_df = view_df.nlargest(15, "avg_price").copy()
            chart_df["label"] = (
                chart_df["commodity"].astype(str)
                + " ("
                + chart_df["unit"].fillna("").astype(str)
                + ")"
            )
            bar_exp = (
                alt.Chart(chart_df)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("avg_price:Q", title="Avg Price (Rs.)"),
                    y=alt.Y("label:N", sort="-x", title=""),
                    color=alt.Color(
                        "avg_price:Q",
                        scale=alt.Scale(scheme="tealblues"),
                        legend=None,
                    ),
                    tooltip=[
                        "commodity:N",
                        "unit:N",
                        "min_price:Q",
                        "max_price:Q",
                        "avg_price:Q",
                    ],
                )
                .properties(height=360, title=f"Top 15 by Avg Price — {selected_date}")
                .configure_view(strokeWidth=0)
                .configure_axis(**_CHART_CFG)
                .configure_title(color="#f1f5f9", fontSize=13)
            )
            st.altair_chart(bar_exp, use_container_width=True)

        # Day-over-day comparison
        idx_list = date_order.index[date_order["scrape_date_bs"] == selected_date].tolist()
        if idx_list and idx_list[0] > 0:
            prev_date = date_order.iloc[idx_list[0] - 1]["scrape_date_bs"]
            prev_df = (
                history_df[history_df["scrape_date_bs"] == prev_date][
                    ["commodity", "unit", "avg_price"]
                ]
                .rename(columns={"avg_price": "prev_avg"})
                .copy()
            )
            curr_df = view_df[["commodity", "unit", "avg_price"]].rename(
                columns={"avg_price": "curr_avg"}
            )
            cmp_df = curr_df.merge(prev_df, on=["commodity", "unit"])
            cmp_df["change"] = cmp_df["curr_avg"] - cmp_df["prev_avg"]
            cmp_df["change_pct"] = (cmp_df["change"] / cmp_df["prev_avg"] * 100).round(1)

            st.subheader(f"Day-over-Day vs {prev_date}")
            disp_cols = ["commodity", "unit", "prev_avg", "curr_avg", "change", "change_pct"]
            c1, c2 = st.columns(2)
            with c1:
                st.caption("Top Increases ↑")
                st.dataframe(
                    cmp_df.nlargest(10, "change")[disp_cols], use_container_width=True
                )
            with c2:
                st.caption("Top Decreases ↓")
                st.dataframe(
                    cmp_df.nsmallest(10, "change")[disp_cols], use_container_width=True
                )

        with st.expander("Full price table + download"):
            if len(view_df) > 0:
                st.download_button(
                    "Download CSV",
                    data=to_csv_bytes(view_df),
                    file_name=f"kalimati_{selected_date}.csv",
                    mime="text/csv",
                )
            tbl_cols = [
                "commodity",
                "unit",
                "min_price",
                "max_price",
                "avg_price",
                "price_spread",
                "history_confidence_band",
                "row_depth_flag",
                "price_quality_flag",
                "policy_manual_review",
            ]
            tbl_cols = [c for c in tbl_cols if c in view_df.columns]
            st.dataframe(view_df[tbl_cols], use_container_width=True)

# =============================================================================
# TAB 3 · ANOMALY WATCHLIST
# =============================================================================
with tab_anomaly:
    if anomaly_df.empty:
        st.info("Anomaly report not found. Run `python -m analysis.anomaly_report` first.")
    else:
        if "latest_bs_date" in anomaly_df.columns:
            latest_anomaly = anomaly_df[anomaly_df["latest_bs_date"] == latest_bs].copy()
        else:
            latest_anomaly = anomaly_df.copy()

        ac1, ac2, ac3 = st.columns(3)
        ac1.metric("Anomaly signals (latest date)", len(latest_anomaly))
        pos_spikes = (
            int((latest_anomaly["pct_change_vs_median"] > 20).sum())
            if "pct_change_vs_median" in latest_anomaly.columns
            else 0
        )
        neg_drops = (
            int((latest_anomaly["pct_change_vs_median"] < -20).sum())
            if "pct_change_vs_median" in latest_anomaly.columns
            else 0
        )
        ac2.metric("Positive spikes > 20%", pos_spikes)
        ac3.metric("Negative drops > 20%", neg_drops)

        if "pct_change_vs_median" in latest_anomaly.columns and len(latest_anomaly) > 0:
            scatter_df = latest_anomaly.dropna(
                subset=["current_avg_price", "baseline_median_7", "pct_change_vs_median"]
            ).copy()
            scatter_df["abs_pct"] = scatter_df["pct_change_vs_median"].abs()
            scatter_df["direction"] = scatter_df["pct_change_vs_median"].apply(
                lambda x: "Up" if x >= 0 else "Down"
            )
            scatter_chart = (
                alt.Chart(scatter_df)
                .mark_circle(opacity=0.8)
                .encode(
                    x=alt.X("baseline_median_7:Q", title="7-Day Baseline Median (Rs.)"),
                    y=alt.Y("current_avg_price:Q", title="Current Avg Price (Rs.)"),
                    size=alt.Size(
                        "abs_pct:Q", scale=alt.Scale(range=[20, 300]), legend=None
                    ),
                    color=alt.Color(
                        "direction:N",
                        scale=alt.Scale(
                            domain=["Up", "Down"], range=["#22c55e", "#ef4444"]
                        ),
                        legend=alt.Legend(title="Direction"),
                    ),
                    tooltip=[
                        "commodity:N",
                        "unit:N",
                        "current_avg_price:Q",
                        "baseline_median_7:Q",
                        "pct_change_vs_median:Q",
                    ],
                )
                .properties(height=360, title="Current Price vs 7-Day Median")
                .configure_view(strokeWidth=0)
                .configure_axis(**_CHART_CFG)
                .configure_title(color="#f1f5f9", fontSize=13)
                .configure_legend(
                    labelColor="#94a3b8",
                    titleColor="#94a3b8",
                    fillColor="#1e293b",
                    strokeColor="#334155",
                )
            )
            st.altair_chart(scatter_chart, use_container_width=True)

        anm_cols = [
            "commodity",
            "unit",
            "current_avg_price",
            "baseline_median_7",
            "pct_change_vs_median",
            "latest_history_confidence_band",
        ]
        anm_cols = [c for c in anm_cols if c in latest_anomaly.columns]
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top Price Spikes ↑")
            st.dataframe(
                latest_anomaly.nlargest(10, "pct_change_vs_median")[anm_cols],
                use_container_width=True,
            )
        with c2:
            st.subheader("Top Price Drops ↓")
            st.dataframe(
                latest_anomaly.nsmallest(10, "pct_change_vs_median")[anm_cols],
                use_container_width=True,
            )

        with st.expander("Full anomaly table + download"):
            st.download_button(
                "Download anomaly CSV",
                data=to_csv_bytes(latest_anomaly),
                file_name=f"kalimati_anomaly_{latest_bs}.csv",
                mime="text/csv",
            )
            st.dataframe(latest_anomaly, use_container_width=True)

# =============================================================================
# TAB 4 · FORECAST
# =============================================================================
with tab_forecast:
    if forecast_df.empty:
        st.info(
            "Forecast report not found. Run `python -m analysis.forecast_baseline` first."
        )
    else:
        if "latest_bs_date" in forecast_df.columns:
            latest_forecast = forecast_df[
                forecast_df["latest_bs_date"] == latest_bs
            ].copy()
        else:
            latest_forecast = forecast_df.copy()

        fc1, fc2, fc3 = st.columns(3)
        fc1.metric("Forecast series (latest date)", len(latest_forecast))
        up_count = (
            int((latest_forecast["forecast_delta_vs_latest"] > 0).sum())
            if "forecast_delta_vs_latest" in latest_forecast.columns
            else 0
        )
        down_count = (
            int((latest_forecast["forecast_delta_vs_latest"] < 0).sum())
            if "forecast_delta_vs_latest" in latest_forecast.columns
            else 0
        )
        fc2.metric("Expected upward revisions", up_count)
        fc3.metric("Expected downward revisions", down_count)

        if "forecast_delta_vs_latest" in latest_forecast.columns and len(latest_forecast) > 0:
            delta_df = latest_forecast.dropna(subset=["forecast_delta_vs_latest"]).copy()
            delta_df["direction"] = delta_df["forecast_delta_vs_latest"].apply(
                lambda x: "Up" if x >= 0 else "Down"
            )
            delta_df["abs_delta"] = delta_df["forecast_delta_vs_latest"].abs()
            top15_f = delta_df.nlargest(15, "abs_delta").copy()
            top15_f["label"] = (
                top15_f["commodity"].astype(str)
                + " ("
                + top15_f["unit"].fillna("").astype(str)
                + ")"
            )
            bar_forecast = (
                alt.Chart(top15_f)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("forecast_delta_vs_latest:Q", title="Forecast Delta (Rs.)"),
                    y=alt.Y("label:N", sort="-x", title=""),
                    color=alt.Color(
                        "direction:N",
                        scale=alt.Scale(
                            domain=["Up", "Down"], range=["#22c55e", "#ef4444"]
                        ),
                        legend=alt.Legend(title="Direction"),
                    ),
                    tooltip=[
                        "commodity:N",
                        "unit:N",
                        "latest_avg_price:Q",
                        "next_day_forecast:Q",
                        "forecast_delta_vs_latest:Q",
                    ],
                )
                .properties(height=380, title="Top 15 Forecast Deltas")
                .configure_view(strokeWidth=0)
                .configure_axis(**_CHART_CFG)
                .configure_title(color="#f1f5f9", fontSize=13)
                .configure_legend(
                    labelColor="#94a3b8",
                    titleColor="#94a3b8",
                    fillColor="#1e293b",
                    strokeColor="#334155",
                )
            )
            st.altair_chart(bar_forecast, use_container_width=True)

        fcast_cols = [
            "commodity",
            "unit",
            "latest_avg_price",
            "rolling_median_7",
            "next_day_forecast",
            "forecast_delta_vs_latest",
            "latest_history_confidence_band",
        ]
        fcast_cols = [c for c in fcast_cols if c in latest_forecast.columns]
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Expected Upward Revisions ↑")
            st.dataframe(
                latest_forecast.nlargest(10, "forecast_delta_vs_latest")[fcast_cols],
                use_container_width=True,
            )
        with c2:
            st.subheader("Expected Downward Revisions ↓")
            st.dataframe(
                latest_forecast.nsmallest(10, "forecast_delta_vs_latest")[fcast_cols],
                use_container_width=True,
            )

        with st.expander("Full forecast table + download"):
            st.download_button(
                "Download forecast CSV",
                data=to_csv_bytes(latest_forecast),
                file_name=f"kalimati_forecast_{latest_bs}.csv",
                mime="text/csv",
            )
            st.dataframe(latest_forecast, use_container_width=True)

# =============================================================================
# TAB 5 · COMMODITY TRENDS
# =============================================================================
with tab_trends:
    trend_keys = sorted(
        {
            f"{row['commodity']} ({row['unit']})"
            for _, row in history_df[["commodity", "unit"]].drop_duplicates().iterrows()
            if pd.notna(row["commodity"])
        }
    )

    if not trend_keys:
        st.info("No commodity data available.")
    else:
        selected_trend = st.selectbox("Select commodity", trend_keys)
        parts = selected_trend.rsplit(" (", 1)
        t_commodity = parts[0]
        t_unit = parts[1].rstrip(")") if len(parts) > 1 else None

        if t_unit and t_unit != "nan":
            trend_data = history_df[
                (history_df["commodity"] == t_commodity)
                & (history_df["unit"] == t_unit)
            ].copy()
        else:
            trend_data = history_df[
                (history_df["commodity"] == t_commodity) & history_df["unit"].isna()
            ].copy()

        trend_data = (
            trend_data.dropna(subset=["sort_date", "avg_price"])
            .sort_values("sort_date")
            .reset_index(drop=True)
        )

        if len(trend_data) == 0:
            st.info("No trend data for the selected commodity.")
        else:
            conf_options = trend_data["history_confidence_band"].dropna().unique().tolist()
            conf_filter = st.multiselect(
                "Filter by confidence band",
                options=["all"] + conf_options,
                default=["all"],
            )
            plot_data = (
                trend_data
                if "all" in conf_filter or not conf_filter
                else trend_data[trend_data["history_confidence_band"].isin(conf_filter)]
            ).copy()

            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("Data points", len(plot_data))
            tc2.metric(
                "Latest avg price",
                f"Rs. {plot_data['avg_price'].iloc[-1]:.2f}" if len(plot_data) else "N/A",
            )
            tc3.metric(
                "All-time avg", f"Rs. {plot_data['avg_price'].mean():.2f}"
            )

            base_t = alt.Chart(plot_data)
            band_t = base_t.mark_area(opacity=0.2, color="#3b82f6").encode(
                x=alt.X("sort_date:T", title="Date"),
                y=alt.Y("min_price:Q", title="Price (Rs.)", scale=alt.Scale(zero=False)),
                y2="max_price:Q",
            )
            line_t = base_t.mark_line(color="#3b82f6", strokeWidth=2).encode(
                x=alt.X("sort_date:T"),
                y=alt.Y("avg_price:Q"),
                tooltip=[
                    "scrape_date_bs:N",
                    "avg_price:Q",
                    "min_price:Q",
                    "max_price:Q",
                ],
            )
            trend_chart = (
                (band_t + line_t)
                .properties(
                    height=360,
                    title=f"{t_commodity} ({t_unit}) — Price History",
                )
                .configure_view(strokeWidth=0)
                .configure_axis(**_CHART_CFG)
                .configure_title(color="#f1f5f9", fontSize=13)
            )
            st.altair_chart(trend_chart, use_container_width=True)

            st.download_button(
                "Download trend CSV",
                data=to_csv_bytes(
                    plot_data[
                        ["sort_date", "scrape_date_bs", "avg_price", "min_price", "max_price"]
                    ]
                ),
                file_name=f"kalimati_trend_{t_commodity}.csv",
                mime="text/csv",
            )

            with st.expander("Full trend table"):
                st.dataframe(
                    plot_data[
                        [
                            "sort_date",
                            "scrape_date_bs",
                            "avg_price",
                            "min_price",
                            "max_price",
                            "history_confidence_band",
                        ]
                    ],
                    use_container_width=True,
                )

# =============================================================================
# TAB 6 · DATA QUALITY
# =============================================================================
with tab_quality:
    col_rd, col_pq = st.columns(2)

    with col_rd:
        st.subheader("Row-Depth Policy Flags")
        if not row_depth_df.empty and "row_depth_severity" in row_depth_df.columns:
            rd_summary = (
                row_depth_df.groupby("row_depth_severity")
                .size()
                .reset_index(name="count")
            )
            rd_bar = (
                alt.Chart(rd_summary)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("row_depth_severity:N", title="Severity"),
                    y=alt.Y("count:Q", title="Date Count"),
                    color=alt.Color(
                        "row_depth_severity:N",
                        scale=alt.Scale(
                            domain=["normal", "low", "critical_low"],
                            range=["#22c55e", "#f59e0b", "#ef4444"],
                        ),
                        legend=None,
                    ),
                    tooltip=["row_depth_severity:N", "count:Q"],
                )
                .properties(height=220)
                .configure_view(strokeWidth=0)
                .configure_axis(**_CHART_CFG)
            )
            st.altair_chart(rd_bar, use_container_width=True)
            qd1, qd2 = st.columns(2)
            qd1.metric("Date coverage rows", len(row_depth_df))
            excl_rd = (
                int(row_depth_df["exclude_from_default_model_window"].sum())
                if "exclude_from_default_model_window" in row_depth_df.columns
                else 0
            )
            qd2.metric("Excluded from model window", excl_rd)
        else:
            st.info(
                "Row-depth flags not found. Run `python -m analysis.generate_policy_flags`."
            )

    with col_pq:
        st.subheader("Price-Quality Flags")
        if not price_quality_df.empty and "price_issue_type" in price_quality_df.columns:
            pq_summary = (
                price_quality_df.groupby("price_issue_type")
                .size()
                .reset_index(name="count")
            )
            pq_bar = (
                alt.Chart(pq_summary)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("price_issue_type:N", title="Issue Type"),
                    y=alt.Y("count:Q", title="Row Count"),
                    color=alt.Color("price_issue_type:N", legend=None),
                    tooltip=["price_issue_type:N", "count:Q"],
                )
                .properties(height=220)
                .configure_view(strokeWidth=0)
                .configure_axis(**_CHART_CFG)
            )
            st.altair_chart(pq_bar, use_container_width=True)
            pq1, pq2 = st.columns(2)
            pq1.metric("Total price-quality flags", len(price_quality_df))
            excl_pq = (
                int(price_quality_df["exclude_from_default_model_window"].sum())
                if "exclude_from_default_model_window" in price_quality_df.columns
                else 0
            )
            pq2.metric("Excluded from model window", excl_pq)
        else:
            st.info(
                "Price-quality flags not found. Run `python -m analysis.generate_policy_flags`."
            )

    with st.expander("Data sources & audit summary"):
        st.markdown(
            f"""
| Dataset | Rows |
|---------|------|
| Price history | {len(history_df):,} |
| Anomaly report | {len(anomaly_df):,} |
| Forecast baseline | {len(forecast_df):,} |
| Row-depth policy flags | {len(row_depth_df):,} |
| Price-quality policy flags | {len(price_quality_df):,} |
"""
        )
        st.caption(
            f"Pipeline ran at: {pipeline_status.get('pipeline_ran_at_utc', 'N/A')} · "
            f"Latest BS date: {pipeline_status.get('latest_history_bs_date', 'N/A')}"
        )

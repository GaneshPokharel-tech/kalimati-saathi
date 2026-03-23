"""
Kalimati Saathi — Production Dashboard
World-class UI/UX: tab navigation, live KPI cards, Altair charts,
color-coded severity badges, multi-horizon forecast visualisation.
"""
from __future__ import annotations

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

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kalimati Saathi",
    layout="wide",
    page_icon="🥬",
    initial_sidebar_state="collapsed",
)

# ── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* ── KPI card ── */
.kpi-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 20px 24px;
    text-align: center;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: var(--accent, #22c55e);
}
.kpi-value {
    font-size: 2rem;
    font-weight: 700;
    color: #f1f5f9;
    line-height: 1.1;
    margin: 4px 0;
}
.kpi-label {
    font-size: 0.78rem;
    font-weight: 500;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.kpi-sub {
    font-size: 0.72rem;
    color: #64748b;
    margin-top: 4px;
}

/* ── Badge ── */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}
.badge-green  { background:#14532d; color:#4ade80; }
.badge-yellow { background:#422006; color:#fbbf24; }
.badge-orange { background:#431407; color:#fb923c; }
.badge-red    { background:#450a0a; color:#f87171; }
.badge-blue   { background:#1e3a5f; color:#60a5fa; }
.badge-gray   { background:#1e293b; color:#94a3b8; }

/* ── Status dot ── */
.dot-green  { color:#22c55e; font-size:0.9rem; }
.dot-yellow { color:#eab308; font-size:0.9rem; }
.dot-red    { color:#ef4444; font-size:0.9rem; }

/* ── Hero header ── */
.hero {
    background: linear-gradient(135deg, #052e16 0%, #064e3b 40%, #0f172a 100%);
    border: 1px solid #166534;
    border-radius: 16px;
    padding: 28px 36px;
    margin-bottom: 8px;
}
.hero-title {
    font-size: 2rem;
    font-weight: 700;
    color: #f0fdf4;
    margin: 0;
}
.hero-sub {
    font-size: 0.9rem;
    color: #86efac;
    margin-top: 4px;
}

/* ── Section header ── */
.section-header {
    font-size: 1.1rem;
    font-weight: 600;
    color: #e2e8f0;
    border-left: 3px solid #22c55e;
    padding-left: 10px;
    margin: 20px 0 12px 0;
}

/* ── Commodity row ── */
.price-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 16px;
    border-radius: 8px;
    background: #1e293b;
    margin-bottom: 4px;
    border: 1px solid #334155;
}
.price-name { font-weight: 500; color: #e2e8f0; font-size: 0.9rem; }
.price-val  { font-weight: 700; color: #4ade80; font-size: 1rem; }

/* ── Streamlit overrides ── */
div[data-testid="stTabs"] button {
    font-size: 0.88rem !important;
    font-weight: 500 !important;
}
div[data-testid="metric-container"] {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 10px;
    padding: 12px 16px;
}
.stAlert { border-radius: 10px !important; }
footer { visibility: hidden; }

/* ── HTML tables (badge tables) ── */
.table-compact {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}
.table-compact th {
    text-align: left;
    color: #cbd5e1;
    padding: 8px 10px;
    background: #0f172a;
    border-bottom: 1px solid #1f2937;
}
.table-compact td {
    padding: 8px 10px;
    color: #e2e8f0;
    border-bottom: 1px solid #1f2937;
}
.table-compact tr:nth-child(even) { background: #111827; }
.table-compact tr:nth-child(odd)  { background: #0b1220; }
.table-compact tr:hover { background: #162033; }

/* ── Responsive KPI spacing ── */
@media (max-width: 1200px) {
    .kpi-card { margin-bottom: 12px; }
}
</style>
""",
    unsafe_allow_html=True,
)

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = PROJECT_ROOT / "data/processed"
history_csv_path          = _BASE / "kalimati_price_history.csv"
anomaly_csv_path          = _BASE / "kalimati_anomaly_report.csv"
forecast_csv_path         = _BASE / "kalimati_forecast_baseline.csv"
market_brief_path         = _BASE / "kalimati_market_brief.md"
row_depth_policy_csv_path = _BASE / "row_depth_policy_flags.csv"
price_quality_policy_csv_path = _BASE / "price_quality_policy_flags.csv"
sqlite_db_path            = _BASE / "kalimati.db"
pipeline_status_path      = _BASE / "kalimati_pipeline_status.json"
scrape_status_path        = _BASE / "kalimati_last_scrape_status.json"

_ALLOWED_TABLES = {
    "price_history", "anomaly_report", "forecast_baseline",
    "market_brief", "pipeline_status", "scrape_status",
    "row_depth_policy_flags", "price_quality_policy_flags",
}

# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def _load_sqlite(table_name: str) -> pd.DataFrame:
    if table_name not in _ALLOWED_TABLES or not sqlite_db_path.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(sqlite_db_path)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def _load_with_fallback(table: str, csv: Path) -> tuple[pd.DataFrame, str]:
    df = _load_sqlite(table)
    if not df.empty:
        return df, "SQLite"
    if csv.exists():
        return pd.read_csv(csv).copy(), "CSV"
    return pd.DataFrame(), "None"


@st.cache_data(ttl=300, show_spinner=False)
def load_all_data():
    history_df, history_src       = _load_with_fallback("price_history", history_csv_path)
    anomaly_df, _                  = _load_with_fallback("anomaly_report", anomaly_csv_path)
    forecast_df, _                 = _load_with_fallback("forecast_baseline", forecast_csv_path)
    rdp_df, _                      = _load_with_fallback("row_depth_policy_flags", row_depth_policy_csv_path)
    pqp_df, _                      = _load_with_fallback("price_quality_policy_flags", price_quality_policy_csv_path)

    brief_df = _load_sqlite("market_brief")
    brief = ""
    if not brief_df.empty and "brief_markdown" in brief_df.columns:
        brief = str(brief_df.iloc[0]["brief_markdown"])
    elif market_brief_path.exists():
        brief = market_brief_path.read_text(encoding="utf-8")

    pipe_df = _load_sqlite("pipeline_status")
    pipe_status = pipe_df.iloc[0].to_dict() if not pipe_df.empty else {}
    if not pipe_status and pipeline_status_path.exists():
        pipe_status = json.loads(pipeline_status_path.read_text(encoding="utf-8"))

    scrape_df = _load_sqlite("scrape_status")
    scrape_status = scrape_df.iloc[0].to_dict() if not scrape_df.empty else {}
    if not scrape_status and scrape_status_path.exists():
        scrape_status = json.loads(scrape_status_path.read_text(encoding="utf-8"))

    return history_df, anomaly_df, forecast_df, rdp_df, pqp_df, brief, pipe_status, scrape_status


# ── Helper: bool normaliser ───────────────────────────────────────────────────
def _norm_bool(s: pd.Series) -> pd.Series:
    if getattr(s, "dtype", None) == bool:
        return s.fillna(False)
    return s.astype("string").fillna("").str.strip().str.lower().isin(["true", "1", "yes"])


# ── Helper: enrich watchlist ──────────────────────────────────────────────────
def enrich_watchlist(wdf: pd.DataFrame, rdp: pd.DataFrame, pqp: pd.DataFrame) -> pd.DataFrame:
    wdf = wdf.copy()
    if wdf.empty:
        return wdf
    if "latest_is_default_model_window" in wdf.columns:
        wdf["latest_is_default_model_window"] = _norm_bool(wdf["latest_is_default_model_window"])
    else:
        wdf["latest_is_default_model_window"] = False

    if not rdp.empty:
        rdp_w = rdp[["scrape_date_bs","row_depth_severity","exclude_from_default_model_window","manual_review","policy_action"]]\
            .drop_duplicates()\
            .rename(columns={"scrape_date_bs":"latest_bs_date","row_depth_severity":"latest_row_depth_flag",
                             "exclude_from_default_model_window":"latest_row_depth_excluded",
                             "manual_review":"latest_row_depth_manual_review",
                             "policy_action":"latest_row_depth_policy_action"})
        wdf = wdf.merge(rdp_w, on="latest_bs_date", how="left")
    else:
        wdf["latest_row_depth_flag"] = pd.NA
        wdf["latest_row_depth_excluded"] = False
        wdf["latest_row_depth_manual_review"] = False

    if not pqp.empty:
        pqp_w = pqp[["scrape_date_bs","commodity","unit","price_issue_type","exclude_from_default_model_window","manual_review","policy_action"]]\
            .drop_duplicates()\
            .rename(columns={"scrape_date_bs":"latest_bs_date","price_issue_type":"latest_price_quality_flag",
                             "exclude_from_default_model_window":"latest_price_quality_excluded",
                             "manual_review":"latest_price_quality_manual_review",
                             "policy_action":"latest_price_quality_policy_action"})
        wdf = wdf.merge(pqp_w, on=["latest_bs_date","commodity","unit"], how="left")
    else:
        wdf["latest_price_quality_flag"] = pd.NA
        wdf["latest_price_quality_excluded"] = False
        wdf["latest_price_quality_manual_review"] = False

    for col in ["latest_row_depth_excluded","latest_row_depth_manual_review",
                "latest_price_quality_excluded","latest_price_quality_manual_review"]:
        wdf[col] = _norm_bool(wdf[col])

    wdf["latest_policy_manual_review"] = wdf["latest_row_depth_manual_review"] | wdf["latest_price_quality_manual_review"]
    wdf["latest_is_policy_excluded_from_default_model_window"] = wdf["latest_row_depth_excluded"] | wdf["latest_price_quality_excluded"]
    wdf["latest_is_safe_default_row"] = wdf["latest_is_default_model_window"] & ~wdf["latest_is_policy_excluded_from_default_model_window"]
    return wdf


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


# ── UI helpers ────────────────────────────────────────────────────────────────
def kpi(label: str, value: str, sub: str = "", accent: str = "#22c55e") -> None:
    st.markdown(
        f"""<div class="kpi-card" style="--accent:{accent}">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub">{sub}</div>
        </div>""",
        unsafe_allow_html=True,
    )


_SEV_BADGE = {
    "critical": '<span class="badge badge-red">⚠ CRITICAL</span>',
    "high":     '<span class="badge badge-orange">↑ HIGH</span>',
    "medium":   '<span class="badge badge-yellow">~ MEDIUM</span>',
    "low":      '<span class="badge badge-blue">LOW</span>',
    "normal":   '<span class="badge badge-green">NORMAL</span>',
}
_TREND_BADGE = {
    "uptrend":   '<span class="badge badge-green">↑ UP</span>',
    "downtrend": '<span class="badge badge-red">↓ DOWN</span>',
    "stable":    '<span class="badge badge-gray">→ STABLE</span>',
}


def sev_badge(s: str) -> str:
    return _SEV_BADGE.get(str(s).lower(), f'<span class="badge badge-gray">{s}</span>')


def trend_badge(s: str) -> str:
    return _TREND_BADGE.get(str(s).lower(), f'<span class="badge badge-gray">{s}</span>')


def section(title: str) -> None:
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)


def render_badge_table(df: pd.DataFrame, badge_cols: dict[str, callable]) -> None:
    if df.empty:
        st.info("No rows to display for current filters.")
        return
    disp = df.copy()
    for col, fn in badge_cols.items():
        if col in disp.columns:
            disp[col] = disp[col].map(fn)
    st.markdown(
        disp.to_html(escape=False, index=False, classes="table-compact"),
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Load data
# ══════════════════════════════════════════════════════════════════════════════
(
    raw_history_df, anomaly_df, forecast_df,
    row_depth_policy_df, price_quality_policy_df,
    market_brief_content, pipeline_status, scrape_status,
) = load_all_data()

if raw_history_df.empty:
    st.error("No history data found. Run `python run_daily_pipeline.py` first.")
    st.stop()

# ── Prepare history ───────────────────────────────────────────────────────────
history_df = raw_history_df.copy()
history_df["price_spread"] = history_df["max_price"] - history_df["min_price"]
history_df["requested_date_ad_dt"] = pd.to_datetime(history_df["requested_date_ad"], errors="coerce")
history_df["fetched_at_utc_dt"] = pd.to_datetime(history_df["fetched_at_utc"], errors="coerce").dt.tz_convert(None)
history_df["sort_date"] = history_df["requested_date_ad_dt"].fillna(history_df["fetched_at_utc_dt"])
if "history_confidence_band" not in history_df.columns:
    history_df = add_history_confidence(history_df)

# Merge policy flags
if not row_depth_policy_df.empty:
    rdp_join = row_depth_policy_df[["requested_date_ad","scrape_date_bs","row_count",
                                     "row_depth_severity","exclude_from_default_model_window",
                                     "manual_review","policy_action"]]\
        .drop_duplicates()\
        .rename(columns={"row_count":"row_depth_row_count","row_depth_severity":"row_depth_flag",
                         "exclude_from_default_model_window":"row_depth_excluded",
                         "manual_review":"row_depth_manual_review","policy_action":"row_depth_policy_action"})
    history_df = history_df.merge(rdp_join, on=["requested_date_ad","scrape_date_bs"], how="left")
else:
    for c in ["row_depth_row_count","row_depth_flag","row_depth_excluded","row_depth_manual_review","row_depth_policy_action"]:
        history_df[c] = pd.NA

if not price_quality_policy_df.empty:
    pqp_join = price_quality_policy_df[["requested_date_ad","scrape_date_bs","commodity","unit",
                                         "price_issue_type","exclude_from_default_model_window",
                                         "manual_review","policy_action"]]\
        .drop_duplicates()\
        .rename(columns={"price_issue_type":"price_quality_flag","exclude_from_default_model_window":"price_quality_excluded",
                         "manual_review":"price_quality_manual_review","policy_action":"price_quality_policy_action"})
    history_df = history_df.merge(pqp_join, on=["requested_date_ad","scrape_date_bs","commodity","unit"], how="left")
else:
    for c in ["price_quality_flag","price_quality_excluded","price_quality_manual_review","price_quality_policy_action"]:
        history_df[c] = pd.NA

for c in ["row_depth_excluded","row_depth_manual_review","price_quality_excluded","price_quality_manual_review"]:
    history_df[c] = _norm_bool(history_df[c])
history_df["policy_manual_review"] = history_df["row_depth_manual_review"] | history_df["price_quality_manual_review"]
history_df["is_policy_excluded_from_default_model_window"] = history_df["row_depth_excluded"] | history_df["price_quality_excluded"]
history_df["is_safe_default_row"] = _norm_bool(history_df.get("is_default_model_window", pd.Series(False, index=history_df.index))) & ~history_df["is_policy_excluded_from_default_model_window"]

date_order_df = (
    history_df.groupby("scrape_date_bs", as_index=False)["sort_date"]
    .max().sort_values("sort_date").reset_index(drop=True)
)
available_dates = date_order_df["scrape_date_bs"].tolist()
latest_saved_bs_date = pipeline_status.get("latest_history_bs_date") or (available_dates[-1] if available_dates else "")

# ══════════════════════════════════════════════════════════════════════════════
# Hero Header
# ══════════════════════════════════════════════════════════════════════════════
scrape_ok = scrape_status.get("status") == "saved"
dot = '<span class="dot-green">●</span>' if scrape_ok else '<span class="dot-yellow">●</span>'
status_text = "Live" if scrape_ok else "Stale"

total_rows    = pipeline_status.get("history_rows", len(history_df))
n_forecasts   = len(forecast_df) if not forecast_df.empty else 0
n_critical    = int((anomaly_df["anomaly_severity"] == "critical").sum()) if (not anomaly_df.empty and "anomaly_severity" in anomaly_df.columns) else 0

top_model_share = "model unavailable"
if not forecast_df.empty and "model_used" in forecast_df.columns:
    latest_slice = forecast_df
    if latest_saved_bs_date:
        subset = forecast_df[forecast_df["latest_bs_date"] == latest_saved_bs_date]
        if not subset.empty:
            latest_slice = subset
    counts = latest_slice["model_used"].fillna("unknown").value_counts()
    top_model = counts.index[0]
    top_pct = int(round(counts.iloc[0] / max(len(latest_slice), 1) * 100))
    top_model_share = f"{top_model.replace('_',' ')} ({top_pct}%)"

st.markdown(
    f"""<div class="hero">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px">
            <div>
                <div class="hero-title">🥬 Kalimati Saathi</div>
                <div class="hero-sub">Nepal Vegetable Price Intelligence · Forecasts &amp; Anomaly Detection</div>
            </div>
            <div style="text-align:right">
                <div style="font-size:0.85rem;color:#86efac">{dot} Data {status_text}</div>
                <div style="font-size:0.78rem;color:#64748b;margin-top:2px">
                    Latest: <strong style="color:#a7f3d0">{latest_saved_bs_date}</strong>
                </div>
            </div>
        </div>
    </div>""",
    unsafe_allow_html=True,
)

# ── Top KPI row (responsive two-row layout) ───────────────────────────────────
k_row1 = st.columns(3)
k_row2 = st.columns(2)
with k_row1[0]:
    kpi("Total History Rows", f"{total_rows:,}", "all dates combined", "#22c55e")
with k_row1[1]:
    kpi("Latest Market Date", latest_saved_bs_date, "Bikram Sambat", "#0ea5e9")
with k_row1[2]:
    kpi("Today's Items", str(scrape_status.get("row_count", "—")), "commodities scraped", "#8b5cf6")
with k_row2[0]:
    kpi("Forecasts", str(n_forecasts), top_model_share, "#f59e0b")
with k_row2[1]:
    accent = "#ef4444" if n_critical > 0 else "#22c55e"
    kpi("Critical Anomalies", str(n_critical), "today vs 7-day median", accent)

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Tab Navigation
# ══════════════════════════════════════════════════════════════════════════════
tab_overview, tab_prices, tab_anomaly, tab_forecast, tab_trends, tab_quality = st.tabs(
    ["📊 Overview", "💰 Price Explorer", "⚠️ Anomaly Watchlist",
     "🔮 Forecast", "📈 Commodity Trends", "🔬 Data Quality"]
)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Overview
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    col_brief, col_snap = st.columns([1, 1], gap="large")

    with col_brief:
        section("Market Brief")
        if market_brief_content:
            with st.container(border=True):
                st.markdown(market_brief_content)
        else:
            st.info("Market brief not available. Run the pipeline first.")

    with col_snap:
        section("Pipeline Status")
        if scrape_status:
            status_val = scrape_status.get("status", "unknown")
            if status_val == "saved":
                st.success(f"Last scrape saved successfully · {scrape_status.get('scrape_ran_at_utc','')[:19]} UTC")
            elif status_val == "no_data":
                st.warning("Last scrape returned no data rows")
            else:
                st.error(f"Scrape status: {status_val}")

            m1, m2 = st.columns(2)
            m1.metric("Returned BS Date", scrape_status.get("returned_bs_date") or "N/A")
            m2.metric("Rows Scraped", scrape_status.get("row_count", 0))
            m3, m4 = st.columns(2)
            m3.metric("History Rows", f"{pipeline_status.get('history_rows', len(history_df)):,}")
            m4.metric("Latest AD Date", pipeline_status.get("latest_history_ad_date", "N/A"))
        else:
            st.info("Run `python run_daily_pipeline.py` to populate pipeline status.")

        section("Confidence Band Distribution")
        if not history_df.empty and "history_confidence_band" in history_df.columns:
            band_counts = history_df["history_confidence_band"].value_counts().reset_index()
            band_counts.columns = ["band", "rows"]
            band_color = alt.Color(
                "band:N",
                scale=alt.Scale(
                    domain=["current_live","stronger_historical","medium_confidence_historical","low_confidence_historical"],
                    range=["#22c55e","#0ea5e9","#f59e0b","#ef4444"],
                ),
                legend=None,
            )
            chart = (
                alt.Chart(band_counts)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("rows:Q", title="Rows"),
                    y=alt.Y("band:N", sort="-x", title=None),
                    color=band_color,
                    tooltip=["band:N", alt.Tooltip("rows:Q", format=",")],
                )
                .properties(height=160)
                .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                .configure_view(strokeWidth=0)
                .configure(background="transparent")
            )
            st.altair_chart(chart, use_container_width=True)

        section("Today's Top 10 by Price")
        today_df = history_df[history_df["scrape_date_bs"] == latest_saved_bs_date].copy()
        if not today_df.empty:
            top10 = today_df.nlargest(10, "avg_price")[["commodity","unit","avg_price","min_price","max_price"]]
            for _, r in top10.iterrows():
                st.markdown(
                    f'<div class="price-row">'
                    f'<span class="price-name">{r["commodity"]} <span style="color:#64748b;font-size:0.78rem">({r["unit"]})</span></span>'
                    f'<span class="price-val">Rs. {r["avg_price"]:.0f}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Price Explorer
# ══════════════════════════════════════════════════════════════════════════════
with tab_prices:
    c_left, c_right = st.columns([1, 3], gap="medium")
    with c_left:
        section("Filters")
        selected_date = st.selectbox(
            "Market Date", available_dates, index=len(available_dates) - 1, key="px_date"
        )
        view_mode = st.radio(
            "View Mode",
            ["Safe window (recommended)", "All rows"],
            horizontal=False,
            help="Safe window excludes policy-flagged and low-confidence rows.",
        )
        units_available = sorted(
            history_df[history_df["scrape_date_bs"] == selected_date]["unit"].dropna().unique().tolist()
        )
        sel_unit = st.selectbox("Unit Filter", ["All"] + units_available, key="px_unit")
        search = st.text_input("Search Commodity", "", placeholder="e.g. टमाटर", key="px_search")
        sort_by = st.selectbox("Sort By", ["avg_price ↓", "avg_price ↑", "price_spread ↓", "commodity ↑"], key="px_sort")

    with c_right:
        sel_raw = history_df[history_df["scrape_date_bs"] == selected_date].copy()
        if view_mode == "Safe window (recommended)":
            sel_df = sel_raw[sel_raw["is_safe_default_row"]].copy()
        else:
            sel_df = sel_raw.copy()

        if sel_unit != "All":
            sel_df = sel_df[sel_df["unit"] == sel_unit]
        if search.strip():
            sel_df = sel_df[sel_df["commodity"].str.contains(search.strip(), case=False, na=False)]

        sort_map = {"avg_price ↓":("avg_price",False),"avg_price ↑":("avg_price",True),
                    "price_spread ↓":("price_spread",False),"commodity ↑":("commodity",True)}
        scol, sasc = sort_map[sort_by]
        sel_df = sel_df.sort_values(scol, ascending=sasc).reset_index(drop=True)

        # KPI strip
        ma, mb, mc, md = st.columns(4)
        ma.metric("Visible Items",    f"{len(sel_df):,}")
        mb.metric("Avg Price (Rs.)",  f"{sel_df['avg_price'].mean():.1f}" if not sel_df.empty else "—")
        mc.metric("Highest (Rs.)",    f"{sel_df['avg_price'].max():.0f}"  if not sel_df.empty else "—")
        md.metric("Lowest (Rs.)",     f"{sel_df['avg_price'].min():.0f}"  if not sel_df.empty else "—")

        if sel_df.empty:
            st.warning("No rows match your filters.")
        else:
            # Bar chart
            section("Top 20 by Average Price")
            chart_df = sel_df.nlargest(20, "avg_price").copy()
            chart_df["label"] = chart_df["commodity"] + " (" + chart_df["unit"].fillna("") + ")"
            bar = (
                alt.Chart(chart_df)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#22c55e")
                .encode(
                    y=alt.Y("label:N", sort="-x", title=None, axis=alt.Axis(labelLimit=200)),
                    x=alt.X("avg_price:Q", title="Avg Price (Rs.)"),
                    tooltip=[
                        alt.Tooltip("commodity:N"),
                        alt.Tooltip("unit:N"),
                        alt.Tooltip("avg_price:Q", format=".2f", title="Avg"),
                        alt.Tooltip("min_price:Q", format=".2f", title="Min"),
                        alt.Tooltip("max_price:Q", format=".2f", title="Max"),
                    ],
                )
                .properties(height=420)
                .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                .configure_view(strokeWidth=0)
                .configure(background="transparent")
            )
            st.altair_chart(bar, use_container_width=True)

            # Trend vs previous date
            sel_idx = date_order_df.index[date_order_df["scrape_date_bs"] == selected_date]
            if len(sel_idx) > 0 and sel_idx[0] > 0:
                prev_date = date_order_df.iloc[sel_idx[0] - 1]["scrape_date_bs"]
                prev_df = (
                    history_df[history_df["scrape_date_bs"] == prev_date][["commodity","unit","avg_price"]]
                    .rename(columns={"avg_price":"prev_price"})
                )
                cmp = sel_df[["commodity","unit","avg_price"]].rename(columns={"avg_price":"curr_price"})\
                    .merge(prev_df, on=["commodity","unit"], how="inner")
                cmp["change"] = cmp["curr_price"] - cmp["prev_price"]
                cmp["pct"]    = (cmp["change"] / cmp["prev_price"] * 100).round(1)

                section(f"Day-over-Day Change vs {prev_date}")
                cc1, cc2 = st.columns(2)
                with cc1:
                    st.caption("Top Increases")
                    render_badge_table(
                        cmp.nlargest(10,"change")[["commodity","unit","prev_price","curr_price","pct"]]
                        .rename(columns={"prev_price":"Prev (Rs.)","curr_price":"Now (Rs.)","pct":"Chg %"})
                        .reset_index(drop=True),
                        {},
                    )
                with cc2:
                    st.caption("Top Decreases")
                    render_badge_table(
                        cmp.nsmallest(10,"change")[["commodity","unit","prev_price","curr_price","pct"]]
                        .rename(columns={"prev_price":"Prev (Rs.)","curr_price":"Now (Rs.)","pct":"Chg %"})
                        .reset_index(drop=True),
                        {},
                    )
            else:
                st.info("Day-over-day change needs at least two market dates. Load another scrape date to compare.")

            section("Full Price Table")
            disp_cols = [c for c in ["commodity","unit","min_price","max_price","avg_price","price_spread",
                                      "history_confidence_band","row_depth_flag","price_quality_flag"] if c in sel_df.columns]
            render_badge_table(sel_df[disp_cols].reset_index(drop=True), {})
            st.download_button(
                "⬇ Download CSV", to_csv_bytes(sel_df[disp_cols]),
                file_name=f"kalimati_prices_{selected_date}.csv", mime="text/csv",
            )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Anomaly Watchlist
# ══════════════════════════════════════════════════════════════════════════════
with tab_anomaly:
    if anomaly_df.empty:
        st.info("No anomaly data. Run `python -m analysis.anomaly_report` first.")
    else:
        aw_df = anomaly_df[anomaly_df["latest_bs_date"] == latest_saved_bs_date].copy()
        aw_df = enrich_watchlist(aw_df, row_depth_policy_df, price_quality_policy_df)

        # Controls
        ac1, ac2, ac3 = st.columns([1, 1, 2])
        with ac1:
            sev_filter = st.multiselect(
                "Severity", ["critical","high","medium","low","normal"],
                default=["critical","high","medium"], key="aw_sev"
            )
        with ac2:
            aw_safe = st.toggle(
                "Safe window (recommended)",
                value=True,
                key="aw_safe",
                help="Filters out policy-flagged or low-confidence rows.",
            )
        with ac3:
            aw_search = st.text_input("Search", "", placeholder="commodity name…", key="aw_search")

        if aw_safe:
            aw_df = aw_df[aw_df["latest_is_safe_default_row"]].copy()
        if sev_filter and "anomaly_severity" in aw_df.columns:
            aw_df = aw_df[aw_df["anomaly_severity"].isin(sev_filter)]
        if aw_search.strip():
            aw_df = aw_df[aw_df["commodity"].str.contains(aw_search.strip(), case=False, na=False)]

        # Severity KPI cards
        if "anomaly_severity" in anomaly_df.columns:
            today_anom = anomaly_df[anomaly_df["latest_bs_date"] == latest_saved_bs_date]
            sev_counts = today_anom["anomaly_severity"].value_counts()
            sc1, sc2, sc3, sc4 = st.columns(4)
            with sc1: kpi("Critical", str(sev_counts.get("critical",0)), "immediate attention", "#ef4444")
            with sc2: kpi("High",     str(sev_counts.get("high",0)),     ">50% deviation",     "#f97316")
            with sc3: kpi("Medium",   str(sev_counts.get("medium",0)),   ">30% deviation",     "#eab308")
            with sc4: kpi("Low",      str(sev_counts.get("low",0)),      ">15% deviation",     "#3b82f6")

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        col_spikes, col_drops = st.columns(2, gap="large")

        with col_spikes:
            section("Top Price Spikes")
            spikes = aw_df.sort_values("pct_change_vs_median", ascending=False).head(15)
            if spikes.empty:
                st.info("No spikes in current filter.")
            else:
                # Scatter chart: current vs baseline
                scatter = (
                    alt.Chart(spikes)
                    .mark_circle(size=80)
                    .encode(
                        x=alt.X("baseline_median_7:Q", title="7-Day Median (Rs.)"),
                        y=alt.Y("current_avg_price:Q", title="Current Price (Rs.)"),
                        color=alt.Color(
                            "anomaly_severity:N",
                            scale=alt.Scale(
                                domain=["critical","high","medium","low","normal"],
                                range=["#ef4444","#f97316","#eab308","#3b82f6","#22c55e"],
                            ),
                        ),
                        tooltip=[
                            alt.Tooltip("commodity:N"),
                            alt.Tooltip("current_avg_price:Q", format=".2f"),
                            alt.Tooltip("baseline_median_7:Q", format=".2f"),
                            alt.Tooltip("pct_change_vs_median:Q", format=".1f", title="% Change"),
                            alt.Tooltip("z_score:Q", format=".2f") if "z_score" in spikes.columns else alt.Tooltip("commodity:N"),
                            alt.Tooltip("anomaly_severity:N"),
                        ],
                    )
                    .properties(height=220)
                    .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                    .configure_view(strokeWidth=0)
                    .configure(background="transparent")
                )
                st.altair_chart(scatter, use_container_width=True)

                show_cols = [c for c in ["commodity","unit","current_avg_price","baseline_median_7",
                                          "pct_change_vs_median","z_score","anomaly_severity"] if c in spikes.columns]
                render_badge_table(
                    spikes[show_cols].reset_index(drop=True),
                    {"anomaly_severity": sev_badge},
                )

        with col_drops:
            section("Top Price Drops")
            drops = aw_df.sort_values("pct_change_vs_median", ascending=True).head(15)
            if drops.empty:
                st.info("No drops in current filter.")
            else:
                bar_drops = (
                    alt.Chart(drops)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                    .encode(
                        y=alt.Y("commodity:N", sort="x", title=None),
                        x=alt.X("pct_change_vs_median:Q", title="% Change vs Median"),
                        color=alt.condition(
                            alt.datum.pct_change_vs_median < 0,
                            alt.value("#ef4444"),
                            alt.value("#22c55e"),
                        ),
                        tooltip=[
                            alt.Tooltip("commodity:N"),
                            alt.Tooltip("pct_change_vs_median:Q", format=".1f", title="% Change"),
                            alt.Tooltip("current_avg_price:Q", format=".2f"),
                        ],
                    )
                    .properties(height=220)
                    .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                    .configure_view(strokeWidth=0)
                    .configure(background="transparent")
                )
                st.altair_chart(bar_drops, use_container_width=True)
                show_cols = [c for c in ["commodity","unit","current_avg_price","baseline_median_7",
                                          "pct_change_vs_median","z_score","anomaly_severity"] if c in drops.columns]
                render_badge_table(
                    drops[show_cols].reset_index(drop=True),
                    {"anomaly_severity": sev_badge},
                )

        st.download_button(
            "⬇ Download Anomaly CSV", to_csv_bytes(aw_df),
            file_name=f"kalimati_anomalies_{latest_saved_bs_date}.csv", mime="text/csv",
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Forecast
# ══════════════════════════════════════════════════════════════════════════════
with tab_forecast:
    if forecast_df.empty:
        st.info("No forecast data. Run `python -m analysis.forecast_baseline` first.")
    else:
        fw_df = forecast_df[forecast_df["latest_bs_date"] == latest_saved_bs_date].copy()
        fw_df = enrich_watchlist(fw_df, row_depth_policy_df, price_quality_policy_df)

        fc1, fc2, fc3 = st.columns([1, 1, 2])
        with fc1:
            fc_safe = st.toggle(
                "Safe window (recommended)",
                value=True,
                key="fc_safe",
                help="Filters out policy-flagged or low-confidence rows.",
            )
        with fc2:
            trend_filter = st.multiselect(
                "Trend", ["uptrend","downtrend","stable"],
                default=["uptrend","downtrend","stable"], key="fc_trend"
            )
        with fc3:
            fc_search = st.text_input("Search", "", placeholder="commodity name…", key="fc_search")

        if fc_safe:
            fw_df = fw_df[fw_df["latest_is_safe_default_row"]].copy()
        if trend_filter and "trend_direction" in fw_df.columns:
            fw_df = fw_df[fw_df["trend_direction"].isin(trend_filter)]
        if fc_search.strip():
            fw_df = fw_df[fw_df["commodity"].str.contains(fc_search.strip(), case=False, na=False)]

        # Trend summary cards
        if "trend_direction" in fw_df.columns:
            tc = fw_df["trend_direction"].value_counts()
            t1, t2, t3, t4 = st.columns(4)
            with t1: kpi("Uptrend",   str(tc.get("uptrend",0)),   "commodities rising",   "#22c55e")
            with t2: kpi("Downtrend", str(tc.get("downtrend",0)), "commodities falling",  "#ef4444")
            with t3: kpi("Stable",    str(tc.get("stable",0)),    "commodities stable",   "#94a3b8")
            if "model_used" in fw_df.columns:
                with t4: kpi("Top Model", top_model_share, "share of latest forecasts", "#8b5cf6")

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        fa_col, fb_col = st.columns(2, gap="large")

        f1d_col = "forecast_1d" if "forecast_1d" in fw_df.columns else "next_day_forecast"

        with fa_col:
            section("Expected Upward Revisions (Tomorrow)")
            up_df = fw_df.sort_values("forecast_delta_vs_latest", ascending=False).head(12)
            if not up_df.empty and f1d_col in up_df.columns:
                up_chart_df = up_df.copy()
                up_chart_df["label"] = up_chart_df["commodity"] + " (" + up_chart_df["unit"].fillna("") + ")"
                bars = (
                    alt.Chart(up_chart_df)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#22c55e")
                    .encode(
                        y=alt.Y("label:N", sort="-x", title=None, axis=alt.Axis(labelLimit=220)),
                        x=alt.X("forecast_delta_vs_latest:Q", title="Forecast Delta (Rs.)"),
                        tooltip=[
                            alt.Tooltip("commodity:N"),
                            alt.Tooltip("latest_avg_price:Q", format=".2f", title="Current"),
                            alt.Tooltip(f"{f1d_col}:Q", format=".2f", title="1-Day Forecast"),
                            alt.Tooltip("forecast_delta_vs_latest:Q", format=".2f", title="Delta"),
                        ],
                    )
                    .properties(height=280)
                    .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                    .configure_view(strokeWidth=0)
                    .configure(background="transparent")
                )
                st.altair_chart(bars, use_container_width=True)

        with fb_col:
            section("Expected Downward Revisions (Tomorrow)")
            dn_df = fw_df.sort_values("forecast_delta_vs_latest", ascending=True).head(12)
            if not dn_df.empty and f1d_col in dn_df.columns:
                dn_chart_df = dn_df.copy()
                dn_chart_df["label"] = dn_chart_df["commodity"] + " (" + dn_chart_df["unit"].fillna("") + ")"
                bars2 = (
                    alt.Chart(dn_chart_df)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#ef4444")
                    .encode(
                        y=alt.Y("label:N", sort="x", title=None, axis=alt.Axis(labelLimit=220)),
                        x=alt.X("forecast_delta_vs_latest:Q", title="Forecast Delta (Rs.)"),
                        tooltip=[
                            alt.Tooltip("commodity:N"),
                            alt.Tooltip("latest_avg_price:Q", format=".2f", title="Current"),
                            alt.Tooltip(f"{f1d_col}:Q", format=".2f", title="1-Day Forecast"),
                            alt.Tooltip("forecast_delta_vs_latest:Q", format=".2f", title="Delta"),
                        ],
                    )
                    .properties(height=280)
                    .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                    .configure_view(strokeWidth=0)
                    .configure(background="transparent")
                )
                st.altair_chart(bars2, use_container_width=True)

        # Full forecast table
        section("Full Forecast Table — Multi-Horizon with Confidence Intervals")
        fc_show_cols = [c for c in [
            "commodity","unit","latest_avg_price",
            "forecast_1d","forecast_7d","forecast_30d",
            "forecast_lower_80","forecast_upper_80",
            "forecast_delta_vs_latest","trend_direction",
            "price_volatility_pct","model_used","backtest_mae",
            "latest_history_confidence_band",
        ] if c in fw_df.columns]
        render_badge_table(
            fw_df[fc_show_cols].reset_index(drop=True),
            {"trend_direction": trend_badge},
        )
        st.download_button(
            "⬇ Download Forecast CSV", to_csv_bytes(fw_df),
            file_name=f"kalimati_forecast_{latest_saved_bs_date}.csv", mime="text/csv",
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Commodity Trends
# ══════════════════════════════════════════════════════════════════════════════
with tab_trends:
    section("Historical Price Trend")

    today_comm = history_df[history_df["scrape_date_bs"] == latest_saved_bs_date]
    trend_keys = sorted(
        (today_comm["commodity"].fillna("").astype(str)
         + " (" + today_comm["unit"].fillna("N/A").astype(str) + ")").unique().tolist()
    )

    if not trend_keys:
        st.info("No commodity data available for latest date.")
    else:
        tc_col, tr_col = st.columns([1, 3], gap="medium")
        with tc_col:
            sel_key = st.selectbox("Select Commodity", trend_keys, key="trend_key")
            conf_filter = st.multiselect(
                "Confidence Bands",
                ["current_live","stronger_historical","medium_confidence_historical","low_confidence_historical"],
                default=["current_live","stronger_historical"],
                key="trend_conf",
            )
            show_range = st.toggle("Show min/max range", value=True, key="trend_range")

        commodity_name = sel_key.split(" (")[0]
        unit_name = sel_key.split("(")[1].rstrip(")") if "(" in sel_key else None

        if unit_name and unit_name != "N/A":
            trend_data = history_df[
                (history_df["commodity"] == commodity_name) &
                (history_df["unit"] == unit_name)
            ].copy()
        else:
            trend_data = history_df[history_df["commodity"] == commodity_name].copy()

        if conf_filter and "history_confidence_band" in trend_data.columns:
            trend_data = trend_data[trend_data["history_confidence_band"].isin(conf_filter)]

        trend_data = trend_data.sort_values("sort_date").dropna(subset=["sort_date","avg_price"]).copy()
        trend_data["date"] = pd.to_datetime(trend_data["sort_date"])

        with tr_col:
            if trend_data.empty:
                st.info("No history data for selected commodity and filters.")
            else:
                # Summary metrics
                tm1, tm2, tm3, tm4 = st.columns(4)
                tm1.metric("History Points", len(trend_data))
                tm2.metric("Current Price", f"Rs. {trend_data.iloc[-1]['avg_price']:.0f}")
                tm3.metric("All-time High",  f"Rs. {trend_data['avg_price'].max():.0f}")
                tm4.metric("All-time Low",   f"Rs. {trend_data['avg_price'].min():.0f}")

                # Line chart
                base = alt.Chart(trend_data).encode(x=alt.X("date:T", title="Date"))

                line = base.mark_line(color="#22c55e", strokeWidth=2).encode(
                    y=alt.Y("avg_price:Q", title="Average Price (Rs.)"),
                    tooltip=[
                        alt.Tooltip("date:T", title="Date"),
                        alt.Tooltip("avg_price:Q", format=".2f", title="Avg Price"),
                        alt.Tooltip("min_price:Q", format=".2f", title="Min"),
                        alt.Tooltip("max_price:Q", format=".2f", title="Max"),
                        alt.Tooltip("scrape_date_bs:N", title="BS Date"),
                    ],
                )

                if show_range and "min_price" in trend_data.columns and "max_price" in trend_data.columns:
                    band = base.mark_area(opacity=0.15, color="#22c55e").encode(
                        y=alt.Y("min_price:Q"),
                        y2=alt.Y2("max_price:Q"),
                    )
                    chart_layers = alt.layer(band, line)
                else:
                    chart_layers = line

                final_chart = (
                    chart_layers
                    .properties(height=380)
                    .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                    .configure_view(strokeWidth=0)
                    .configure(background="transparent")
                )
                st.altair_chart(final_chart, use_container_width=True)

                st.download_button(
                    "⬇ Download Trend CSV",
                    to_csv_bytes(trend_data[["date","scrape_date_bs","commodity","unit","avg_price","min_price","max_price"]]),
                    file_name=f"kalimati_trend_{commodity_name}.csv", mime="text/csv",
                )

                with st.expander("Raw trend data table"):
                    st.dataframe(
                        trend_data[["date","scrape_date_bs","avg_price","min_price","max_price","history_confidence_band"]]
                        .rename(columns={"date":"AD Date","scrape_date_bs":"BS Date"})
                        .reset_index(drop=True),
                        use_container_width=True, height=280,
                    )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — Data Quality
# ══════════════════════════════════════════════════════════════════════════════
with tab_quality:
    section("Policy Flag Summary")

    dq1, dq2 = st.columns(2, gap="large")

    with dq1:
        st.caption("Row Depth Policy Flags")
        if not row_depth_policy_df.empty:
            sev_summary = row_depth_policy_df["row_depth_severity"].value_counts().reset_index()
            sev_summary.columns = ["severity", "dates"]
            sev_chart = (
                alt.Chart(sev_summary)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("severity:N", title=None),
                    y=alt.Y("dates:Q", title="Dates"),
                    color=alt.Color(
                        "severity:N",
                        scale=alt.Scale(
                            domain=["normal","low","critical"],
                            range=["#22c55e","#eab308","#ef4444"],
                        ),
                        legend=None,
                    ),
                    tooltip=["severity:N", "dates:Q"],
                )
                .properties(height=200)
                .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                .configure_view(strokeWidth=0)
                .configure(background="transparent")
            )
            st.altair_chart(sev_chart, use_container_width=True)
            act_summary = row_depth_policy_df["policy_action"].value_counts()
            for act, cnt in act_summary.items():
                badge_cls = "badge-red" if act=="exclude" else ("badge-yellow" if act=="manual_review" else "badge-green")
                st.markdown(f'<span class="badge {badge_cls}">{act}</span> <span style="color:#94a3b8;font-size:0.85rem">{cnt:,} dates</span>', unsafe_allow_html=True)
        else:
            st.info("No row depth policy flags found.")

    with dq2:
        st.caption("Price Quality Policy Flags")
        if not price_quality_policy_df.empty:
            issue_summary = price_quality_policy_df["price_issue_type"].value_counts().reset_index()
            issue_summary.columns = ["issue", "rows"]
            issue_chart = (
                alt.Chart(issue_summary)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("issue:N", title=None, axis=alt.Axis(labelAngle=-20)),
                    y=alt.Y("rows:Q", title="Rows"),
                    color=alt.Color(
                        "issue:N",
                        scale=alt.Scale(
                            domain=["statistical_outlier","zero_or_negative_price","invalid_price_logic"],
                            range=["#f59e0b","#ef4444","#8b5cf6"],
                        ),
                        legend=None,
                    ),
                    tooltip=["issue:N", "rows:Q"],
                )
                .properties(height=200)
                .configure_axis(labelColor="#94a3b8", titleColor="#94a3b8", gridColor="#1e293b")
                .configure_view(strokeWidth=0)
                .configure(background="transparent")
            )
            st.altair_chart(issue_chart, use_container_width=True)
            act2 = price_quality_policy_df["policy_action"].value_counts()
            for act, cnt in act2.items():
                badge_cls = "badge-red" if act=="exclude" else ("badge-yellow" if act=="manual_review" else "badge-green")
                st.markdown(f'<span class="badge {badge_cls}">{act}</span> <span style="color:#94a3b8;font-size:0.85rem">{cnt:,} rows</span>', unsafe_allow_html=True)
        else:
            st.info("No price quality policy flags found (data is clean).")

    section("Flagged Commodities (Price Quality)")
    if not price_quality_policy_df.empty:
        pq_show = price_quality_policy_df.sort_values("price_issue_type").head(100)
        st.dataframe(pq_show.reset_index(drop=True), use_container_width=True, height=280)
        st.download_button(
            "⬇ Download Price Quality Flags",
            to_csv_bytes(price_quality_policy_df),
            file_name="kalimati_price_quality_flags.csv", mime="text/csv",
        )

    section("Commodity Name Normalisation")
    norm_csv = _BASE / "commodity_normalization_fuzzy_pairs.csv"
    if norm_csv.exists():
        try:
            norm_df = pd.read_csv(norm_csv)
        except pd.errors.EmptyDataError:
            norm_df = pd.DataFrame()

        if not norm_df.empty:
            st.caption(f"{len(norm_df)} fuzzy commodity name pairs detected (similarity ≥ 0.84)")
            st.dataframe(norm_df.head(50).reset_index(drop=True), use_container_width=True, height=240)
        else:
            st.success("No fuzzy commodity name pairs found — names are clean.")
    else:
        st.info("Run `python -m analysis.commodity_normalization_audit` to generate.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(
    """<hr style="border:1px solid #1e293b;margin-top:32px">
    <div style="text-align:center;color:#475569;font-size:0.75rem;padding:8px 0">
        Kalimati Saathi · Forecast Intelligence · Data from kalimatimarket.gov.np
    </div>""",
    unsafe_allow_html=True,
)

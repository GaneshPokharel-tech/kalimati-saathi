import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"

HISTORY_CSV = PROCESSED / "kalimati_price_history.csv"
ANOMALY_CSV = PROCESSED / "kalimati_anomaly_report.csv"
FORECAST_CSV = PROCESSED / "kalimati_forecast_baseline.csv"
MARKET_BRIEF_MD = PROCESSED / "kalimati_market_brief.md"
COMMODITY_NAME_COUNTS_CSV = PROCESSED / "commodity_name_counts.csv"
COMMODITY_NORMALIZATION_EXACT_GROUPS_CSV = PROCESSED / "commodity_normalization_exact_groups.csv"
COMMODITY_NORMALIZATION_FUZZY_PAIRS_CSV = PROCESSED / "commodity_normalization_fuzzy_pairs.csv"
DATA_QUALITY_AUDIT_SUMMARY_TXT = PROCESSED / "data_quality_audit_summary.txt"
ROW_DEPTH_POLICY_FLAGS_CSV = PROCESSED / "row_depth_policy_flags.csv"
PRICE_QUALITY_POLICY_FLAGS_CSV = PROCESSED / "price_quality_policy_flags.csv"
PIPELINE_STATUS_JSON = PROCESSED / "kalimati_pipeline_status.json"
SCRAPE_STATUS_JSON = PROCESSED / "kalimati_last_scrape_status.json"
SQLITE_DB = PROCESSED / "kalimati.db"


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)
    print(f"[OK] {message}")


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def table_count(conn, table_name):
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def read_csv_allow_empty(path, label):
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        print(f"[OK] {label} is empty and treated as optional")
        return pd.DataFrame()


def main():
    print("Kalimati Saathi smoke test")
    print("=" * 60)

    assert_true(HISTORY_CSV.exists(), "history CSV exists")
    assert_true(ANOMALY_CSV.exists(), "anomaly CSV exists")
    assert_true(FORECAST_CSV.exists(), "forecast CSV exists")
    assert_true(MARKET_BRIEF_MD.exists(), "market brief markdown exists")
    assert_true(COMMODITY_NAME_COUNTS_CSV.exists(), "commodity name counts CSV exists")
    assert_true(COMMODITY_NORMALIZATION_EXACT_GROUPS_CSV.exists(), "commodity normalization exact groups CSV exists")
    assert_true(COMMODITY_NORMALIZATION_FUZZY_PAIRS_CSV.exists(), "commodity normalization fuzzy pairs CSV exists")
    assert_true(DATA_QUALITY_AUDIT_SUMMARY_TXT.exists(), "data quality audit summary text exists")
    assert_true(ROW_DEPTH_POLICY_FLAGS_CSV.exists(), "row depth policy flags CSV exists")
    assert_true(PRICE_QUALITY_POLICY_FLAGS_CSV.exists(), "price quality policy flags CSV exists")
    assert_true(PIPELINE_STATUS_JSON.exists(), "pipeline status JSON exists")
    assert_true(SCRAPE_STATUS_JSON.exists(), "scrape status JSON exists")
    assert_true(SQLITE_DB.exists(), "SQLite DB exists")

    history_df = pd.read_csv(HISTORY_CSV).copy()
    anomaly_df = pd.read_csv(ANOMALY_CSV)
    forecast_df = pd.read_csv(FORECAST_CSV)
    commodity_name_counts_df = pd.read_csv(COMMODITY_NAME_COUNTS_CSV)
    commodity_normalization_exact_groups_df = read_csv_allow_empty(
        COMMODITY_NORMALIZATION_EXACT_GROUPS_CSV,
        "commodity normalization exact groups CSV",
    )
    commodity_normalization_fuzzy_pairs_df = read_csv_allow_empty(
        COMMODITY_NORMALIZATION_FUZZY_PAIRS_CSV,
        "commodity normalization fuzzy pairs CSV",
    )
    row_depth_policy_flags_df = pd.read_csv(ROW_DEPTH_POLICY_FLAGS_CSV)
    price_quality_policy_flags_df = pd.read_csv(PRICE_QUALITY_POLICY_FLAGS_CSV)
    pipeline_status = load_json(PIPELINE_STATUS_JSON)
    scrape_status = load_json(SCRAPE_STATUS_JSON)
    market_brief_text = MARKET_BRIEF_MD.read_text(encoding="utf-8")
    data_quality_audit_summary_text = DATA_QUALITY_AUDIT_SUMMARY_TXT.read_text(encoding="utf-8")

    assert_true(len(history_df) > 0, "history CSV has rows")
    assert_true(len(anomaly_df) > 0, "anomaly CSV has rows")
    assert_true(len(forecast_df) > 0, "forecast CSV has rows")
    assert_true(len(commodity_name_counts_df) > 0, "commodity name counts CSV has rows")
    assert_true(len(row_depth_policy_flags_df) > 0, "row depth policy flags CSV has rows")
    assert_true(len(price_quality_policy_flags_df) > 0, "price quality policy flags CSV has rows")
    assert_true("Kalimati Market Brief" in market_brief_text, "market brief has expected title")
    assert_true("Kalimati Saathi Data Quality Audit" in data_quality_audit_summary_text, "data quality audit summary has expected title")
    assert_true("history_rows" in pipeline_status, "pipeline status has history_rows")
    assert_true("status" in scrape_status, "scrape status has status field")

    required_row_depth_policy_columns = {
        "requested_date_ad",
        "scrape_date_bs",
        "row_count",
        "history_confidence_band",
        "row_depth_severity",
        "exclude_from_default_model_window",
        "manual_review",
        "policy_action",
    }
    assert_true(
        required_row_depth_policy_columns.issubset(set(row_depth_policy_flags_df.columns)),
        "row depth policy flags CSV has required columns",
    )

    required_price_quality_policy_columns = {
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
    }
    assert_true(
        required_price_quality_policy_columns.issubset(set(price_quality_policy_flags_df.columns)),
        "price quality policy flags CSV has required columns",
    )

    conn = sqlite3.connect(SQLITE_DB)
    try:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

        required_tables = {
            "price_history",
            "anomaly_report",
            "forecast_baseline",
            "commodity_name_counts",
            "data_quality_audit_summary",
            "pipeline_status",
            "scrape_status",
            "market_brief",
            "row_depth_policy_flags",
            "price_quality_policy_flags",
        }

        if len(commodity_normalization_exact_groups_df) > 0:
            required_tables.add("commodity_normalization_exact_groups")
        if len(commodity_normalization_fuzzy_pairs_df) > 0:
            required_tables.add("commodity_normalization_fuzzy_pairs")

        missing_tables = required_tables - tables
        assert_true(not missing_tables, f"required SQLite tables exist: {sorted(required_tables)}")

        sqlite_history_rows = table_count(conn, "price_history")
        sqlite_anomaly_rows = table_count(conn, "anomaly_report")
        sqlite_forecast_rows = table_count(conn, "forecast_baseline")
        sqlite_commodity_name_counts_rows = table_count(conn, "commodity_name_counts")
        sqlite_row_depth_policy_flags_rows = table_count(conn, "row_depth_policy_flags")
        sqlite_price_quality_policy_flags_rows = table_count(conn, "price_quality_policy_flags")
        sqlite_data_quality_audit_summary_rows = table_count(conn, "data_quality_audit_summary")
        sqlite_pipeline_rows = table_count(conn, "pipeline_status")
        sqlite_scrape_rows = table_count(conn, "scrape_status")
        sqlite_brief_rows = table_count(conn, "market_brief")

        assert_true(sqlite_history_rows == len(history_df), "SQLite history row count matches CSV")
        assert_true(sqlite_anomaly_rows == len(anomaly_df), "SQLite anomaly row count matches CSV")
        assert_true(sqlite_forecast_rows == len(forecast_df), "SQLite forecast row count matches CSV")
        assert_true(
            sqlite_commodity_name_counts_rows == len(commodity_name_counts_df),
            "SQLite commodity_name_counts row count matches CSV"
        )
        assert_true(
            sqlite_row_depth_policy_flags_rows == len(row_depth_policy_flags_df),
            "SQLite row_depth_policy_flags row count matches CSV"
        )
        assert_true(
            sqlite_price_quality_policy_flags_rows == len(price_quality_policy_flags_df),
            "SQLite price_quality_policy_flags row count matches CSV"
        )

        if "commodity_normalization_exact_groups" in tables:
            sqlite_exact_groups_rows = table_count(conn, "commodity_normalization_exact_groups")
            assert_true(
                sqlite_exact_groups_rows == len(commodity_normalization_exact_groups_df),
                "SQLite commodity_normalization_exact_groups row count matches CSV"
            )
        else:
            assert_true(
                len(commodity_normalization_exact_groups_df) == 0,
                "commodity_normalization_exact_groups table may be absent only when CSV is empty"
            )

        if "commodity_normalization_fuzzy_pairs" in tables:
            sqlite_fuzzy_pairs_rows = table_count(conn, "commodity_normalization_fuzzy_pairs")
            assert_true(
                sqlite_fuzzy_pairs_rows == len(commodity_normalization_fuzzy_pairs_df),
                "SQLite commodity_normalization_fuzzy_pairs row count matches CSV"
            )
        else:
            assert_true(
                len(commodity_normalization_fuzzy_pairs_df) == 0,
                "commodity_normalization_fuzzy_pairs table may be absent only when CSV is empty"
            )

        assert_true(sqlite_data_quality_audit_summary_rows == 1, "SQLite data_quality_audit_summary has one row")
        assert_true(sqlite_pipeline_rows == 1, "SQLite pipeline_status has one row")
        assert_true(sqlite_scrape_rows == 1, "SQLite scrape_status has one row")
        assert_true(sqlite_brief_rows == 1, "SQLite market_brief has one row")

    finally:
        conn.close()

    history_df["requested_date_ad_dt"] = pd.to_datetime(history_df["requested_date_ad"], errors="coerce")
    history_df["fetched_at_utc_dt"] = pd.to_datetime(
        history_df["fetched_at_utc"], errors="coerce", utc=True
    ).dt.tz_convert(None)
    history_df["sort_date"] = history_df["requested_date_ad_dt"].fillna(history_df["fetched_at_utc_dt"])

    latest_idx = history_df["sort_date"].idxmax()
    latest_history_bs = history_df.loc[latest_idx, "scrape_date_bs"]

    assert_true(
        pipeline_status.get("latest_history_bs_date") == latest_history_bs,
        "pipeline status latest_history_bs_date matches latest history by sort_date"
    )

    print()
    print("Smoke test passed successfully.")


if __name__ == "__main__":
    main()

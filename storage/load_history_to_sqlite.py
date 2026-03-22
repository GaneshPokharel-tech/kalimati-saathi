import json
import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path("data/processed/kalimati.db")

HISTORY_CSV = Path("data/processed/kalimati_price_history.csv")
ANOMALY_CSV = Path("data/processed/kalimati_anomaly_report.csv")
FORECAST_CSV = Path("data/processed/kalimati_forecast_baseline.csv")
MARKET_BRIEF_MD = Path("data/processed/kalimati_market_brief.md")

COMMODITY_NAME_COUNTS_CSV = Path("data/processed/commodity_name_counts.csv")
COMMODITY_NORMALIZATION_EXACT_GROUPS_CSV = Path("data/processed/commodity_normalization_exact_groups.csv")
COMMODITY_NORMALIZATION_FUZZY_PAIRS_CSV = Path("data/processed/commodity_normalization_fuzzy_pairs.csv")
DATA_QUALITY_AUDIT_SUMMARY_TXT = Path("data/processed/data_quality_audit_summary.txt")
BACKFILL_COVERAGE_CSV = Path("data/processed/kalimati_backfill_coverage.csv")
ROW_DEPTH_POLICY_FLAGS_CSV = Path("data/processed/row_depth_policy_flags.csv")
PRICE_QUALITY_POLICY_FLAGS_CSV = Path("data/processed/price_quality_policy_flags.csv")

PIPELINE_STATUS_JSON = Path("data/processed/kalimati_pipeline_status.json")
SCRAPE_STATUS_JSON = Path("data/processed/kalimati_last_scrape_status.json")


def load_csv_to_table(conn, csv_path, table_name):
    if not csv_path.exists():
        print(f"Skipped {table_name}: file not found -> {csv_path}")
        return 0

    try:
        df = pd.read_csv(csv_path).copy()
    except pd.errors.EmptyDataError:
        print(f"Skipped {table_name}: empty CSV -> {csv_path}")
        return 0

    df.to_sql(table_name, conn, if_exists="replace", index=False)

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Loaded {table_name}: {row_count} rows")
    return row_count


def load_json_to_table(conn, json_path, table_name):
    if not json_path.exists():
        print(f"Skipped {table_name}: file not found -> {json_path}")
        return 0

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    df = pd.DataFrame([payload])
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Loaded {table_name}: {row_count} rows")
    return row_count


def load_market_brief_to_table(conn, md_path, table_name):
    if not md_path.exists():
        print(f"Skipped {table_name}: file not found -> {md_path}")
        return 0

    content = md_path.read_text(encoding="utf-8")
    df = pd.DataFrame([{"brief_markdown": content}])
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Loaded {table_name}: {row_count} rows")
    return row_count


def load_text_to_table(conn, text_path, table_name, column_name="content"):
    if not text_path.exists():
        print(f"Skipped {table_name}: file not found -> {text_path}")
        return 0

    content = text_path.read_text(encoding="utf-8")
    df = pd.DataFrame([{column_name: content}])
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Loaded {table_name}: {row_count} rows")
    return row_count


def main():
    conn = sqlite3.connect(DB_PATH)

    try:
        history_rows = load_csv_to_table(conn, HISTORY_CSV, "price_history")
        anomaly_rows = load_csv_to_table(conn, ANOMALY_CSV, "anomaly_report")
        forecast_rows = load_csv_to_table(conn, FORECAST_CSV, "forecast_baseline")
        commodity_name_counts_rows = load_csv_to_table(conn, COMMODITY_NAME_COUNTS_CSV, "commodity_name_counts")
        commodity_normalization_exact_groups_rows = load_csv_to_table(
            conn, COMMODITY_NORMALIZATION_EXACT_GROUPS_CSV, "commodity_normalization_exact_groups"
        )
        commodity_normalization_fuzzy_pairs_rows = load_csv_to_table(
            conn, COMMODITY_NORMALIZATION_FUZZY_PAIRS_CSV, "commodity_normalization_fuzzy_pairs"
        )
        pipeline_status_rows = load_json_to_table(conn, PIPELINE_STATUS_JSON, "pipeline_status")
        scrape_status_rows = load_json_to_table(conn, SCRAPE_STATUS_JSON, "scrape_status")
        market_brief_rows = load_market_brief_to_table(conn, MARKET_BRIEF_MD, "market_brief")
        data_quality_audit_summary_rows = load_text_to_table(
            conn,
            DATA_QUALITY_AUDIT_SUMMARY_TXT,
            "data_quality_audit_summary",
            column_name="summary_text",
        )
        backfill_coverage_rows = load_csv_to_table(conn, BACKFILL_COVERAGE_CSV, "kalimati_backfill_coverage")
        row_depth_policy_flags_rows = load_csv_to_table(conn, ROW_DEPTH_POLICY_FLAGS_CSV, "row_depth_policy_flags")
        price_quality_policy_flags_rows = load_csv_to_table(
            conn, PRICE_QUALITY_POLICY_FLAGS_CSV, "price_quality_policy_flags"
        )

        if history_rows > 0:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_bs_date ON price_history(scrape_date_bs)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_commodity ON price_history(commodity)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_commodity_unit ON price_history(commodity, unit)")

        if anomaly_rows > 0:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_anomaly_report_commodity_unit ON anomaly_report(commodity, unit)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_anomaly_report_latest_bs_date ON anomaly_report(latest_bs_date)")

        if forecast_rows > 0:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_baseline_commodity_unit ON forecast_baseline(commodity, unit)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_baseline_latest_bs_date ON forecast_baseline(latest_bs_date)")

        if backfill_coverage_rows > 0:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_kalimati_backfill_coverage_requested_date ON kalimati_backfill_coverage(requested_date_ad)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_kalimati_backfill_coverage_status ON kalimati_backfill_coverage(status)")

        if row_depth_policy_flags_rows > 0:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_row_depth_policy_flags_requested_date ON row_depth_policy_flags(requested_date_ad)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_row_depth_policy_flags_band ON row_depth_policy_flags(history_confidence_band)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_row_depth_policy_flags_action ON row_depth_policy_flags(policy_action)")

        if price_quality_policy_flags_rows > 0:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_quality_policy_flags_requested_date ON price_quality_policy_flags(requested_date_ad)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_quality_policy_flags_commodity ON price_quality_policy_flags(commodity)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_quality_policy_flags_action ON price_quality_policy_flags(policy_action)")

        conn.commit()

        print()
        print("SQLite load completed")
        print("Database:", DB_PATH)

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        print("Tables:", ", ".join(name for (name,) in tables))
        print("Status tables loaded:", pipeline_status_rows, scrape_status_rows)
        print("Market brief table loaded:", market_brief_rows)
        print(
            "Audit tables loaded:",
            commodity_name_counts_rows,
            commodity_normalization_exact_groups_rows,
            commodity_normalization_fuzzy_pairs_rows,
            data_quality_audit_summary_rows,
        )
        print("Backfill coverage table loaded:", backfill_coverage_rows)
        print("Row-depth policy table loaded:", row_depth_policy_flags_rows)
        print("Price-quality policy table loaded:", price_quality_policy_flags_rows)

    finally:
        conn.close()


if __name__ == "__main__":
    main()

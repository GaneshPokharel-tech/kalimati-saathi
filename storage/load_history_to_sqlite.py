"""Load all pipeline output files into the SQLite database."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data/processed"

DB_PATH = PROCESSED / "kalimati.db"

HISTORY_CSV = PROCESSED / "kalimati_price_history.csv"
ANOMALY_CSV = PROCESSED / "kalimati_anomaly_report.csv"
FORECAST_CSV = PROCESSED / "kalimati_forecast_baseline.csv"
MARKET_BRIEF_MD = PROCESSED / "kalimati_market_brief.md"

COMMODITY_NAME_COUNTS_CSV = PROCESSED / "commodity_name_counts.csv"
COMMODITY_NORMALIZATION_EXACT_GROUPS_CSV = PROCESSED / "commodity_normalization_exact_groups.csv"
COMMODITY_NORMALIZATION_FUZZY_PAIRS_CSV = PROCESSED / "commodity_normalization_fuzzy_pairs.csv"
DATA_QUALITY_AUDIT_SUMMARY_TXT = PROCESSED / "data_quality_audit_summary.txt"
BACKFILL_COVERAGE_CSV = PROCESSED / "kalimati_backfill_coverage.csv"
ROW_DEPTH_POLICY_FLAGS_CSV = PROCESSED / "row_depth_policy_flags.csv"
PRICE_QUALITY_POLICY_FLAGS_CSV = PROCESSED / "price_quality_policy_flags.csv"

PIPELINE_STATUS_JSON = PROCESSED / "kalimati_pipeline_status.json"
SCRAPE_STATUS_JSON = PROCESSED / "kalimati_last_scrape_status.json"

# Only these table names are allowed to prevent any SQL injection risk.
_SAFE_TABLE_NAMES: frozenset[str] = frozenset(
    {
        "price_history",
        "anomaly_report",
        "forecast_baseline",
        "commodity_name_counts",
        "commodity_normalization_exact_groups",
        "commodity_normalization_fuzzy_pairs",
        "pipeline_status",
        "scrape_status",
        "market_brief",
        "data_quality_audit_summary",
        "kalimati_backfill_coverage",
        "row_depth_policy_flags",
        "price_quality_policy_flags",
    }
)

_STATUS_TIMESTAMP_COLUMNS: dict[str, str] = {
    "pipeline_status": "pipeline_ran_at_utc",
    "scrape_status": "scrape_ran_at_utc",
}


def _safe_count(conn: sqlite3.Connection, table_name: str) -> int:
    if table_name not in _SAFE_TABLE_NAMES:
        raise ValueError(f"Unknown table name: {table_name!r}")
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]  # noqa: S608


def _safe_single_row(
    conn: sqlite3.Connection, table_name: str
) -> dict[str, object] | None:
    if table_name not in _SAFE_TABLE_NAMES:
        raise ValueError(f"Unknown table name: {table_name!r}")

    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1", conn)
    except Exception:
        return None

    if df.empty:
        return None
    return df.iloc[0].to_dict()


def _parse_utc_timestamp(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts


def _json_payload_is_fresher(
    conn: sqlite3.Connection, table_name: str, payload: dict
) -> bool:
    timestamp_column = _STATUS_TIMESTAMP_COLUMNS.get(table_name)
    if not timestamp_column:
        return True

    incoming_ts = _parse_utc_timestamp(payload.get(timestamp_column))
    if incoming_ts is None:
        return True

    existing_row = _safe_single_row(conn, table_name)
    if not existing_row:
        return True

    existing_ts = _parse_utc_timestamp(existing_row.get(timestamp_column))
    if existing_ts is None:
        return True

    return incoming_ts >= existing_ts


def load_csv_to_table(
    conn: sqlite3.Connection, csv_path: Path, table_name: str
) -> int:
    if table_name not in _SAFE_TABLE_NAMES:
        raise ValueError(f"Unknown table name: {table_name!r}")

    if not csv_path.exists():
        print(f"Skipped {table_name}: file not found → {csv_path}")
        return 0

    try:
        df = pd.read_csv(csv_path).copy()
    except pd.errors.EmptyDataError:
        print(f"Skipped {table_name}: empty CSV → {csv_path}")
        return 0

    df.to_sql(table_name, conn, if_exists="replace", index=False)
    row_count = _safe_count(conn, table_name)
    print(f"Loaded {table_name}: {row_count:,} rows")
    return row_count


def load_json_to_table(
    conn: sqlite3.Connection, json_path: Path, table_name: str
) -> int:
    if table_name not in _SAFE_TABLE_NAMES:
        raise ValueError(f"Unknown table name: {table_name!r}")

    if not json_path.exists():
        print(f"Skipped {table_name}: file not found → {json_path}")
        return 0

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    if not _json_payload_is_fresher(conn, table_name, payload):
        print(f"Skipped {table_name}: newer SQLite row already present → {json_path}")
        return _safe_count(conn, table_name)

    pd.DataFrame([payload]).to_sql(table_name, conn, if_exists="replace", index=False)
    row_count = _safe_count(conn, table_name)
    print(f"Loaded {table_name}: {row_count} rows")
    return row_count


def load_market_brief_to_table(
    conn: sqlite3.Connection, md_path: Path, table_name: str
) -> int:
    if table_name not in _SAFE_TABLE_NAMES:
        raise ValueError(f"Unknown table name: {table_name!r}")

    if not md_path.exists():
        print(f"Skipped {table_name}: file not found → {md_path}")
        return 0

    content = md_path.read_text(encoding="utf-8")
    pd.DataFrame([{"brief_markdown": content}]).to_sql(
        table_name, conn, if_exists="replace", index=False
    )
    row_count = _safe_count(conn, table_name)
    print(f"Loaded {table_name}: {row_count} rows")
    return row_count


def load_text_to_table(
    conn: sqlite3.Connection,
    text_path: Path,
    table_name: str,
    column_name: str = "content",
) -> int:
    if table_name not in _SAFE_TABLE_NAMES:
        raise ValueError(f"Unknown table name: {table_name!r}")

    if not text_path.exists():
        print(f"Skipped {table_name}: file not found → {text_path}")
        return 0

    content = text_path.read_text(encoding="utf-8")
    pd.DataFrame([{column_name: content}]).to_sql(
        table_name, conn, if_exists="replace", index=False
    )
    row_count = _safe_count(conn, table_name)
    print(f"Loaded {table_name}: {row_count} rows")
    return row_count


def main() -> None:
    PROCESSED.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    try:
        history_rows = load_csv_to_table(conn, HISTORY_CSV, "price_history")
        anomaly_rows = load_csv_to_table(conn, ANOMALY_CSV, "anomaly_report")
        forecast_rows = load_csv_to_table(conn, FORECAST_CSV, "forecast_baseline")
        commodity_name_counts_rows = load_csv_to_table(
            conn, COMMODITY_NAME_COUNTS_CSV, "commodity_name_counts"
        )
        commodity_normalization_exact_groups_rows = load_csv_to_table(
            conn,
            COMMODITY_NORMALIZATION_EXACT_GROUPS_CSV,
            "commodity_normalization_exact_groups",
        )
        commodity_normalization_fuzzy_pairs_rows = load_csv_to_table(
            conn,
            COMMODITY_NORMALIZATION_FUZZY_PAIRS_CSV,
            "commodity_normalization_fuzzy_pairs",
        )
        pipeline_status_rows = load_json_to_table(
            conn, PIPELINE_STATUS_JSON, "pipeline_status"
        )
        scrape_status_rows = load_json_to_table(
            conn, SCRAPE_STATUS_JSON, "scrape_status"
        )
        market_brief_rows = load_market_brief_to_table(
            conn, MARKET_BRIEF_MD, "market_brief"
        )
        data_quality_audit_summary_rows = load_text_to_table(
            conn,
            DATA_QUALITY_AUDIT_SUMMARY_TXT,
            "data_quality_audit_summary",
            column_name="summary_text",
        )
        backfill_coverage_rows = load_csv_to_table(
            conn, BACKFILL_COVERAGE_CSV, "kalimati_backfill_coverage"
        )
        row_depth_policy_flags_rows = load_csv_to_table(
            conn, ROW_DEPTH_POLICY_FLAGS_CSV, "row_depth_policy_flags"
        )
        price_quality_policy_flags_rows = load_csv_to_table(
            conn, PRICE_QUALITY_POLICY_FLAGS_CSV, "price_quality_policy_flags"
        )

        # ── Indices ───────────────────────────────────────────────────────────
        if history_rows > 0:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ph_bs_date ON price_history(scrape_date_bs)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ph_commodity ON price_history(commodity)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ph_commodity_unit ON price_history(commodity, unit)"
            )

        if anomaly_rows > 0:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ar_commodity_unit ON anomaly_report(commodity, unit)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ar_bs_date ON anomaly_report(latest_bs_date)"
            )

        if forecast_rows > 0:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fb_commodity_unit ON forecast_baseline(commodity, unit)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fb_bs_date ON forecast_baseline(latest_bs_date)"
            )

        if backfill_coverage_rows > 0:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_bc_date ON kalimati_backfill_coverage(requested_date_ad)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_bc_status ON kalimati_backfill_coverage(status)"
            )

        if row_depth_policy_flags_rows > 0:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_rdpf_date ON row_depth_policy_flags(requested_date_ad)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_rdpf_band ON row_depth_policy_flags(history_confidence_band)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_rdpf_action ON row_depth_policy_flags(policy_action)"
            )

        if price_quality_policy_flags_rows > 0:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pqpf_date ON price_quality_policy_flags(requested_date_ad)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pqpf_commodity ON price_quality_policy_flags(commodity)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pqpf_action ON price_quality_policy_flags(policy_action)"
            )

        conn.commit()

        print()
        print("SQLite load completed")
        print(f"Database: {DB_PATH}")

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        print(f"Tables: {', '.join(name for (name,) in tables)}")
        print(
            f"Status tables loaded: {pipeline_status_rows}, {scrape_status_rows}"
        )
        print(f"Market brief table loaded: {market_brief_rows}")
        print(
            f"Audit tables loaded: {commodity_name_counts_rows}, "
            f"{commodity_normalization_exact_groups_rows}, "
            f"{commodity_normalization_fuzzy_pairs_rows}, "
            f"{data_quality_audit_summary_rows}"
        )
        print(f"Backfill coverage table loaded: {backfill_coverage_rows}")
        print(f"Row-depth policy table loaded: {row_depth_policy_flags_rows}")
        print(f"Price-quality policy table loaded: {price_quality_policy_flags_rows}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

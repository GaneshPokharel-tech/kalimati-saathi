import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"

DB_PATH = PROCESSED_DIR / "kalimati.db"
pipeline_status_path = PROCESSED_DIR / "kalimati_pipeline_status.json"
scrape_status_path = PROCESSED_DIR / "kalimati_last_scrape_status.json"
history_path = PROCESSED_DIR / "kalimati_price_history.csv"
anomaly_path = PROCESSED_DIR / "kalimati_anomaly_report.csv"
forecast_path = PROCESSED_DIR / "kalimati_forecast_baseline.csv"


def load_json(path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_single_row_table(table_name):
    if not DB_PATH.exists():
        return None

    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 1")
        row = cursor.fetchone()
        if row is None:
            return None

        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))
    except Exception:
        return None
    finally:
        conn.close()


def yes_no(value):
    if value in (True, 1, "1"):
        return "Yes"
    if value in (False, 0, "0"):
        return "No"
    return value


pipeline_status = load_single_row_table("pipeline_status")
scrape_status = load_single_row_table("scrape_status")
pipeline_status_source = "SQLite" if pipeline_status else "None"
scrape_status_source = "SQLite" if scrape_status else "None"

if not pipeline_status:
    pipeline_status = load_json(pipeline_status_path)
    pipeline_status_source = "JSON" if pipeline_status else "None"

if not scrape_status:
    scrape_status = load_json(scrape_status_path)
    scrape_status_source = "JSON" if scrape_status else "None"

print("Kalimati Saathi Pipeline Health Check")
print("=" * 60)

print("SQLite DB exists:", DB_PATH.exists())
print("pipeline_status.json exists:", pipeline_status_path.exists())
print("last_scrape_status.json exists:", scrape_status_path.exists())
print("history CSV exists:", history_path.exists())
print("anomaly CSV exists:", anomaly_path.exists())
print("forecast CSV exists:", forecast_path.exists())
print()

print("Status sources")
print("- pipeline status source:", pipeline_status_source)
print("- scrape status source:", scrape_status_source)
print()

if scrape_status:
    print("Last scrape summary")
    print("- status:", scrape_status.get("status"))
    print("- requested mode:", scrape_status.get("requested_mode"))
    print("- requested AD date:", scrape_status.get("requested_date_ad"))
    print("- returned BS date:", scrape_status.get("returned_bs_date"))
    print("- row count:", scrape_status.get("row_count"))
    print("- wrote outputs:", yes_no(scrape_status.get("wrote_outputs")))
    print("- scrape ran at UTC:", scrape_status.get("scrape_ran_at_utc"))
    print()

if pipeline_status:
    print("Pipeline summary")
    print("- pipeline ran at UTC:", pipeline_status.get("pipeline_ran_at_utc"))
    print("- history exists:", yes_no(pipeline_status.get("history_exists")))
    print("- history rows:", pipeline_status.get("history_rows"))
    print("- latest history BS date:", pipeline_status.get("latest_history_bs_date"))
    print("- latest history AD date:", pipeline_status.get("latest_history_ad_date"))
    print()

if scrape_status and scrape_status.get("status") == "no_data":
    print("WARNING: latest scrape returned no rows.")
    print("Dashboard is expected to fall back to latest saved history date.")
elif scrape_status and scrape_status.get("status") == "saved":
    print("OK: latest scrape saved successfully.")
else:
    print("INFO: scrape status unavailable or unknown.")

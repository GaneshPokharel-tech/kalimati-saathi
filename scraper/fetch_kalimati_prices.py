"""
Scraper for Kalimati Market vegetable prices.
Fetches daily prices (or a historical date) from the official website and
appends to the cumulative price history CSV.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent

URL = "https://kalimatimarket.gov.np/price"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = 45
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries

RAW_DIR = ROOT / "data/raw"
PROCESSED_DIR = ROOT / "data/processed"
ARCHIVE_DIR = ROOT / "data/archive"

DAILY_RAW_PATH = RAW_DIR / "kalimati_daily_prices.csv"
DAILY_CLEAN_PATH = PROCESSED_DIR / "kalimati_daily_prices_clean.csv"
HISTORY_PATH = PROCESSED_DIR / "kalimati_price_history.csv"
HTML_ARCHIVE_PATH = ARCHIVE_DIR / "kalimati_price_page.html"
SCRAPE_STATUS_PATH = PROCESSED_DIR / "kalimati_last_scrape_status.json"

COLUMN_ORDER = [
    "fetched_at_utc",
    "requested_date_ad",
    "scrape_date_bs",
    "commodity",
    "unit",
    "min_price",
    "max_price",
    "avg_price",
]


def _ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def empty_dataframe() -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMN_ORDER)


def clean_text(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def extract_number(text) -> float | None:
    if text is None:
        return None
    nep_to_eng = str.maketrans("०१२३४५६७८९", "0123456789")
    cleaned = str(text).translate(nep_to_eng)
    cleaned = cleaned.replace("रू", "").replace(",", "").strip()
    match = re.search(r"\d+(?:\.\d+)?", cleaned)
    return float(match.group()) if match else None


def normalize_unit(unit) -> str:
    u = str(unit).strip()
    u = re.sub(r"\s+", "", u)
    u = u.replace(".", "")
    if u in ["केजी", "केजि"]:
        return "केजी"
    if u == "दर्जन":
        return "दर्जन"
    if u == "प्रतिगोटा":
        return "प्रति गोटा"
    return str(unit).strip()


def _write_html_archives(html: str, requested_date_ad: str | None = None) -> None:
    HTML_ARCHIVE_PATH.write_text(html, encoding="utf-8")
    key = requested_date_ad or f"latest_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    (ARCHIVE_DIR / f"kalimati_price_page_{key}.html").write_text(html, encoding="utf-8")


def fetch_page_html(requested_date_ad: str | None = None) -> str:
    """
    Fetch the price page HTML with retry logic and proper error handling.
    Raises requests.HTTPError or requests.ConnectionError on unrecoverable failure.
    """
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = requests.Session()
            get_response = session.get(URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            get_response.raise_for_status()

            if not requested_date_ad:
                return get_response.text

            get_soup = BeautifulSoup(get_response.text, "lxml")
            token_input = get_soup.find("input", {"name": "_token"})
            csrf_token = token_input.get("value") if token_input else None

            if not csrf_token:
                raise ValueError("CSRF token not found on page — site structure may have changed")

            payload = {"_token": csrf_token, "datePricing": requested_date_ad}
            post_response = session.post(
                URL, headers=HEADERS, data=payload, timeout=REQUEST_TIMEOUT
            )
            post_response.raise_for_status()
            return post_response.text

        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                print(
                    f"Fetch attempt {attempt}/{MAX_RETRIES} failed: {exc}. "
                    f"Retrying in {RETRY_DELAY}s…"
                )
                time.sleep(RETRY_DELAY)
            else:
                print(f"All {MAX_RETRIES} fetch attempts failed.")
                raise

    raise RuntimeError("Unexpected retry loop exit") from last_exc


def parse_prices_from_html(
    html: str, requested_date_ad: str | None = None
) -> tuple[pd.DataFrame, str]:
    fetched_at_utc = datetime.now(timezone.utc).isoformat()
    soup = BeautifulSoup(html, "lxml")

    _write_html_archives(html, requested_date_ad=requested_date_ad)

    page_text = soup.get_text("\n", strip=True)
    date_match = re.search(r"वि\.सं\.\s*([^\n]+)", page_text)
    scrape_date_bs = clean_text(date_match.group(1)) if date_match else ""

    table = soup.find("table")
    if table is None:
        raise ValueError("No price table found on page — site may have changed structure")

    data = []
    for row in table.find_all("tr")[1:]:
        cols = [
            clean_text(td.get_text(" ", strip=True))
            for td in row.find_all(["td", "th"])
        ]
        if len(cols) >= 5:
            data.append(
                {
                    "fetched_at_utc": fetched_at_utc,
                    "requested_date_ad": requested_date_ad or "",
                    "scrape_date_bs": scrape_date_bs,
                    "commodity": cols[0].strip(),
                    "unit": normalize_unit(cols[1]),
                    "min_price": extract_number(cols[2]),
                    "max_price": extract_number(cols[3]),
                    "avg_price": extract_number(cols[4]),
                }
            )

    if data:
        df = pd.DataFrame(data)
        df = df.drop_duplicates(
            subset=["scrape_date_bs", "commodity", "unit"], keep="last"
        )
    else:
        df = empty_dataframe()

    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = None

    df = df[COLUMN_ORDER].sort_values("commodity").reset_index(drop=True)
    return df, scrape_date_bs


def read_history_if_exists() -> pd.DataFrame:
    if HISTORY_PATH.exists():
        history_df = pd.read_csv(HISTORY_PATH)
        for col in COLUMN_ORDER:
            if col not in history_df.columns:
                history_df[col] = None
        return history_df[COLUMN_ORDER]
    return empty_dataframe()


def save_outputs(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    if df.empty:
        return read_history_if_exists(), False

    df.to_csv(DAILY_RAW_PATH, index=False, encoding="utf-8-sig")
    df.to_csv(DAILY_CLEAN_PATH, index=False, encoding="utf-8-sig")

    history_df = read_history_if_exists()
    combined_df = pd.concat([history_df, df], ignore_index=True)

    for col in COLUMN_ORDER:
        if col not in combined_df.columns:
            combined_df[col] = None

    combined_df = combined_df[COLUMN_ORDER]
    combined_df = combined_df.drop_duplicates(
        subset=["scrape_date_bs", "commodity", "unit"], keep="last"
    ).sort_values(["scrape_date_bs", "commodity"]).reset_index(drop=True)

    combined_df.to_csv(HISTORY_PATH, index=False, encoding="utf-8-sig")
    return combined_df, True


def write_scrape_status(
    requested_date_ad: str | None,
    scrape_date_bs: str,
    row_count: int,
    wrote_outputs: bool,
    history_rows: int,
    error: str | None = None,
) -> None:
    payload = {
        "scrape_ran_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_date_ad": requested_date_ad,
        "requested_mode": "historical_date" if requested_date_ad else "latest_page",
        "returned_bs_date": scrape_date_bs or None,
        "row_count": int(row_count),
        "wrote_outputs": bool(wrote_outputs),
        "history_rows_after_run": int(history_rows),
        "daily_csv_updated": bool(wrote_outputs),
        "history_updated": bool(wrote_outputs),
        "status": "saved" if wrote_outputs else ("error" if error else "no_data"),
        "error": error,
    }
    SCRAPE_STATUS_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> None:
    _ensure_dirs()

    parser = argparse.ArgumentParser(
        description="Fetch Kalimati vegetable prices"
    )
    parser.add_argument(
        "--date", help="Gregorian date in YYYY-MM-DD format", default=None
    )
    args = parser.parse_args()

    error_msg: str | None = None
    df = empty_dataframe()
    scrape_date_bs = ""
    wrote_outputs = False
    history_df = read_history_if_exists()

    try:
        html = fetch_page_html(requested_date_ad=args.date)
        df, scrape_date_bs = parse_prices_from_html(html, requested_date_ad=args.date)
        history_df, wrote_outputs = save_outputs(df)
    except Exception as exc:
        error_msg = str(exc)
        print(f"ERROR during scrape: {exc}")

    write_scrape_status(
        requested_date_ad=args.date,
        scrape_date_bs=scrape_date_bs,
        row_count=len(df),
        wrote_outputs=wrote_outputs,
        history_rows=len(history_df),
        error=error_msg,
    )

    print("Scrape completed")
    print(f"Requested AD date: {args.date or 'latest page date'}")
    print(f"Returned BS date:  {scrape_date_bs}")
    print(f"Today rows:        {len(df)}")
    print(f"History rows:      {len(history_df)}")

    if wrote_outputs:
        print(f"Saved daily clean: {DAILY_CLEAN_PATH}")
        print(f"Saved history:     {HISTORY_PATH}")
    elif error_msg:
        print(f"Scrape failed — error saved to status file")
    else:
        print("No data rows found for this date")

    print(f"Saved scrape status: {SCRAPE_STATUS_PATH}")

    if error_msg:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

import re
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://kalimatimarket.gov.np/price"
HEADERS = {"User-Agent": "Mozilla/5.0"}

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
ARCHIVE_DIR = Path("data/archive")

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

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


def empty_dataframe():
    return pd.DataFrame(columns=COLUMN_ORDER)


def clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def extract_number(text):
    if text is None:
        return None

    nep_to_eng = str.maketrans("०१२३४५६७८९", "0123456789")
    cleaned = str(text).translate(nep_to_eng)
    cleaned = cleaned.replace("रू", "").replace(",", "").strip()

    match = re.search(r"\d+(?:\.\d+)?", cleaned)
    return float(match.group()) if match else None


def normalize_unit(unit):
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


def write_html_archives(html, requested_date_ad=None):
    with open(HTML_ARCHIVE_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    if requested_date_ad:
        archive_key = requested_date_ad
    else:
        archive_key = f"latest_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    per_request_archive_path = ARCHIVE_DIR / f"kalimati_price_page_{archive_key}.html"
    with open(per_request_archive_path, "w", encoding="utf-8") as f:
        f.write(html)


def fetch_page_html(requested_date_ad=None):
    session = requests.Session()

    get_response = session.get(URL, headers=HEADERS, timeout=30)
    get_response.raise_for_status()

    if not requested_date_ad:
        return get_response.text

    get_soup = BeautifulSoup(get_response.text, "lxml")
    token_input = get_soup.find("input", {"name": "_token"})
    csrf_token = token_input.get("value") if token_input else None

    if not csrf_token:
        raise ValueError("CSRF token not found")

    payload = {
        "_token": csrf_token,
        "datePricing": requested_date_ad,
    }

    post_response = session.post(URL, headers=HEADERS, data=payload, timeout=30)
    post_response.raise_for_status()
    return post_response.text


def parse_prices_from_html(html, requested_date_ad=None):
    fetched_at_utc = datetime.now(timezone.utc).isoformat()
    soup = BeautifulSoup(html, "lxml")

    write_html_archives(html, requested_date_ad=requested_date_ad)

    page_text = soup.get_text("\n", strip=True)
    date_match = re.search(r"वि\.सं\.\s*([^\n]+)", page_text)
    scrape_date_bs = clean_text(date_match.group(1)) if date_match else ""

    table = soup.find("table")
    if table is None:
        raise ValueError("No table found on page")

    rows = table.find_all("tr")

    data = []
    for row in rows[1:]:
        cols = [clean_text(td.get_text(" ", strip=True)) for td in row.find_all(["td", "th"])]
        if len(cols) >= 5:
            data.append({
                "fetched_at_utc": fetched_at_utc,
                "requested_date_ad": requested_date_ad if requested_date_ad else "",
                "scrape_date_bs": scrape_date_bs,
                "commodity": cols[0].strip(),
                "unit": normalize_unit(cols[1]),
                "min_price": extract_number(cols[2]),
                "max_price": extract_number(cols[3]),
                "avg_price": extract_number(cols[4]),
            })

    if data:
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=["scrape_date_bs", "commodity", "unit"], keep="last")
    else:
        df = empty_dataframe()

    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = None

    df = df[COLUMN_ORDER].sort_values("commodity").reset_index(drop=True)
    return df, scrape_date_bs


def read_history_if_exists():
    if HISTORY_PATH.exists():
        history_df = pd.read_csv(HISTORY_PATH)
        for col in COLUMN_ORDER:
            if col not in history_df.columns:
                history_df[col] = None
        return history_df[COLUMN_ORDER]
    return empty_dataframe()


def save_outputs(df):
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
        subset=["scrape_date_bs", "commodity", "unit"],
        keep="last"
    ).sort_values(["scrape_date_bs", "commodity"]).reset_index(drop=True)

    combined_df.to_csv(HISTORY_PATH, index=False, encoding="utf-8-sig")
    return combined_df, True


def write_scrape_status(requested_date_ad, scrape_date_bs, row_count, wrote_outputs, history_rows):
    payload = {
        "scrape_ran_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_date_ad": requested_date_ad,
        "requested_mode": "historical_date" if requested_date_ad else "latest_page",
        "returned_bs_date": scrape_date_bs if scrape_date_bs else None,
        "row_count": int(row_count),
        "wrote_outputs": bool(wrote_outputs),
        "history_rows_after_run": int(history_rows),
        "daily_csv_updated": bool(wrote_outputs),
        "history_updated": bool(wrote_outputs),
        "status": "saved" if wrote_outputs else "no_data",
    }

    SCRAPE_STATUS_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Gregorian date in YYYY-MM-DD format", default=None)
    args = parser.parse_args()

    html = fetch_page_html(requested_date_ad=args.date)
    df, scrape_date_bs = parse_prices_from_html(html, requested_date_ad=args.date)
    history_df, wrote_outputs = save_outputs(df)
    write_scrape_status(
        requested_date_ad=args.date,
        scrape_date_bs=scrape_date_bs,
        row_count=len(df),
        wrote_outputs=wrote_outputs,
        history_rows=len(history_df),
    )

    print("Scrape completed")
    print("Requested AD date:", args.date if args.date else "latest page date")
    print("Returned BS date:", scrape_date_bs)
    print("Today rows:", len(df))
    print("History rows:", len(history_df))

    if wrote_outputs:
        print("Saved daily clean:", DAILY_CLEAN_PATH)
        print("Saved history:", HISTORY_PATH)
    else:
        print("No data rows found for this date")
        print("Daily CSV files preserved")
        print("History file not modified")

    print("Saved scrape status:", SCRAPE_STATUS_PATH)


if __name__ == "__main__":
    main()

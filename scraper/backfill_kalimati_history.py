import time
import argparse
import sys
from pathlib import Path

import pandas as pd

try:
    from scraper.fetch_kalimati_prices import (
        fetch_page_html,
        parse_prices_from_html,
        save_outputs,
    )
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from scraper.fetch_kalimati_prices import (
        fetch_page_html,
        parse_prices_from_html,
        save_outputs,
    )

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

BACKFILL_COVERAGE_PATH = PROCESSED_DIR / "kalimati_backfill_coverage.csv"


def build_longest_no_data_gap(coverage_df):
    if coverage_df.empty:
        return None

    df = coverage_df.copy()
    df["requested_date_ad_dt"] = pd.to_datetime(df["requested_date_ad"], errors="coerce")
    df = df.sort_values("requested_date_ad_dt").reset_index(drop=True)

    longest = None
    current = None

    for _, row in df.iterrows():
        if row["status"] != "no_data" or pd.isna(row["requested_date_ad_dt"]):
            current = None
            continue

        current_date = row["requested_date_ad_dt"]

        if current is None:
            current = {
                "start": current_date,
                "end": current_date,
                "days": 1,
            }
        else:
            expected_next = current["end"] + pd.Timedelta(days=1)
            if current_date == expected_next:
                current["end"] = current_date
                current["days"] += 1
            else:
                current = {
                    "start": current_date,
                    "end": current_date,
                    "days": 1,
                }

        if longest is None or current["days"] > longest["days"]:
            longest = current.copy()

    return longest


def print_summary(coverage_df):
    total_requested_dates = int(len(coverage_df))
    saved_dates = int((coverage_df["status"] == "saved").sum())
    no_data_dates = int((coverage_df["status"] == "no_data").sum())
    failed_dates = int((coverage_df["status"] == "failed").sum())

    saved_df = coverage_df.loc[coverage_df["status"] == "saved"].copy()
    first_usable_date = None
    latest_usable_date = None

    if not saved_df.empty:
        saved_df["requested_date_ad_dt"] = pd.to_datetime(saved_df["requested_date_ad"], errors="coerce")
        saved_df = saved_df.sort_values("requested_date_ad_dt")
        first_usable_date = saved_df.iloc[0]["requested_date_ad"]
        latest_usable_date = saved_df.iloc[-1]["requested_date_ad"]

    longest_gap = build_longest_no_data_gap(coverage_df)

    print()
    print("Backfill coverage summary")
    print(f"Total requested dates: {total_requested_dates}")
    print(f"Saved dates: {saved_dates}")
    print(f"No-data dates: {no_data_dates}")
    print(f"Failed dates: {failed_dates}")
    print(f"First usable date: {first_usable_date if first_usable_date else 'None'}")
    print(f"Latest usable date: {latest_usable_date if latest_usable_date else 'None'}")

    if longest_gap:
        start_str = pd.Timestamp(longest_gap["start"]).strftime("%Y-%m-%d")
        end_str = pd.Timestamp(longest_gap["end"]).strftime("%Y-%m-%d")
        print(f"Longest no-data gap: {longest_gap['days']} days ({start_str} to {end_str})")
    else:
        print("Longest no-data gap: None")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    start_date = pd.to_datetime(args.start_date).date()
    end_date = pd.to_datetime(args.end_date).date()

    if start_date > end_date:
        raise ValueError("start-date must be <= end-date")

    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")

    print(f"Backfill start: {args.start_date}")
    print(f"Backfill end: {args.end_date}")
    print(f"Total dates to request: {len(all_dates)}")
    print()

    coverage_rows = []

    success_count = 0
    no_data_count = 0
    fail_count = 0

    for dt in all_dates:
        requested_date = dt.strftime("%Y-%m-%d")
        print("=" * 70)
        print(f"Requesting: {requested_date}")

        try:
            html = fetch_page_html(requested_date_ad=requested_date)
            df, scrape_date_bs = parse_prices_from_html(html, requested_date_ad=requested_date)
            history_df, wrote_outputs = save_outputs(df)

            parsed_row_count = int(len(df))
            status = "saved" if wrote_outputs else "no_data"

            coverage_rows.append({
                "requested_date_ad": requested_date,
                "returned_bs_date": scrape_date_bs if scrape_date_bs else None,
                "parsed_row_count": parsed_row_count,
                "status": status,
            })

            print(f"Returned BS date: {scrape_date_bs}")
            print(f"Parsed rows today: {parsed_row_count}")
            print(f"History rows now: {len(history_df)}")

            if wrote_outputs:
                print("Status: saved")
                success_count += 1
            else:
                print("Status: no data for this date")
                no_data_count += 1

        except Exception as e:
            coverage_rows.append({
                "requested_date_ad": requested_date,
                "returned_bs_date": None,
                "parsed_row_count": None,
                "status": "failed",
            })
            print(f"FAILED for {requested_date}: {e}")
            fail_count += 1

        time.sleep(args.sleep_seconds)

    coverage_df = pd.DataFrame(
        coverage_rows,
        columns=["requested_date_ad", "returned_bs_date", "parsed_row_count", "status"],
    )

    if BACKFILL_COVERAGE_PATH.exists():
        try:
            existing_coverage_df = pd.read_csv(BACKFILL_COVERAGE_PATH)
        except pd.errors.EmptyDataError:
            existing_coverage_df = pd.DataFrame(columns=coverage_df.columns)

        coverage_df = pd.concat([existing_coverage_df, coverage_df], ignore_index=True)
        coverage_df = coverage_df.drop_duplicates(subset=["requested_date_ad"], keep="last")
        coverage_df["requested_date_ad_dt"] = pd.to_datetime(coverage_df["requested_date_ad"], errors="coerce")
        coverage_df = coverage_df.sort_values("requested_date_ad_dt").drop(columns=["requested_date_ad_dt"])

    coverage_df.to_csv(BACKFILL_COVERAGE_PATH, index=False, encoding="utf-8-sig")

    print()
    print("Backfill completed")
    print(f"Successful dates with saved rows: {success_count}")
    print(f"No-data dates: {no_data_count}")
    print(f"Failed dates: {fail_count}")
    print(f"Saved coverage CSV: {BACKFILL_COVERAGE_PATH}")

    print_summary(coverage_df)


if __name__ == "__main__":
    main()

import pandas as pd
from pathlib import Path

HISTORY_PATH = "data/processed/kalimati_price_history.csv"
OUTPUT_DIR = Path("data/processed")
OUTPUT_PATH = OUTPUT_DIR / "kalimati_anomaly_report.csv"


def load_history():
    df = pd.read_csv(HISTORY_PATH).copy()
    df["requested_date_ad_dt"] = pd.to_datetime(df["requested_date_ad"], errors="coerce")
    df["fetched_at_utc_dt"] = pd.to_datetime(df["fetched_at_utc"], errors="coerce", utc=True).dt.tz_convert(None)
    df["sort_date"] = df["requested_date_ad_dt"].fillna(df["fetched_at_utc_dt"])
    df = df.dropna(subset=["commodity", "unit", "avg_price", "sort_date"]).copy()
    df = df.sort_values(["commodity", "unit", "sort_date"]).reset_index(drop=True)
    return df


def build_anomaly_report(df, lookback=7, min_history=8):
    rows = []

    for (commodity, unit), group in df.groupby(["commodity", "unit"], sort=True):
        group = group.sort_values("sort_date").reset_index(drop=True)

        if len(group) < min_history:
            continue

        latest_row = group.iloc[-1]
        history_window = group.iloc[-(lookback + 1):-1].copy()

        if len(history_window) < lookback:
            continue

        baseline_median = history_window["avg_price"].median()
        baseline_mean = history_window["avg_price"].mean()
        current_price = latest_row["avg_price"]

        if baseline_median == 0 or pd.isna(baseline_median):
            continue

        abs_change = current_price - baseline_median
        pct_change = (abs_change / baseline_median) * 100

        rows.append({
            "commodity": commodity,
            "unit": unit,
            "latest_date": latest_row["sort_date"],
            "latest_bs_date": latest_row["scrape_date_bs"],
            "current_avg_price": round(current_price, 2),
            "baseline_median_7": round(baseline_median, 2),
            "baseline_mean_7": round(baseline_mean, 2),
            "abs_change_vs_median": round(abs_change, 2),
            "pct_change_vs_median": round(pct_change, 2),
            "history_points": len(group),
        })

    report_df = pd.DataFrame(rows)

    if report_df.empty:
        return report_df

    report_df["abs_pct_change"] = report_df["pct_change_vs_median"].abs()
    report_df = report_df.sort_values(
        ["abs_pct_change", "commodity"],
        ascending=[False, True]
    ).reset_index(drop=True)

    return report_df


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_history()
    report_df = build_anomaly_report(df, lookback=7, min_history=8)

    print("Total history rows:", len(df))
    print("Latest market date in data:", df["scrape_date_bs"].dropna().iloc[-1])
    print("Series evaluated:", len(report_df))
    print()

    if report_df.empty:
        print("No anomaly candidates found.")
        return

    report_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print("Saved anomaly report:", OUTPUT_PATH)
    print()

    print("Top 20 anomaly candidates vs 7-point rolling median:")
    print(
        report_df[
            [
                "commodity",
                "unit",
                "latest_bs_date",
                "current_avg_price",
                "baseline_median_7",
                "abs_change_vs_median",
                "pct_change_vs_median",
                "history_points",
            ]
        ].head(20).to_string(index=False)
    )

    print()
    print("Top positive spikes:")
    print(
        report_df.sort_values("pct_change_vs_median", ascending=False)[
            [
                "commodity",
                "unit",
                "current_avg_price",
                "baseline_median_7",
                "pct_change_vs_median",
            ]
        ].head(10).to_string(index=False)
    )

    print()
    print("Top negative drops:")
    print(
        report_df.sort_values("pct_change_vs_median", ascending=True)[
            [
                "commodity",
                "unit",
                "current_avg_price",
                "baseline_median_7",
                "pct_change_vs_median",
            ]
        ].head(10).to_string(index=False)
    )


if __name__ == "__main__":
    main()

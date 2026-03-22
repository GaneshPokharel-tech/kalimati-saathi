import pandas as pd
from pathlib import Path

from analysis.history_confidence import add_history_confidence

HISTORY_PATH = "data/processed/kalimati_price_history.csv"
OUTPUT_DIR = Path("data/processed")
OUTPUT_PATH = OUTPUT_DIR / "kalimati_forecast_baseline.csv"


def load_history():
    df = pd.read_csv(HISTORY_PATH).copy()
    df = add_history_confidence(df)
    df = df.dropna(subset=["commodity", "unit", "avg_price", "sort_date"]).copy()
    df = df.sort_values(["commodity", "unit", "sort_date"]).reset_index(drop=True)
    return df


def build_forecast(df, lookback=7, min_history=8):
    rows = []

    latest_live_df = df[df["history_confidence_band"] == "current_live"].copy()
    if latest_live_df.empty:
        return pd.DataFrame()

    latest_live_date = latest_live_df["sort_date"].max()

    for (commodity, unit), group in df.groupby(["commodity", "unit"], sort=True):
        group = group.sort_values("sort_date").reset_index(drop=True)

        if len(group) < min_history:
            continue

        latest_row = group.iloc[-1]

        if latest_row["sort_date"] != latest_live_date:
            continue

        if latest_row["history_confidence_band"] != "current_live":
            continue

        if not bool(latest_row["is_default_model_window"]):
            continue

        history_window = group.iloc[-lookback:].copy()

        if len(history_window) < lookback:
            continue

        last_price = float(latest_row["avg_price"])
        rolling_mean_7 = float(history_window["avg_price"].mean())
        rolling_median_7 = float(history_window["avg_price"].median())

        next_day_forecast = rolling_median_7

        rows.append({
            "commodity": commodity,
            "unit": unit,
            "latest_date": latest_row["sort_date"],
            "latest_bs_date": latest_row["scrape_date_bs"],
            "latest_avg_price": round(last_price, 2),
            "rolling_mean_7": round(rolling_mean_7, 2),
            "rolling_median_7": round(rolling_median_7, 2),
            "next_day_forecast": round(next_day_forecast, 2),
            "forecast_delta_vs_latest": round(next_day_forecast - last_price, 2),
            "history_points": len(group),
            "latest_history_confidence_band": latest_row["history_confidence_band"],
            "latest_history_confidence_rank": int(latest_row["history_confidence_rank"]),
            "latest_is_default_model_window": bool(latest_row["is_default_model_window"]),
        })

    forecast_df = pd.DataFrame(rows)

    if forecast_df.empty:
        return forecast_df

    forecast_df["abs_forecast_delta"] = forecast_df["forecast_delta_vs_latest"].abs()
    forecast_df = forecast_df.sort_values(
        ["abs_forecast_delta", "commodity"],
        ascending=[False, True]
    ).reset_index(drop=True)

    return forecast_df


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_history()
    forecast_df = build_forecast(df, lookback=7, min_history=8)

    print("Total history rows:", len(df))
    print("Series forecasted:", len(forecast_df))
    print()

    if forecast_df.empty:
        print("No forecast rows generated.")
        return

    forecast_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print("Saved forecast baseline:", OUTPUT_PATH)
    print()

    print("Top 20 largest forecast deltas:")
    print(
        forecast_df[
            [
                "commodity",
                "unit",
                "latest_bs_date",
                "latest_avg_price",
                "rolling_median_7",
                "next_day_forecast",
                "forecast_delta_vs_latest",
                "history_points",
                "latest_history_confidence_band",
            ]
        ].head(20).to_string(index=False)
    )


if __name__ == "__main__":
    main()

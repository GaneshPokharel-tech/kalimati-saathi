import re
import sqlite3
import unicodedata
from pathlib import Path
from difflib import SequenceMatcher

import pandas as pd

DB_PATH = Path("data/processed/kalimati.db")
HISTORY_CSV = Path("data/processed/kalimati_price_history.csv")

COUNTS_OUT = Path("data/processed/commodity_name_counts.csv")
EXACT_OUT = Path("data/processed/commodity_normalization_exact_groups.csv")
FUZZY_OUT = Path("data/processed/commodity_normalization_fuzzy_pairs.csv")


def load_history():
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        try:
            df = pd.read_sql_query("SELECT commodity, unit, scrape_date_bs FROM price_history", conn)
            if not df.empty:
                return df
        finally:
            conn.close()

    if HISTORY_CSV.exists():
        return pd.read_csv(HISTORY_CSV, usecols=["commodity", "unit", "scrape_date_bs"]).copy()

    raise FileNotFoundError("No history source found")


def canonicalize(text: str) -> str:
    text = "" if pd.isna(text) else str(text)
    text = unicodedata.normalize("NFKC", text).strip().lower()
    text = text.replace("(", " ").replace(")", " ")
    text = re.sub(r"[^0-9a-zA-Zऀ-ॿ]+", "", text)
    return text


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def main():
    df = load_history().copy()
    df = df.dropna(subset=["commodity", "unit"]).copy()

    counts_df = (
        df.groupby(["commodity", "unit"], as_index=False)
        .agg(
            observations=("scrape_date_bs", "count"),
            unique_dates=("scrape_date_bs", "nunique"),
        )
        .sort_values(["observations", "commodity"], ascending=[False, True])
        .reset_index(drop=True)
    )

    counts_df["canonical_key"] = counts_df["commodity"].map(canonicalize)
    counts_df.to_csv(COUNTS_OUT, index=False, encoding="utf-8-sig")

    exact_groups = []
    grouped = counts_df.groupby(["unit", "canonical_key"], dropna=False)

    for (unit, canonical_key), group in grouped:
        raw_names = sorted(group["commodity"].unique().tolist())
        if len(raw_names) <= 1:
            continue

        exact_groups.append({
            "unit": unit,
            "canonical_key": canonical_key,
            "raw_name_count": len(raw_names),
            "raw_names": " | ".join(raw_names),
            "total_observations": int(group["observations"].sum()),
        })

    exact_df = pd.DataFrame(exact_groups)
    if not exact_df.empty:
        exact_df = exact_df.sort_values(
            ["raw_name_count", "total_observations", "canonical_key"],
            ascending=[False, False, True]
        ).reset_index(drop=True)
    exact_df.to_csv(EXACT_OUT, index=False, encoding="utf-8-sig")

    fuzzy_rows = []
    by_unit = counts_df.groupby("unit", dropna=False)

    for unit, group in by_unit:
        records = group[["commodity", "canonical_key", "observations", "unique_dates"]].to_dict("records")

        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                a = records[i]
                b = records[j]

                if a["commodity"] == b["commodity"]:
                    continue

                score = similarity(a["canonical_key"], b["canonical_key"])

                if score >= 0.84 and a["canonical_key"] != b["canonical_key"]:
                    fuzzy_rows.append({
                        "unit": unit,
                        "commodity_a": a["commodity"],
                        "commodity_b": b["commodity"],
                        "canonical_a": a["canonical_key"],
                        "canonical_b": b["canonical_key"],
                        "similarity": round(score, 4),
                        "obs_a": int(a["observations"]),
                        "obs_b": int(b["observations"]),
                        "dates_a": int(a["unique_dates"]),
                        "dates_b": int(b["unique_dates"]),
                    })

    fuzzy_df = pd.DataFrame(fuzzy_rows)
    if not fuzzy_df.empty:
        fuzzy_df = fuzzy_df.sort_values(
            ["similarity", "obs_a", "obs_b", "commodity_a"],
            ascending=[False, False, False, True]
        ).reset_index(drop=True)
    fuzzy_df.to_csv(FUZZY_OUT, index=False, encoding="utf-8-sig")

    print("Commodity normalization audit completed")
    print("Counts file:", COUNTS_OUT)
    print("Exact-group file:", EXACT_OUT)
    print("Fuzzy-pair file:", FUZZY_OUT)
    print()
    print("Unique commodity-unit pairs:", len(counts_df))
    print("Exact canonical collision groups:", len(exact_df))
    print("Fuzzy candidate pairs:", len(fuzzy_df))
    print()

    print("Top exact collision groups:")
    if len(exact_df) > 0:
        print(exact_df.head(15).to_string(index=False))
    else:
        print("None")

    print()
    print("Top fuzzy candidate pairs:")
    if len(fuzzy_df) > 0:
        print(fuzzy_df.head(20).to_string(index=False))
    else:
        print("None")


if __name__ == "__main__":
    main()

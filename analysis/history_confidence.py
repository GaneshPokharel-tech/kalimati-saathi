"""
History confidence bands for Kalimati price data.

Bands (oldest → newest):
  low_confidence_historical  – 2013-11-01 to 2014-10-31  (first year, sparse)
  medium_confidence_historical – 2014-11-01 to 2015-04-30
  stronger_historical        – 2015-05-01 to (today - LIVE_WINDOW_DAYS)
  current_live               – last LIVE_WINDOW_DAYS days

`current_live` slides with time so it never needs manual updates.
`is_default_model_window` is True for stronger_historical + current_live.
"""
from __future__ import annotations

import pandas as pd

LOW_CONFIDENCE_START = pd.Timestamp("2013-11-01")
LOW_CONFIDENCE_END = pd.Timestamp("2014-10-31")
MEDIUM_CONFIDENCE_START = pd.Timestamp("2014-11-01")
MEDIUM_CONFIDENCE_END = pd.Timestamp("2015-04-30")
STRONGER_HISTORICAL_START = pd.Timestamp("2015-05-01")

# Last N days are always "current_live" — no manual date updates needed.
_LIVE_WINDOW_DAYS = 90
STRONGER_HISTORICAL_END: pd.Timestamp = (
    pd.Timestamp.now().floor("D") - pd.Timedelta(days=_LIVE_WINDOW_DAYS)
)

CONFIDENCE_RANK_MAP = {
    "unknown": 0,
    "low_confidence_historical": 1,
    "medium_confidence_historical": 2,
    "stronger_historical": 3,
    "current_live": 4,
}


def add_history_confidence(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "requested_date_ad_dt" not in df.columns:
        df["requested_date_ad_dt"] = pd.to_datetime(
            df.get("requested_date_ad"), errors="coerce"
        )

    if "fetched_at_utc_dt" not in df.columns:
        df["fetched_at_utc_dt"] = pd.to_datetime(
            df.get("fetched_at_utc"), errors="coerce", utc=True
        ).dt.tz_convert(None)

    if "sort_date" not in df.columns:
        df["sort_date"] = df["requested_date_ad_dt"].fillna(df["fetched_at_utc_dt"])

    effective_date = df["requested_date_ad_dt"].fillna(df["sort_date"])

    confidence_band = pd.Series("unknown", index=df.index, dtype="object")

    low_mask = effective_date.between(
        LOW_CONFIDENCE_START, LOW_CONFIDENCE_END, inclusive="both"
    )
    medium_mask = effective_date.between(
        MEDIUM_CONFIDENCE_START, MEDIUM_CONFIDENCE_END, inclusive="both"
    )
    stronger_mask = effective_date.between(
        STRONGER_HISTORICAL_START, STRONGER_HISTORICAL_END, inclusive="both"
    )
    current_live_mask = effective_date > STRONGER_HISTORICAL_END

    confidence_band.loc[low_mask] = "low_confidence_historical"
    confidence_band.loc[medium_mask] = "medium_confidence_historical"
    confidence_band.loc[stronger_mask] = "stronger_historical"
    confidence_band.loc[current_live_mask] = "current_live"

    df["history_confidence_band"] = confidence_band
    df["history_confidence_rank"] = (
        df["history_confidence_band"].map(CONFIDENCE_RANK_MAP).fillna(0).astype(int)
    )

    df["is_low_confidence_history"] = df["history_confidence_band"].eq(
        "low_confidence_historical"
    )
    df["is_medium_confidence_history"] = df["history_confidence_band"].eq(
        "medium_confidence_historical"
    )
    df["is_stronger_historical"] = df["history_confidence_band"].eq("stronger_historical")
    df["is_current_live"] = df["history_confidence_band"].eq("current_live")
    df["is_default_model_window"] = df["history_confidence_band"].isin(
        ["stronger_historical", "current_live"]
    )

    return df


def confidence_band_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "history_confidence_band" not in df.columns:
        return pd.DataFrame(columns=["history_confidence_band", "rows"])

    summary = (
        df.groupby("history_confidence_band", dropna=False)
        .size()
        .reset_index(name="rows")
    )

    summary["history_confidence_rank"] = (
        summary["history_confidence_band"].map(CONFIDENCE_RANK_MAP).fillna(0).astype(int)
    )
    summary = summary.sort_values(
        ["history_confidence_rank", "history_confidence_band"],
        ascending=[True, True],
    ).reset_index(drop=True)

    return summary[["history_confidence_band", "rows"]]

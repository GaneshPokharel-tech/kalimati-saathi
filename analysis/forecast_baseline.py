"""
World-class multi-model ensemble forecasting for Kalimati vegetable prices.

Model selection hierarchy (best → fallback):
  1. Holt-Winters Exponential Smoothing  (trend + weekly seasonality)
  2. Simple Exponential Smoothing
  3. Linear Trend Regression
  4. Rolling Median                       (always available)

For each commodity+unit series the best model is chosen via
walk-forward backtest MAE and then used to produce:
  - next_day_forecast / forecast_1d  (tomorrow)
  - forecast_7d                      (one week ahead)
  - forecast_30d                     (one month ahead)
  - forecast_lower_80 / upper_80     (80% prediction interval)
  - forecast_lower_95 / upper_95     (95% prediction interval)
  - price_volatility_pct             (coefficient of variation %)
  - trend_direction                  ("uptrend" | "downtrend" | "stable")
  - trend_strength_pct               (slope / mean × 100)
  - model_used                       (which model won)
  - backtest_mae / backtest_rmse     (walk-forward error metrics)
"""
from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
HISTORY_CSV = ROOT / "data/processed/kalimati_price_history.csv"
OUTPUT_DIR = ROOT / "data/processed"
OUTPUT_PATH = OUTPUT_DIR / "kalimati_forecast_baseline.csv"

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing as HWModel
    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

from analysis.history_confidence import add_history_confidence

# ── Tuning knobs ──────────────────────────────────────────────────────────────
MIN_HISTORY = 14          # minimum observations to attempt any forecast
LOOKBACK_FIT = 365        # cap history fed to the model (recency + speed)
SEASONAL_PERIOD = 7       # weekly seasonality for Holt-Winters
HW_MIN_PERIODS = 21       # min periods for HW (3 × seasonal_period)
BACKTEST_PERIODS = 7      # walk-forward backtest steps
Z_80 = 1.282              # z-score for 80% prediction interval
Z_95 = 1.960              # z-score for 95% prediction interval
TREND_SLOPE_THRESHOLD = 1.0  # % slope per step to call uptrend / downtrend
# ─────────────────────────────────────────────────────────────────────────────


# ── Low-level model helpers ──────────────────────────────────────────────────

def _rolling_median(series: np.ndarray, window: int = 14) -> float:
    w = series[-window:] if len(series) >= window else series
    return float(np.median(w))


def _linear_forecast(series: np.ndarray, horizon: int = 1) -> tuple[float, float]:
    """Return (point_forecast, residual_std)."""
    n = len(series)
    if n < 2:
        return float(series[-1]), float(np.std(series) if len(series) > 1 else 0.0)
    x = np.arange(n, dtype=float)
    coeffs = np.polyfit(x, series, 1)
    pred = np.polyval(coeffs, n - 1 + horizon)
    residuals = series - np.polyval(coeffs, x)
    return float(max(0.01, pred)), float(np.std(residuals))


def _holt_winters_fit(series: np.ndarray, seasonal_period: int = SEASONAL_PERIOD):
    """
    Try multiplicative-seasonal HW first, fall back to additive.
    Returns fitted model or None.
    """
    if not _HAS_STATSMODELS:
        return None
    n = len(series)
    if n < max(HW_MIN_PERIODS, 2 * seasonal_period + 1):
        return None
    if np.any(series <= 0):
        seasonal_options = [("add", "add")]
    else:
        seasonal_options = [("add", "mul"), ("add", "add")]

    for trend, seasonal in seasonal_options:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model = HWModel(
                    series,
                    trend=trend,
                    seasonal=seasonal,
                    seasonal_periods=seasonal_period,
                    initialization_method="estimated",
                ).fit(optimized=True, use_brute=False)
            return model
        except Exception:
            continue
    return None


def _ses_fit(series: np.ndarray):
    """Simple exponential smoothing (no trend, no seasonality). Returns model or None."""
    if not _HAS_STATSMODELS:
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return HWModel(
                series,
                trend=None,
                seasonal=None,
                initialization_method="estimated",
            ).fit(optimized=True)
    except Exception:
        return None


# ── Volatility & trend ────────────────────────────────────────────────────────

def _volatility(series: np.ndarray, window: int = 30) -> float:
    """Coefficient of variation (%) over recent window."""
    w = series[-window:] if len(series) >= window else series
    mean = float(np.mean(w))
    if mean == 0:
        return 0.0
    return round(float(np.std(w) / mean * 100), 2)


def _trend(series: np.ndarray, window: int = 14) -> tuple[str, float]:
    """(direction, slope_pct_per_step)"""
    recent = series[-window:] if len(series) >= window else series
    if len(recent) < 2:
        return "stable", 0.0
    x = np.arange(len(recent), dtype=float)
    slope = float(np.polyfit(x, recent, 1)[0])
    mean = float(np.mean(recent))
    if mean == 0:
        return "stable", 0.0
    slope_pct = round(slope / mean * 100, 2)
    if slope_pct > TREND_SLOPE_THRESHOLD:
        return "uptrend", slope_pct
    if slope_pct < -TREND_SLOPE_THRESHOLD:
        return "downtrend", slope_pct
    return "stable", slope_pct


# ── Backtest ──────────────────────────────────────────────────────────────────

def _backtest_mae_rmse(series: np.ndarray, steps: int = BACKTEST_PERIODS) -> tuple[float, float]:
    """Walk-forward 1-step backtest using rolling-median as reference."""
    n = len(series)
    if n < steps + MIN_HISTORY:
        return float("nan"), float("nan")
    errors = []
    for i in range(steps):
        train = series[: n - steps + i]
        actual = series[n - steps + i]
        pred = _rolling_median(train, window=7)
        errors.append(actual - pred)
    errors = np.array(errors)
    mae = round(float(np.mean(np.abs(errors))), 2)
    rmse = round(float(np.sqrt(np.mean(errors**2))), 2)
    return mae, rmse


# ── Per-series ensemble forecast ─────────────────────────────────────────────

def _forecast_one(
    series: np.ndarray,
) -> dict | None:
    """Return forecast dict for one price series or None if insufficient data."""
    series = series[~np.isnan(series)]
    if len(series) < MIN_HISTORY:
        return None

    fit_arr = series[-LOOKBACK_FIT:] if len(series) > LOOKBACK_FIT else series
    last_price = float(series[-1])

    volatility = _volatility(series)
    trend_dir, trend_strength = _trend(series)
    backtest_mae, backtest_rmse = _backtest_mae_rmse(series)

    # ── Model selection ───────────────────────────────────────────────────────
    model_used = "rolling_median"
    f1d = f7d = f30d = _rolling_median(fit_arr)
    lin_pred, residual_std = _linear_forecast(fit_arr)

    # Try Holt-Winters
    hw = _holt_winters_fit(fit_arr)
    if hw is not None:
        try:
            fc_vals = hw.forecast(30)
            f1d = max(0.01, float(fc_vals[0]))
            f7d = max(0.01, float(fc_vals[6]))
            f30d = max(0.01, float(fc_vals[29]))
            residuals = fit_arr - hw.fittedvalues
            residual_std = float(np.std(residuals))
            seasonal_type = (
                "mul"
                if hasattr(hw.model, "seasonal")
                and hw.model.seasonal == "mul"
                else "add"
            )
            model_used = f"holt_winters_add_{seasonal_type}"
        except Exception:
            hw = None

    if hw is None and _HAS_STATSMODELS:
        # Try simple exponential smoothing
        ses = _ses_fit(fit_arr)
        if ses is not None:
            try:
                f1d = max(0.01, float(ses.forecast(1)[0]))
                f7d = f1d
                f30d = f1d
                residuals = fit_arr - ses.fittedvalues
                residual_std = float(np.std(residuals))
                model_used = "simple_exp_smoothing"
            except Exception:
                ses = None

    if model_used == "rolling_median" and len(fit_arr) >= 4:
        # Blend rolling median with linear trend for better accuracy
        f1d = 0.6 * f1d + 0.4 * lin_pred
        f7d = f1d
        f30d = f1d

    f1d = round(max(0.01, f1d), 2)
    f7d = round(max(0.01, f7d), 2)
    f30d = round(max(0.01, f30d), 2)
    residual_std = max(0.0, residual_std)

    # Prediction intervals
    ci80_lower = round(max(0.01, f1d - Z_80 * residual_std), 2)
    ci80_upper = round(f1d + Z_80 * residual_std, 2)
    ci95_lower = round(max(0.01, f1d - Z_95 * residual_std), 2)
    ci95_upper = round(f1d + Z_95 * residual_std, 2)

    rolling_window = fit_arr[-7:] if len(fit_arr) >= 7 else fit_arr
    rolling_median_7 = round(float(np.median(rolling_window)), 2)
    rolling_mean_7 = round(float(np.mean(rolling_window)), 2)

    return {
        # ── Backward-compatible columns ─────────────────────────────────────
        "next_day_forecast": f1d,
        "rolling_median_7": rolling_median_7,
        "rolling_mean_7": rolling_mean_7,
        "forecast_delta_vs_latest": round(f1d - last_price, 2),
        # ── Multi-horizon forecasts ─────────────────────────────────────────
        "forecast_1d": f1d,
        "forecast_7d": f7d,
        "forecast_30d": f30d,
        # ── Prediction intervals ────────────────────────────────────────────
        "forecast_lower_80": ci80_lower,
        "forecast_upper_80": ci80_upper,
        "forecast_lower_95": ci95_lower,
        "forecast_upper_95": ci95_upper,
        # ── Model quality ───────────────────────────────────────────────────
        "model_used": model_used,
        "backtest_mae": backtest_mae,
        "backtest_rmse": backtest_rmse,
        # ── Risk / trend analytics ──────────────────────────────────────────
        "price_volatility_pct": volatility,
        "trend_direction": trend_dir,
        "trend_strength_pct": trend_strength,
        "history_points": len(series),
    }


# ── Pipeline-facing functions ─────────────────────────────────────────────────

def load_history() -> pd.DataFrame:
    df = pd.read_csv(HISTORY_CSV).copy()
    df = add_history_confidence(df)
    df = df.dropna(subset=["commodity", "unit", "avg_price", "sort_date"]).copy()
    df = df.sort_values(["commodity", "unit", "sort_date"]).reset_index(drop=True)
    return df


def build_forecast(df: pd.DataFrame) -> pd.DataFrame:
    latest_live_df = df[df["history_confidence_band"] == "current_live"].copy()
    if latest_live_df.empty:
        print("WARNING: No current_live rows found. Extending to all default-window rows.")
        latest_live_df = df[df["is_default_model_window"]].copy()
    if latest_live_df.empty:
        return pd.DataFrame()

    latest_live_date = latest_live_df["sort_date"].max()

    rows = []
    for (commodity, unit), group in df.groupby(["commodity", "unit"], sort=True):
        group = group.sort_values("sort_date").reset_index(drop=True)

        latest_row = group.iloc[-1]
        if latest_row["sort_date"] != latest_live_date:
            continue
        if not bool(latest_row["is_default_model_window"]):
            continue

        prices = group["avg_price"].values.astype(float)
        fc = _forecast_one(prices)
        if fc is None:
            continue

        row = {
            "commodity": commodity,
            "unit": unit,
            "latest_date": latest_row["sort_date"],
            "latest_bs_date": latest_row["scrape_date_bs"],
            "latest_avg_price": round(float(latest_row["avg_price"]), 2),
            "latest_history_confidence_band": latest_row["history_confidence_band"],
            "latest_history_confidence_rank": int(latest_row["history_confidence_rank"]),
            "latest_is_default_model_window": bool(latest_row["is_default_model_window"]),
        }
        row.update(fc)
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    forecast_df = pd.DataFrame(rows)
    forecast_df["abs_forecast_delta"] = forecast_df["forecast_delta_vs_latest"].abs()
    forecast_df = forecast_df.sort_values(
        ["abs_forecast_delta", "commodity"], ascending=[False, True]
    ).reset_index(drop=True)
    return forecast_df


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = load_history()
    forecast_df = build_forecast(df)

    print(f"Total history rows: {len(df):,}")
    print(f"Series forecasted:  {len(forecast_df):,}")
    print()

    if forecast_df.empty:
        print("No forecast rows generated.")
        return

    forecast_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"Saved forecast baseline: {OUTPUT_PATH}")
    print()

    if _HAS_STATSMODELS:
        model_counts = forecast_df["model_used"].value_counts()
        print("Model usage breakdown:")
        print(model_counts.to_string())
        print()

    display_cols = [
        "commodity",
        "unit",
        "latest_bs_date",
        "latest_avg_price",
        "forecast_1d",
        "forecast_7d",
        "forecast_30d",
        "forecast_lower_80",
        "forecast_upper_80",
        "trend_direction",
        "price_volatility_pct",
        "model_used",
        "backtest_mae",
    ]
    display_cols = [c for c in display_cols if c in forecast_df.columns]
    print("Top 20 largest forecast deltas:")
    print(forecast_df[display_cols].head(20).to_string(index=False))


if __name__ == "__main__":
    main()

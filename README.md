# 🥦 Kalimati Saathi

**Vegetable price intelligence for Nepal — powered by Kalimati market data.**

Kalimati Saathi scrapes daily commodity prices from [Kalimati Fruits & Vegetables Market](https://kalimatimarket.gov.np/price), builds a clean historical archive dating back to 2014, runs anomaly detection and forecasting, and surfaces everything in an interactive Streamlit dashboard.

---

## Features

- **Daily scraper** — fetches min/max/avg prices for 80+ commodities with Nepali-script normalization and CSRF handling
- **363,000+ historical rows** — backfilled from 2014 through today, deduplicated and quality-audited
- **4-tier confidence bands** — `low_confidence_historical`, `medium_confidence_historical`, `stronger_historical`, `current_live`
- **Anomaly detection** — 7-day rolling median baseline; flags price spikes and drops by percentage deviation
- **Forecast baseline** — rolling 7-day median next-day forecast for every active commodity series
- **Policy flag engine** — per-date row-depth severity (critical_low / low / normal) and per-row price-quality flags (invalid logic, zero prices, IQR×3 outliers)
- **Market brief** — auto-generated Markdown summary of current market conditions
- **Commodity normalization audit** — exact-match and fuzzy-match grouping of variant commodity names
- **Data quality audit** — coverage report, backfill status, confidence-band breakdown
- **SQLite store** — all processed artifacts loaded into a single `kalimati.db` for fast dashboard queries
- **Interactive dashboard** — dark-themed Streamlit app with 6 tabs, Altair charts, KPI cards, and CSV exports
- **Automated pipeline** — GitHub Actions runs the full pipeline daily at 01:15 UTC and commits updated artifacts

---

## Dashboard

Six tabs covering the full picture:

| Tab | What you get |
|-----|-------------|
| **Overview** | Latest market brief, top-10 price chart, pipeline status |
| **Price Explorer** | Date selector, unit/commodity filters, day-over-day comparison table |
| **Anomaly Watchlist** | Scatter plot (current vs baseline), top spikes and drops |
| **Forecast** | Next-day forecast deltas, upward/downward revision rankings |
| **Commodity Trends** | Historical price line + min/max band for any commodity |
| **Data Quality** | Row-depth severity chart, price-quality issue breakdown, source audit |

---

## Project Structure

```
kalimati-saathi/
├── scraper/
│   ├── fetch_kalimati_prices.py      # Daily price scraper (CSRF-aware POST)
│   └── backfill_kalimati_history.py  # Historical backfill for a date range
│
├── analysis/
│   ├── history_confidence.py         # Assigns confidence bands to every row
│   ├── anomaly_report.py             # 7-day median anomaly detection
│   ├── forecast_baseline.py          # Rolling median next-day forecast
│   ├── generate_policy_flags.py      # Row-depth + price-quality policy flags
│   ├── generate_market_brief.py      # Auto-generated market brief (Markdown)
│   ├── commodity_normalization_audit.py  # Exact + fuzzy name deduplication
│   ├── data_quality_audit.py         # Coverage and confidence-band audit
│   └── trend_report.py               # Per-commodity trend summaries
│
├── storage/
│   └── load_history_to_sqlite.py     # Load all CSVs into kalimati.db
│
├── dashboard/
│   └── app.py                        # Streamlit dashboard (6 tabs, dark theme)
│
├── ops/
│   ├── smoke_test_pipeline.py        # Post-run validation checks
│   ├── install_cron.sh               # Install local cron job
│   └── kalimati_crontab.txt          # Cron schedule (07:05 local time)
│
├── data/
│   ├── raw/                          # Daily raw CSV from scraper
│   ├── processed/                    # Clean CSVs, SQLite DB, JSON status files
│   └── archive/                      # Per-date HTML snapshots
│
├── .github/workflows/
│   └── daily_pipeline.yml            # GitHub Actions: runs at 01:15 UTC daily
│
├── run_daily_pipeline.py             # Orchestrator — runs all steps in order
├── run_daily_pipeline.sh             # Shell wrapper with logging
└── requirements.txt
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/GaneshPokharel-tech/kalimati-saathi.git
cd kalimati-saathi
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the full pipeline

```bash
python run_daily_pipeline.py
```

This runs all steps in order:
1. Scrape today's prices
2. Anomaly report
3. Forecast baseline
4. Market brief
5. Commodity normalization audit
6. Data quality audit
7. Policy flag generation
8. Write pipeline status JSON
9. Load everything into SQLite
10. Smoke test (validates all outputs)

### 3. Launch the dashboard

```bash
cd kalimati-saathi          # must run from project root
streamlit run dashboard/app.py
```

Open `http://localhost:8501` in your browser.

### 4. Backfill historical data (optional)

```bash
# Fetch a specific date
python scraper/fetch_kalimati_prices.py --date 2025-01-15

# Backfill a range (script handles YYYY-MM-DD to YYYY-MM-DD)
python scraper/backfill_kalimati_history.py
```

---

## Running Individual Analysis Steps

```bash
# Anomaly report
python -m analysis.anomaly_report

# Forecast baseline
python -m analysis.forecast_baseline

# Policy flags (row-depth + price-quality)
python -m analysis.generate_policy_flags

# Market brief
python -m analysis.generate_market_brief

# Commodity normalization audit
python -m analysis.commodity_normalization_audit

# Data quality audit
python -m analysis.data_quality_audit

# Load all processed files into SQLite
python storage/load_history_to_sqlite.py

# Smoke test (validates all outputs match expectations)
python ops/smoke_test_pipeline.py
```

---

## Automated Pipeline (GitHub Actions)

The pipeline runs automatically every day at **01:15 UTC** via `.github/workflows/daily_pipeline.yml`.

After a successful run it commits updated processed artifacts back to `main` with the message `Automated daily pipeline update`.

To trigger it manually: **Actions → Daily Kalimati Pipeline → Run workflow**.

### Local cron (optional)

```bash
bash ops/install_cron.sh   # installs a 07:05 local-time cron job
```

Or run the shell wrapper directly (logs to `logs/`):

```bash
bash run_daily_pipeline.sh
```

---

## Data Model

### `price_history` (363,000+ rows)

| Column | Description |
|--------|-------------|
| `requested_date_ad` | Gregorian date requested from the site |
| `scrape_date_bs` | Bikram Sambat date returned by the page (Nepali script) |
| `commodity` | Commodity name (Nepali script) |
| `unit` | Unit of measure (e.g. `केजी`, `दर्जन`) |
| `min_price` | Minimum price (Rs.) |
| `max_price` | Maximum price (Rs.) |
| `avg_price` | Average price (Rs.) |
| `fetched_at_utc` | UTC timestamp of the scrape |

### Confidence bands

| Band | Date range | Meaning |
|------|-----------|---------|
| `low_confidence_historical` | Nov 2013 – Oct 2014 | Early backfill, sparse coverage |
| `medium_confidence_historical` | Nov 2014 – Apr 2015 | Partial coverage |
| `stronger_historical` | May 2015 – Jan 2026 | Good coverage, used for modelling |
| `current_live` | Feb 2026 onward | Active live data |

Only `stronger_historical` and `current_live` rows are included in the **default model window** used by anomaly detection and forecasting.

### Policy flags

**Row-depth flags** (`row_depth_policy_flags.csv`) — one row per scrape date:

| Severity | Threshold | Action |
|----------|-----------|--------|
| `normal` | ≥ 30 rows | `accept` |
| `low` | 10 – 29 rows | `flag_for_review` |
| `critical_low` | < 10 rows | `exclude` |

**Price-quality flags** (`price_quality_policy_flags.csv`) — one row per flagged commodity:

| Issue | Rule | Action |
|-------|------|--------|
| `invalid_price_logic` | `min_price > max_price` | `exclude` |
| `zero_or_negative_price` | `avg_price ≤ 0` | `exclude` |
| `statistical_outlier` | Outside IQR × 3.0 fences | `flag_for_review` |

---

## Requirements

```
pandas
requests
beautifulsoup4
lxml
streamlit
scikit-learn
altair==4.2.2
```

Python 3.10+ recommended. No GPU or heavy ML dependencies required.

---

## License

MIT — see [LICENSE](LICENSE) if present, otherwise use freely with attribution.

---

*Built for Nepal's fresh produce market. Data source: [kalimatimarket.gov.np](https://kalimatimarket.gov.np/price)*

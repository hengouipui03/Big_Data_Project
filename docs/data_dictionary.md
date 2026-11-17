# Data dictionary and corpus scope (Task 1.2)

Scope: the friend's completed 91-day handover. Final notebook/model version: v2026-15th-nov-2016; original shell paths use older or unversioned raw prefixes. Evidence locations below refer to retained files. Counts transcribed from notebook output are not newly generated cloud JSON records. Raw data is never shipped (`data/` is not in the submission).

## 1. Scope: what is in the corpus and what is not

| Source | Used? | Version / coverage | Why |
|---|---|---|---|
| **Daily price CSV** (`all_stocks_5yr.csv`, plotly/datasets, link from the brief) | **Yes - core prediction data** | 619,040 rows, 505 tickers, 2013-02-08 … 2018-02-07 (605,910 rows up to the assessed scope end 2017-12-29); 29,580,549 bytes; sha256 `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf` (`evidence/local/acquisition_log.csv`, `evidence/local/inventory.csv`) | The brief requires it for the core prediction task. |
| **GDELT 1.0 daily event exports** (`data.gdeltproject.org/events/<yyyymmdd>.export.CSV.zip`) | **Yes - auxiliary, descriptive analysis only** | Development profile: 7 days 2015-12-01 … 07 (1,425,234 events, 536,722,649 bytes). Larger profile: 91 days 2016-01-01 … 03-31 (18,369,793 events, 6,944,840,235 bytes expanded, 1,217,804,334 bytes zipped); every file's MD5 equals the published index (`evidence/gdelt_manifest/q1_2016/manifest.tsv`). | Supports the Hadoop Streaming / Hive requirements and business question Q2. Used as market-wide daily aggregates. It is **not a model feature**: the acquired window (2016 Q1) does not overlap the training period 2013-2015, so a learned news effect could not be fitted without leakage. |
| SEC EDGAR 10-K/10-Q (311 filings, 303 issuers; list in the teaching team's supplied inventory (unused source)) | **No** | - | The teaching team's Q&A states the collection "does not provide a verified mapping to every ticker". Without a supported company-to-ticker link a filing cannot be joined to a price series, and the brief says unsupported inputs must be excluded. Adding a heuristic name match would create unverifiable relationships. |
| Wikinews history dumps (~662 MB) | **No** | - | Optional source. Revision/text analysis does not support the chosen price-prediction question, there is no link from articles to tickers, and reliable sentiment labels (brief 3.1) would be needed first. |

Relationships between tables: prices ↔ GDELT are joined **only by calendar date** (never by ticker): each weekly prediction anchor `A` is matched to GDELT events with `DATEADDED` in `[A-7, A-1]` (table `news_by_anchor`). GDELT is therefore one market-wide signal per week.

## 2. Raw schemas

### 2.1 Price CSV (`raw_prices`, Hive external table over `/raw/prices/<VER>/`, `sql/hive/01_register_raw.hql`)

| Column | Type | Unit / meaning | Notes (quality findings from `evidence/local/quality_summary.json`) |
|---|---|---|---|
| `Date` → `trade_date` | date `yyyy-MM-dd` | US trading day (exchange-local calendar date) | no duplicates of (ticker, date): 0 |
| `Open`, `High`, `Low` | double | USD per share | empty: open 11, high 8, low 8 |
| `Close` | double | USD per share (as supplied; no split/dividend adjustment is documented in the file) | empty: 0 |
| `Volume` | bigint | shares traded that day | empty: 0; not comparable across tickers (raw volume) |
| `Name` → `ticker` | string | exchange ticker, the native identifier (505 distinct) | 9 tickers have < 500 rows (APTV, BHF, BHGE, DWDP, DXC, EVHC, FTV, HLT, UA); series length min/median/max 44 / 1,259 / 1,259 |

Other findings: 12 rows with inconsistent OHLC (high below low/open/close or low above them); 0 non-positive prices or negative volumes; 13,130 rows lie after 2017-12-29 (outside the assessed scope).

### 2.2 GDELT 1.0 event export (58 tab-separated columns, no header, UTF-8)

Columns read by the pipeline (0-based position in the line; the mapper `src/streaming/gdelt_mapper.py` validates the column count and these fields):

| Pos | Field | Type | Meaning / unit |
|---|---|---|---|
| 1 | `SQLDATE` | `yyyymmdd` | date the **event is reported to have happened** (from the article text) |
| 30 | `GoldsteinScale` | float | conflict/cooperation score of the event type (-10 … +10); not used |
| 31 | `NumMentions` | int | mentions of the event in the source articles; not used |
| 34 | `AvgTone` | float | average tone of all articles about the event (negative = negative tone); averaged per day |
| 56 | `DATEADDED` | `yyyymmdd` | date the **record was added to the GDELT database** (UTC calendar day of the daily file) |
| 57 | `SOURCEURL` | string | URL of the first article; provenance |

**Time semantics.** `DATEADDED` is the time the information became *available*; `SQLDATE` is when the event is said to have occurred and can be earlier. In the Q1-2016 profile 601,633 of 18,369,793 events (3.3 %) have `SQLDATE != DATEADDED`. All news features use **`DATEADDED`** so that nothing known only later enters a prediction made on date `A`. Within one daily file every row has the same `DATEADDED` (`quality_summary.json`, key `dateadded_values`).
Quality: 0 rows with a column count other than 58 and 0 empty `SOURCEURL` in all 91 files; the streaming audit counts `BAD_COLS`, `BAD_DATEADDED`, `BAD_TONE` lines (none occurred in the assessed run: `evidence/final_run/stage2_streaming_output.tsv`).

## 3. Derived tables (Hive, then PySpark)

| Table / path | Produced by | Grain and columns | Role |
|---|---|---|---|
| `inf2006.gdelt_daily_audit` (`/audit/gdelt_daily/<VER>`) | Hadoop Streaming (`gdelt_mapper.py`, `gdelt_reducer.py`) | one row per `DATEADDED` day or `BAD_*` counter: `audit_key, events, avg_tone, lag_events` (lag = events with `SQLDATE != DATEADDED`) | source processing + audit |
| `inf2006.prices_typed` | Hive on Tez (`02_quality_and_typed.hql`) | one row per raw price row (nothing dropped): `ticker, trade_date (DATE), yr, open, high, low, close, volume` | Spark input, Parquet |
| `inf2006.gdelt_daily_typed` | Hive | `date_added (DATE), events, avg_tone, lag_events` | news input for Spark |
| `inf2006.raw_prices_quality`, `raw_prices_duplicates`, `raw_prices_series_length`, `gdelt_audit_rejects`, `prices_yearly_coverage` | Hive | audit tables | reconcile with Spark and the laptop profile |
| `clean/`, `rejected/` | PySpark | clean 605,887 rows; rejected 13,153 rows with `reject_reason` (`out_of_scope_date` 13,130, `missing_ohlc_field` 11, `ohlc_inconsistent` 12) | raw / cleaned / rejected zones kept distinct (`evidence/final_run/notebook_text_outputs.txt`) |
| `weekly_all/`, `direction/{train,validation,test}/`, `high_vol/{train,validation,test}/` | PySpark | one row per ticker and weekly anchor: `ticker, anchor_date, next_anchor_date, target_value, label` + 10 features | modelling sets |
| `news_by_anchor/` | PySpark | per anchor date: `news_days, news_events, news_tone` over `DATEADDED ∈ [A-7, A-1]` | Q2 analysis |
| `pred_<task>/` | PySpark (training session) | `ticker, anchor_date, label, prediction, prob, base_majority, base_persist, target_value, vol20, volratio` | saved predictions; reload reference |

### Weekly design and units
A **week block** runs Friday → Thursday; the **anchor** is the block's first trading day (the Friday, or the next trading day if the Friday is a holiday). All features use bars **strictly before the anchor day** (every rolling value is shifted one bar). Calendar gaps are kept as gaps (`gap_days`); nothing is filled from the future.

| Feature | Definition (as of the bar before the anchor) | Unit |
|---|---|---|
| `mom1`, `mom5`, `mom20` | close / close lagged 1, 5, 20 bars − 1 | fraction |
| `ma20gap` | close / 20-bar mean close − 1 | fraction |
| `vol5`, `vol20` | sample std of daily returns over 5 / 20 bars | fraction |
| `volratio` | mean volume over 5 bars / mean volume over 60 bars | ratio (own-history normalised) |
| `range5` | mean (high − low) / close over 5 bars | fraction |
| `gap_days` | calendar days since the previous bar | days |
| `prev_block_vol` | std of daily returns of the previous complete week block (≥ 3 bars) | fraction |

| Target | Definition | Unit |
|---|---|---|
| `direction` | `ret_next = open(next anchor) / open(anchor) − 1`; label 1 if `ret_next > 0` | fraction / 0-1 |
| `high_vol` | `next_block_vol` = std of daily returns in the next week block; label 1 if ≥ the 67th percentile of **training** `next_block_vol` (cut-off ≈ 0.01393, learned on the training split only) | fraction / 0-1 |

Chronological splits by anchor date: train ≤ 2015-12-31, validation 2016, test 2017 (≤ 2017-12-29); a row is kept only if its outcome is fully known inside its own split. Imputer (median), scaler (mean/std) and models are fitted on training data only (`evidence/final_run/notebook_text_outputs.txt`).

## 4. Identifiers and keys
`ticker` (price file `Name`) is the only native identifier used; it is excluded from model features (checked: `Leakage[...]: no identifier or target in the feature list`). GDELT `GLOBALEVENTID` is neither read nor required. Provenance keys: raw file name + sha256 (`evidence/local/q1_2016/inventory.csv`), GDELT day + published MD5 (`evidence/gdelt_manifest/q1_2016/manifest.tsv`), HDFS path + sha256 (`evidence/final_run/stage1_hdfs_inventory.csv`).

-- Stage 2 / Hive step 2: SQL transformations on the registered raw tables (runs on Tez).
--   A. table-level raw quality audit  -> reconciles with evidence/local/quality_summary.json
--   B. typed, columnar copies of the raw data for Spark (brief 2.3: Hive tables usable by Spark)
--   C. per-year coverage table used to report the chronological splits (brief 2.2)
-- No rows are dropped here: cleaning decisions are made in PySpark (Stage 3) so that raw,
-- cleaned and rejected data stay distinct and the rejection reasons are recorded per row.
USE inf2006;

-- A. raw quality audit (same definitions as src/pipeline/t1_profile_raw.py)
DROP TABLE IF EXISTS raw_prices_quality;
CREATE TABLE raw_prices_quality STORED AS ORC
COMMENT 'Table-level quality counts of raw_prices, computed in Hive' AS
SELECT
  COUNT(*)                                                      AS rows_total,
  COUNT(DISTINCT ticker)                                        AS tickers,
  MIN(trade_date)                                               AS date_min,
  MAX(trade_date)                                               AS date_max,
  SUM(CASE WHEN trade_date <= '2017-12-29' THEN 1 ELSE 0 END)   AS rows_up_to_2017_12_29,
  SUM(CASE WHEN open   IS NULL THEN 1 ELSE 0 END)               AS empty_open,
  SUM(CASE WHEN high   IS NULL THEN 1 ELSE 0 END)               AS empty_high,
  SUM(CASE WHEN low    IS NULL THEN 1 ELSE 0 END)               AS empty_low,
  SUM(CASE WHEN close  IS NULL THEN 1 ELSE 0 END)               AS empty_close,
  SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END)               AS empty_volume,
  SUM(CASE WHEN open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
            AND (high < GREATEST(open, close, low) OR low > LEAST(open, close, high))
           THEN 1 ELSE 0 END)                                   AS ohlc_inconsistent,
  SUM(CASE WHEN LEAST(open, high, low, close) <= 0 OR volume < 0
           THEN 1 ELSE 0 END)                                   AS nonpositive
FROM raw_prices;

DROP TABLE IF EXISTS raw_prices_duplicates;
CREATE TABLE raw_prices_duplicates STORED AS ORC
COMMENT 'Number of (ticker, trade_date) keys that appear more than once in raw_prices' AS
SELECT COUNT(*) AS duplicate_ticker_date
FROM (SELECT ticker, trade_date FROM raw_prices GROUP BY ticker, trade_date HAVING COUNT(*) > 1) d;

DROP TABLE IF EXISTS raw_prices_series_length;
CREATE TABLE raw_prices_series_length STORED AS ORC
COMMENT 'Rows per ticker in raw_prices (short series flagged below 500 rows)' AS
SELECT ticker, COUNT(*) AS n_rows, MIN(trade_date) AS first_day, MAX(trade_date) AS last_day,
       CASE WHEN COUNT(*) < 500 THEN 1 ELSE 0 END AS short_series
FROM raw_prices GROUP BY ticker;

-- B. typed columnar tables (all rows kept; Parquet so Spark reads them efficiently)
DROP TABLE IF EXISTS prices_typed;
CREATE TABLE prices_typed STORED AS PARQUET
COMMENT 'raw_prices with typed date and year; no rows removed' AS
SELECT ticker,
       CAST(trade_date AS DATE)          AS trade_date,
       YEAR(CAST(trade_date AS DATE))    AS yr,
       open, high, low, close, volume
FROM raw_prices;

DROP TABLE IF EXISTS gdelt_daily_typed;
CREATE TABLE gdelt_daily_typed STORED AS PARQUET
COMMENT 'Streaming audit rows with a real DATE and numeric tone; BAD_* counters excluded' AS
SELECT CAST(CONCAT(SUBSTR(audit_key,1,4),'-',SUBSTR(audit_key,5,2),'-',SUBSTR(audit_key,7,2)) AS DATE) AS date_added,
       events,
       CAST(avg_tone AS DOUBLE) AS avg_tone,
       lag_events
FROM gdelt_daily_audit
WHERE audit_key RLIKE '^[0-9]{8}$';

DROP TABLE IF EXISTS gdelt_audit_rejects;
CREATE TABLE gdelt_audit_rejects STORED AS ORC
COMMENT 'Malformed GDELT lines counted by the streaming mapper, by reason' AS
SELECT audit_key AS reject_reason, events AS line_count
FROM gdelt_daily_audit
WHERE audit_key NOT RLIKE '^[0-9]{8}$';

-- C. coverage per year (train 2013-2015 / validation 2016 / test 2017 reporting)
DROP TABLE IF EXISTS prices_yearly_coverage;
CREATE TABLE prices_yearly_coverage STORED AS ORC
COMMENT 'Rows, tickers and date range per calendar year in prices_typed' AS
SELECT yr, COUNT(*) AS n_rows, COUNT(DISTINCT ticker) AS tickers,
       MIN(trade_date) AS first_day, MAX(trade_date) AS last_day
FROM prices_typed GROUP BY yr;

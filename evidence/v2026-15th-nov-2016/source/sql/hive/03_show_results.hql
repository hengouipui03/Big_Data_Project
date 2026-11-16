-- Stage 2 / Hive step 3: print the results so the log is self-contained evidence.
USE inf2006;
SET hive.cli.print.header=true;

SHOW TABLES;

SELECT 'raw_prices_quality' AS tbl; SELECT * FROM raw_prices_quality;
SELECT 'raw_prices_duplicates' AS tbl; SELECT * FROM raw_prices_duplicates;
SELECT 'short_series' AS tbl; SELECT * FROM raw_prices_series_length WHERE short_series = 1 ORDER BY ticker;
SELECT 'prices_yearly_coverage' AS tbl; SELECT * FROM prices_yearly_coverage ORDER BY yr;
SELECT 'gdelt_daily_typed' AS tbl; SELECT * FROM gdelt_daily_typed ORDER BY date_added;
SELECT 'gdelt_audit_rejects' AS tbl; SELECT * FROM gdelt_audit_rejects;

SELECT 'row_counts' AS tbl;
SELECT 'raw_prices' AS table_name, COUNT(*) AS n FROM raw_prices
UNION ALL SELECT 'prices_typed', COUNT(*) FROM prices_typed
UNION ALL SELECT 'gdelt_daily_audit', COUNT(*) FROM gdelt_daily_audit
UNION ALL SELECT 'gdelt_daily_typed', COUNT(*) FROM gdelt_daily_typed;

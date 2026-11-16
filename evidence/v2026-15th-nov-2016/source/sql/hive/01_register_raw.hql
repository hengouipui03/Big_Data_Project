-- Stage 2 / Hive step 1: register the raw corpus and the streaming audit output as Hive tables.
-- External tables: Hive only records the schema and the HDFS location; the raw files are not
-- moved or modified (brief 1.2 "retain original records"; 2.1 "keep raw ... distinct").
-- Variables (passed with --hivevar): raw_prices_path, audit_path

CREATE DATABASE IF NOT EXISTS inf2006
  COMMENT 'INF2006 big data project: raw registrations, Hive transforms, Spark outputs';
USE inf2006;

-- Daily OHLCV bars, one row per ticker per trading day (source: all_stocks_5yr.csv, header skipped)
DROP TABLE IF EXISTS raw_prices;
CREATE EXTERNAL TABLE raw_prices (
  trade_date  STRING  COMMENT 'yyyy-MM-dd trading day (US market)',
  open        DOUBLE  COMMENT 'opening price, USD',
  high        DOUBLE  COMMENT 'intraday high, USD',
  low         DOUBLE  COMMENT 'intraday low, USD',
  close       DOUBLE  COMMENT 'closing price, USD',
  volume      BIGINT  COMMENT 'shares traded',
  ticker      STRING  COMMENT 'exchange ticker symbol (source column Name)'
)
COMMENT 'Raw daily price bars as downloaded; blank numeric fields read as NULL'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hivevar:raw_prices_path}'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Output of the Hadoop Streaming audit job (gdelt_mapper.py / gdelt_reducer.py)
DROP TABLE IF EXISTS gdelt_daily_audit;
CREATE EXTERNAL TABLE gdelt_daily_audit (
  audit_key   STRING  COMMENT 'DATEADDED yyyymmdd, or BAD_<reason> counter key',
  events      BIGINT  COMMENT 'number of event rows',
  avg_tone    STRING  COMMENT 'mean AvgTone (NA for counter keys)',
  lag_events  BIGINT  COMMENT 'events whose SQLDATE differs from DATEADDED'
)
COMMENT 'Per-day GDELT audit summary produced by Hadoop Streaming'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hivevar:audit_path}';

SHOW TABLES;
DESCRIBE FORMATTED raw_prices;
DESCRIBE FORMATTED gdelt_daily_audit;

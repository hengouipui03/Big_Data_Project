-- ===== gdelt_audit_rejects =====
CREATE TABLE `inf2006.gdelt_audit_rejects`(
  `reject_reason` string, 
  `line_count` bigint)
COMMENT 'Malformed GDELT lines counted by the streaming mapper, by reason'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/user/hive/warehouse/inf2006.db/gdelt_audit_rejects'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813476')
;
-- ===== gdelt_daily_audit =====
CREATE EXTERNAL TABLE `inf2006.gdelt_daily_audit`(
  `audit_key` string COMMENT 'DATEADDED yyyymmdd, or BAD_<reason> counter key', 
  `events` bigint COMMENT 'number of event rows', 
  `avg_tone` string COMMENT 'mean AvgTone (NA for counter keys)', 
  `lag_events` bigint COMMENT 'events whose SQLDATE differs from DATEADDED')
COMMENT 'Per-day GDELT audit summary produced by Hadoop Streaming'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/audit/gdelt_daily/v2026-10-29-q1-2016'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813436')
;
-- ===== gdelt_daily_typed =====
CREATE TABLE `inf2006.gdelt_daily_typed`(
  `date_added` date, 
  `events` bigint, 
  `avg_tone` double, 
  `lag_events` bigint)
COMMENT 'Streaming audit rows with a real DATE and numeric tone\; BAD_* counters excluded'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/user/hive/warehouse/inf2006.db/gdelt_daily_typed'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813475')
;
-- ===== prices_typed =====
CREATE TABLE `inf2006.prices_typed`(
  `ticker` string, 
  `trade_date` date, 
  `yr` int, 
  `open` double, 
  `high` double, 
  `low` double, 
  `close` double, 
  `volume` bigint)
COMMENT 'raw_prices with typed date and year\; no rows removed'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/user/hive/warehouse/inf2006.db/prices_typed'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813474')
;
-- ===== prices_yearly_coverage =====
CREATE TABLE `inf2006.prices_yearly_coverage`(
  `yr` int, 
  `n_rows` bigint, 
  `tickers` bigint, 
  `first_day` date, 
  `last_day` date)
COMMENT 'Rows, tickers and date range per calendar year in prices_typed'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/user/hive/warehouse/inf2006.db/prices_yearly_coverage'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813478')
;
-- ===== raw_prices =====
CREATE EXTERNAL TABLE `inf2006.raw_prices`(
  `trade_date` string COMMENT 'yyyy-MM-dd trading day (US market)', 
  `open` double COMMENT 'opening price, USD', 
  `high` double COMMENT 'intraday high, USD', 
  `low` double COMMENT 'intraday low, USD', 
  `close` double COMMENT 'closing price, USD', 
  `volume` bigint COMMENT 'shares traded', 
  `ticker` string COMMENT 'exchange ticker symbol (source column Name)')
COMMENT 'Raw daily price bars as downloaded\; blank numeric fields read as NULL'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/raw/prices'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1794813436')
;
-- ===== raw_prices_duplicates =====
CREATE TABLE `inf2006.raw_prices_duplicates`(
  `duplicate_ticker_date` bigint)
COMMENT 'Number of (ticker, trade_date) keys that appear more than once in raw_prices'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/user/hive/warehouse/inf2006.db/raw_prices_duplicates'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813468')
;
-- ===== raw_prices_quality =====
CREATE TABLE `inf2006.raw_prices_quality`(
  `rows_total` bigint, 
  `tickers` bigint, 
  `date_min` string, 
  `date_max` string, 
  `rows_up_to_2017_12_29` bigint, 
  `empty_open` bigint, 
  `empty_high` bigint, 
  `empty_low` bigint, 
  `empty_close` bigint, 
  `empty_volume` bigint, 
  `ohlc_inconsistent` bigint, 
  `nonpositive` bigint)
COMMENT 'Table-level quality counts of raw_prices, computed in Hive'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/user/hive/warehouse/inf2006.db/raw_prices_quality'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813464')
;
-- ===== raw_prices_series_length =====
CREATE TABLE `inf2006.raw_prices_series_length`(
  `ticker` string, 
  `n_rows` bigint, 
  `first_day` string, 
  `last_day` string, 
  `short_series` int)
COMMENT 'Rows per ticker in raw_prices (short series flagged below 500 rows)'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ip-172-31-31-40.ec2.internal:8020/user/hive/warehouse/inf2006.db/raw_prices_series_length'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1794813470')
;

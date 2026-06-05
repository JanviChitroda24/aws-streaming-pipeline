-- ============================================================================
-- athena_validation_queries.sql — Bronze layer validation (Hours 8–10)
--
-- PREREQ:
--   1. raw-trades-crawler has run → table stock_streaming_db.bronze_raw_trades exists
--   2. Athena query result location is set:
--        Athena → Settings → s3://stock-streaming-pipeline-jc/athena-results/
--
-- Run each query in the Athena Query editor with database = stock_streaming_db.
-- ============================================================================


-- 1. Trade count + avg price + time range per ticker -------------------------
SELECT ticker,
       COUNT(*)               AS trade_count,
       ROUND(AVG(price), 2)   AS avg_price,
       MIN(event_time)        AS first_trade,
       MAX(event_time)        AS last_trade
FROM stock_streaming_db.bronze_raw_trades
GROUP BY ticker
ORDER BY trade_count DESC;


-- 2. Totals — records, unique tickers, sources ------------------------------
SELECT COUNT(*)                  AS total_records,
       COUNT(DISTINCT ticker)    AS unique_tickers,
       COUNT(DISTINCT source)    AS unique_sources,
       MIN(event_time)           AS earliest_trade,
       MAX(event_time)           AS latest_trade
FROM stock_streaming_db.bronze_raw_trades;


-- 3. Source distribution (simulator vs finnhub_live) ------------------------
SELECT source, COUNT(*) AS count
FROM stock_streaming_db.bronze_raw_trades
GROUP BY source;


-- 4. Duplicate check — exactly-once validation ------------------------------
-- Should return 0 rows.
SELECT trade_id, COUNT(*) AS cnt
FROM stock_streaming_db.bronze_raw_trades
GROUP BY trade_id
HAVING COUNT(*) > 1;


-- 5. Pipeline latency — avg seconds between event_time and ingested_at ------
-- The gap = how long from trade happening to landing in S3 (micro-batch latency).
SELECT ROUND(AVG(
         CAST(to_unixtime(ingested_at) AS DOUBLE)
       - CAST(to_unixtime(event_time)  AS DOUBLE)
       ), 3) AS avg_latency_seconds
FROM stock_streaming_db.bronze_raw_trades;


-- 6. Data quality — 7 checks in one query -----------------------------------
-- All violations should be 0.
SELECT 'null_trade_id'   AS check_name, COUNT(*) AS violations FROM stock_streaming_db.bronze_raw_trades WHERE trade_id IS NULL
UNION ALL
SELECT 'null_ticker',    COUNT(*) FROM stock_streaming_db.bronze_raw_trades WHERE ticker IS NULL
UNION ALL
SELECT 'negative_price', COUNT(*) FROM stock_streaming_db.bronze_raw_trades WHERE price <= 0
UNION ALL
SELECT 'zero_quantity',  COUNT(*) FROM stock_streaming_db.bronze_raw_trades WHERE quantity <= 0
UNION ALL
SELECT 'invalid_side',   COUNT(*) FROM stock_streaming_db.bronze_raw_trades WHERE side NOT IN ('buy', 'sell', 'unknown')
UNION ALL
SELECT 'null_timestamp', COUNT(*) FROM stock_streaming_db.bronze_raw_trades WHERE event_time IS NULL
UNION ALL
SELECT 'future_trades',  COUNT(*) FROM stock_streaming_db.bronze_raw_trades WHERE event_time > current_timestamp + interval '5' minute;

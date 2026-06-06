-- ============================================================================
-- athena_analytics_queries.sql — Production analytics queries (Hour 11)
--
-- Interview-demo-ready queries across all three medallion layers.
-- Run in Athena with database = stock_streaming_db.
-- (Validation/DQ basics live in athena_validation_queries.sql.)
-- ============================================================================


-- ============================================================================
-- 1. VWAP Dashboard — latest windows per ticker (Silver layer)
-- ============================================================================
-- Most recent 1-min VWAP per ticker. A dashboard tool (Grafana/QuickSight)
-- would refresh this every ~30s for near-real-time VWAP.

SELECT ticker,
       window_start,
       window_end,
       ROUND(vwap, 2)            AS vwap,
       total_volume,
       trade_count,
       ROUND(buy_ratio * 100, 1) AS buy_pct,
       source
FROM stock_streaming_db.silver_vwap_1min
ORDER BY window_start DESC, ticker
LIMIT 50;


-- ============================================================================
-- 2. Anomaly Summary — per-ticker anomaly stats (Gold layer)
-- ============================================================================
-- Which tickers have the most anomalies? Worst deviation? above vs below?

SELECT ticker,
       COUNT(*)                                             AS anomaly_count,
       ROUND(AVG(deviation_pct) * 100, 2)                   AS avg_deviation_pct,
       ROUND(MAX(deviation_pct) * 100, 2)                   AS max_deviation_pct,
       ROUND(MIN(deviation_pct) * 100, 2)                   AS min_deviation_pct,
       SUM(CASE WHEN direction = 'above' THEN 1 ELSE 0 END) AS above_count,
       SUM(CASE WHEN direction = 'below' THEN 1 ELSE 0 END) AS below_count
FROM stock_streaming_db.gold_anomaly_alerts
GROUP BY ticker
ORDER BY anomaly_count DESC;


-- ============================================================================
-- 3. BATCH RECONCILIATION — streaming VWAP vs batch recompute (Bronze + Silver)
-- ============================================================================
-- THE MOST IMPORTANT QUERY FOR INTERVIEWS.
-- Recompute VWAP from raw bronze trades with a batch GROUP BY, compare to the
-- streaming silver VWAP. diff_pct < 1% → streaming is mathematically correct.
-- Small diffs are expected: watermark drops late trades from streaming (batch
-- keeps everything); micro-batch boundaries don't align perfectly to minutes.
-- diff_pct > 5% → investigate.

WITH batch_vwap AS (
    SELECT ticker,
           DATE_TRUNC('minute', event_time) AS minute,
           SUM(price * quantity) / SUM(CAST(quantity AS DOUBLE)) AS batch_vwap,
           SUM(quantity) AS batch_volume,
           COUNT(*)      AS batch_trades
    FROM stock_streaming_db.bronze_raw_trades
    GROUP BY ticker, DATE_TRUNC('minute', event_time)
),
streaming_vwap AS (
    SELECT ticker,
           window_start AS minute,
           vwap         AS stream_vwap,
           total_volume AS stream_volume,
           trade_count  AS stream_trades
    FROM stock_streaming_db.silver_vwap_1min
)
SELECT b.ticker,
       b.minute,
       ROUND(b.batch_vwap, 4)  AS batch_vwap,
       ROUND(s.stream_vwap, 4) AS stream_vwap,
       ROUND(ABS(b.batch_vwap - s.stream_vwap) / b.batch_vwap * 100, 4) AS diff_pct,
       b.batch_volume,  s.stream_volume,
       b.batch_trades,  s.stream_trades
FROM batch_vwap b
JOIN streaming_vwap s ON b.ticker = s.ticker AND b.minute = s.minute
ORDER BY diff_pct DESC
LIMIT 50;


-- ============================================================================
-- 4. Data Quality Summary — all checks in one query (Bronze layer)
-- ============================================================================
-- All violations should be 0. Run after every pipeline session.

SELECT 'null_trade_id'   AS check_name, COUNT(*) AS violations
  FROM stock_streaming_db.bronze_raw_trades WHERE trade_id IS NULL
UNION ALL
SELECT 'null_ticker',    COUNT(*)
  FROM stock_streaming_db.bronze_raw_trades WHERE ticker IS NULL
UNION ALL
SELECT 'negative_price', COUNT(*)
  FROM stock_streaming_db.bronze_raw_trades WHERE price <= 0
UNION ALL
SELECT 'zero_quantity',  COUNT(*)
  FROM stock_streaming_db.bronze_raw_trades WHERE quantity <= 0
UNION ALL
SELECT 'invalid_side',   COUNT(*)
  FROM stock_streaming_db.bronze_raw_trades WHERE side NOT IN ('buy', 'sell', 'unknown')
UNION ALL
SELECT 'null_timestamp', COUNT(*)
  FROM stock_streaming_db.bronze_raw_trades WHERE event_time IS NULL
UNION ALL
SELECT 'future_trades',  COUNT(*)
  FROM stock_streaming_db.bronze_raw_trades WHERE event_time > current_timestamp + interval '5' minute
UNION ALL
SELECT 'duplicate_ids',  COUNT(*) - COUNT(DISTINCT trade_id)
  FROM stock_streaming_db.bronze_raw_trades;


-- ============================================================================
-- 5. Pipeline Health — records per layer (Cross-layer)
-- ============================================================================
-- Bronze should have the most rows, gold the least.

SELECT 'bronze_raw_trades'   AS layer, COUNT(*) AS record_count
  FROM stock_streaming_db.bronze_raw_trades
UNION ALL
SELECT 'silver_vwap_1min',   COUNT(*)
  FROM stock_streaming_db.silver_vwap_1min
UNION ALL
SELECT 'silver_vwap_5min',   COUNT(*)
  FROM stock_streaming_db.silver_vwap_5min
UNION ALL
SELECT 'gold_anomaly_alerts', COUNT(*)
  FROM stock_streaming_db.gold_anomaly_alerts;


-- ============================================================================
-- 6. 1-Min vs 5-Min VWAP Comparison (Silver layer)
-- ============================================================================
-- Short-term (1-min) vs trend (5-min). Large divergence = short-term move that
-- hasn't shifted the trend yet. Join each 1-min window into its 5-min window.

WITH vwap_1m AS (
    SELECT ticker, window_start, vwap AS vwap_1min
    FROM stock_streaming_db.silver_vwap_1min
),
vwap_5m AS (
    SELECT ticker,
           window_start AS window_5m_start,
           window_end   AS window_5m_end,
           vwap         AS vwap_5min
    FROM stock_streaming_db.silver_vwap_5min
)
SELECT v1.ticker,
       v1.window_start                AS minute,
       ROUND(v1.vwap_1min, 2)         AS vwap_1min,
       ROUND(v5.vwap_5min, 2)         AS vwap_5min,
       ROUND(ABS(v1.vwap_1min - v5.vwap_5min) / v5.vwap_5min * 100, 2) AS divergence_pct
FROM vwap_1m v1
JOIN vwap_5m v5
  ON v1.ticker = v5.ticker
 AND v1.window_start >= v5.window_5m_start
 AND v1.window_start <  v5.window_5m_end
ORDER BY divergence_pct DESC
LIMIT 30;


-- ============================================================================
-- 7. Anomaly Timeline — when do anomalies cluster? (Gold layer)
-- ============================================================================
-- Anomaly count per minute. A cluster in one minute across many tickers
-- suggests a market-wide event, not a single ticker acting up.

SELECT DATE_TRUNC('minute', detected_at)  AS minute,
       COUNT(*)                           AS anomalies_in_minute,
       COUNT(DISTINCT ticker)             AS tickers_affected,
       ROUND(AVG(deviation_pct) * 100, 2) AS avg_deviation_pct
FROM stock_streaming_db.gold_anomaly_alerts
GROUP BY DATE_TRUNC('minute', detected_at)
ORDER BY minute;

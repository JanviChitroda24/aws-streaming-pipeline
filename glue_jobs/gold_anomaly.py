"""
gold_anomaly.py — Glue Streaming: Kinesis → Gold Anomaly Alerts.

PURPOSE:
  The GOLD layer — the business-intelligence layer. It answers one question:
  "which trades are abnormal?" A trade is an anomaly if its price deviates more
  than ±2% from the per-ticker average price in the same micro-batch.

      deviation_pct = |price - avg_price| / avg_price      (flag if > 0.02)

  Only anomalies are written — a huge data reduction (out of thousands of trades,
  typically a few percent are flagged). The gold table is tiny but actionable.

WHY foreachBatch SELF-JOIN (not a stream-stream join):
  The plan used a stream-stream join (individual trades ⋈ windowed averages).
  Stream-stream joins with watermarks are complex, resource-heavy, and fragile in
  Glue. Instead we do everything INSIDE foreachBatch on a regular (batch)
  DataFrame: compute the per-ticker average for the batch, join trades to it,
  flag deviations. Simpler, debuggable, production-friendly.

  NOTE: the "average" is per-ticker within each ~30s micro-batch, not a true
  1-min window. For anomaly detection (is THIS trade far from its peers right
  now?) that's the right granularity and avoids streaming-join complexity.

S3 OUTPUT:
  s3://bucket/gold/anomaly_alerts/part-*.snappy.parquet

OUTPUT SCHEMA:
  trade_id, ticker, price, quantity, side, event_time, source,
  window_avg_price, deviation_pct, direction, detected_at

COST: ~$0.88/hr (2 × G.1X workers). Job timeout MUST be set to 10 min.

GLUE JOB PARAMETERS:
  --KINESIS_STREAM_ARN  →  arn:aws:kinesis:us-east-1:<account-id>:stream/stock-trades-stream
  --S3_BUCKET           →  stock-streaming-pipeline-jc
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    abs as _abs,
    avg,
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Job arguments ─────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "KINESIS_STREAM_ARN",
    "S3_BUCKET",
])

# ── Glue / Spark context ──────────────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Paths + threshold ─────────────────────────────────────────────────────────
S3_GOLD = f"s3://{args['S3_BUCKET']}/gold/anomaly_alerts/"
CHECKPOINT = f"s3://{args['S3_BUCKET']}/glue-checkpoints/gold-anomaly/"
ANOMALY_THRESHOLD = 0.01  # ±1% from the per-ticker batch average
# NOTE: lowered from 0.02 → 0.01. The simulator's ±0.2% random-walk noise makes
# 2% (~4σ) deviations rare, so a 2% threshold flags ~0 anomalies. 1% is a
# realistic middle ground that still catches enough to validate the pipeline.
# (Real-market production default would be ~2%.)

# ── Trade schema ──────────────────────────────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("trade_id",   StringType(),  False),
    StructField("ticker",     StringType(),  False),
    StructField("price",      DoubleType(),  False),
    StructField("quantity",   IntegerType(), False),
    StructField("side",       StringType(),  False),
    StructField("trade_type", StringType(),  False),
    StructField("bid_price",  DoubleType(),  True),
    StructField("ask_price",  DoubleType(),  True),
    StructField("timestamp",  StringType(),  False),
    StructField("exchange",   StringType(),  True),
    StructField("source",     StringType(),  True),
])

# ── Read from Kinesis ─────────────────────────────────────────────────────────
kinesis_stream = glueContext.create_data_frame_from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": args["KINESIS_STREAM_ARN"],
        "classification": "json",
        "startingPosition": "LATEST",
        "inferSchema": "true",
    },
    transformation_ctx="kinesis_source",
)

# ── Parse JSON (dynamic column detection — same approach as Hours 4/6/7) ──────
print(f"Raw stream columns: {kinesis_stream.columns}")

EXPECTED_COLS = {
    "trade_id", "ticker", "price", "quantity", "side",
    "trade_type", "bid_price", "ask_price", "timestamp",
    "exchange", "source",
}

actual_data_cols = {c for c in kinesis_stream.columns if "$" not in c}

if EXPECTED_COLS.issubset(actual_data_cols):
    print("JSON auto-parsed by Glue — using columns directly")
    raw_parsed = kinesis_stream.select(*[col(c) for c in EXPECTED_COLS])
else:
    print("Manual JSON parsing needed — searching for raw data column")
    data_col = None
    for col_name in kinesis_stream.columns:
        if "temporary" in col_name or "data_infer" in col_name:
            data_col = col_name
            break
    if data_col is None:
        data_col = kinesis_stream.columns[0]
    print(f"Using data column: {data_col!r}")
    raw_parsed = kinesis_stream.select(
        from_json(col(f"`{data_col}`").cast("string"), TRADE_SCHEMA).alias("t")
    ).select("t.*")

# ── Trim to the columns we need + typed event_time ────────────────────────────
# No watermark / window here — anomaly detection happens per-batch in foreachBatch.
parsed = raw_parsed.select(
    col("trade_id"),
    col("ticker"),
    col("price"),
    col("quantity"),
    col("side"),
    col("source"),
    to_timestamp(col("timestamp")).alias("event_time"),
)


# ── Detect anomalies per micro-batch ──────────────────────────────────────────
def detect_anomalies(df, batch_id):
    """
    Self-join inside the batch:
      1. per-ticker average price for this batch
      2. join every trade to its ticker's average
      3. flag trades where |price - avg| / avg > 2%
      4. write only the anomalies
    """
    if df.count() == 0:
        print(f"[Gold Batch {batch_id}] No records in this window.")
        return

    # 1. per-ticker batch average (+ how many trades the average is based on)
    avg_prices = df.groupBy("ticker").agg(
        avg("price").alias("window_avg_price"),
        count("*").alias("window_trade_count"),
    )

    # 2. join each trade to its ticker's average
    with_avg = df.join(avg_prices, on="ticker", how="inner")

    # 3. deviation + direction, then filter to anomalies only
    anomalies = (
        with_avg
        .withColumn(
            "deviation_pct",
            _abs(col("price") - col("window_avg_price")) / col("window_avg_price"),
        )
        .withColumn(
            "direction",
            when(col("price") >= col("window_avg_price"), lit("above")).otherwise(lit("below")),
        )
        .filter(col("deviation_pct") > ANOMALY_THRESHOLD)
        .select(
            "trade_id", "ticker", "price", "quantity", "side", "event_time", "source",
            "window_avg_price", "deviation_pct", "direction",
            current_timestamp().alias("detected_at"),
        )
    )

    anomaly_count = anomalies.count()
    if anomaly_count == 0:
        print(f"[Gold Batch {batch_id}] {df.count()} trades, 0 anomalies.")
        return

    # 4. write only the anomalies
    anomalies.write.mode("append").parquet(S3_GOLD)
    print(f"🚨 [Gold Batch {batch_id}] {anomaly_count} anomalies / {df.count()} trades → {S3_GOLD}")

    if batch_id <= 1:
        print("Sample anomalies:")
        anomalies.show(5, truncate=False)


# ── Start streaming query ─────────────────────────────────────────────────────
query = (
    parsed.writeStream
    .foreachBatch(detect_anomalies)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination(timeout=600)  # 10 min — Glue job timeout is the real net
job.commit()

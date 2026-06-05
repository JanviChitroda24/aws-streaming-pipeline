"""
silver_vwap_5min.py — Glue Streaming: Kinesis → Silver VWAP (5-min windows).

PURPOSE:
  Computes Volume-Weighted Average Price (VWAP) over 5-minute tumbling windows
  per ticker. Companion to silver_vwap.py (1-min). This is the SILVER layer —
  cleaned, aggregated, analytically useful.

      VWAP = sum(price × quantity) / sum(quantity)

  Uses a 10-second watermark to handle late-arriving events — same logic as the
  1-min job and Project 03.

WHY BOTH 1-MIN AND 5-MIN:
  Different granularities serve different purposes. 1-min captures short-term
  price action (scalpers, real-time alerts); 5-min smooths noise (trend analysis,
  institutional traders). Divergence between the two is itself an anomaly signal.

  This is a SEPARATE job (not a second query in silver_vwap.py) to keep one job =
  one streaming query = simpler resource management and debugging.

S3 OUTPUT:
  s3://bucket/silver/vwap_5min/part-*.snappy.parquet

OUTPUT SCHEMA:
  window_start, window_end, ticker, vwap, total_volume, trade_count,
  low_price, high_price, buy_ratio, source, computed_at

COST: ~$0.88/hr (2 × G.1X workers). Job timeout MUST be set to 10 min.
  NOTE: a 5-min window needs 5 min of data + 10s watermark before it closes, so
  run the producer ~6-7 min to see the first completed window land in S3.

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
    col,
    count,
    current_timestamp,
    first,
    from_json,
    max as _max,
    min as _min,
    sum as _sum,
    to_timestamp,
    when,
    window,
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

# ── Paths (5-min variant) ─────────────────────────────────────────────────────
S3_SILVER_5MIN = f"s3://{args['S3_BUCKET']}/silver/vwap_5min/"
CHECKPOINT = f"s3://{args['S3_BUCKET']}/glue-checkpoints/silver-vwap-5min/"

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

# ── Parse JSON (dynamic column detection — same approach as Hours 4/6) ────────
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

# ── Prepare for VWAP ──────────────────────────────────────────────────────────
parsed = raw_parsed.select(
    col("ticker"),
    col("price"),
    col("quantity"),
    (col("price") * col("quantity")).alias("notional"),
    col("side"),
    col("source"),  # carried through for traceability (simulator vs finnhub_live)
    to_timestamp(col("timestamp")).alias("event_time"),
).withWatermark("event_time", "10 seconds")

# ── 5-Minute VWAP aggregation ─────────────────────────────────────────────────
# Tumbling 5-min window per ticker. Boundaries align to the clock
# (14:00:00–14:05:00, ...). Identical to the 1-min job except the window size.
vwap_5min = parsed.groupBy(
    window(col("event_time"), "5 minutes"),
    col("ticker"),
).agg(
    (_sum("notional") / _sum("quantity")).alias("vwap"),
    _sum("quantity").alias("total_volume"),
    count("*").alias("trade_count"),
    _min("price").alias("low_price"),
    _max("price").alias("high_price"),
    # buy_ratio = fraction of trades on the buy side (0.0–1.0). sum(1/0 flag).
    (_sum(when(col("side") == "buy", 1).otherwise(0)) / count("*")).alias("buy_ratio"),
    # source carried through for traceability (not in groupBy — one source/session)
    first("source").alias("source"),
)


# ── Write each micro-batch to S3 ──────────────────────────────────────────────
def write_vwap_batch(df, batch_id):
    """Flatten the window struct and append 5-min VWAP results to S3 Silver."""
    if df.count() == 0:
        print(f"[Silver 5min Batch {batch_id}] No completed windows.")
        return

    flat = df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "ticker", "vwap", "total_volume", "trade_count",
        "low_price", "high_price", "buy_ratio", "source",
        current_timestamp().alias("computed_at"),
    )

    window_count = flat.count()
    flat.write.mode("append").parquet(S3_SILVER_5MIN)
    print(f"[Silver 5min Batch {batch_id}] Wrote {window_count} windows to {S3_SILVER_5MIN}")

    if batch_id <= 1:
        print("Sample VWAP windows:")
        flat.show(5, truncate=False)


# ── Start streaming query ─────────────────────────────────────────────────────
query = (
    vwap_5min.writeStream
    .foreachBatch(write_vwap_batch)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination(timeout=600)  # 10 min — Glue job timeout is the real net
job.commit()

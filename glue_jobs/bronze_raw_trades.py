"""
bronze_raw_trades.py — Glue Streaming: Kinesis → S3 Bronze Layer.

PURPOSE:
  Reads raw trade events from Kinesis and writes them to S3 as Parquet.
  This is the BRONZE layer — append-only, no transformations to the trade
  values themselves, every record preserved forever. Same role as
  Project 03's Delta Lake raw_trades writer, but S3 + Parquet instead of Delta.

HOW IT WORKS (delta from stream_reader_test.py — only 2 things change):
  1. Reads Kinesis + parses JSON   ← identical to the Hour 4 reader test
  2. Adds event_time / ingested_at / year / month / day columns  ← NEW
  3. process_batch() WRITES Parquet to S3 instead of printing to logs  ← NEW

S3 OUTPUT (Hive-style partitioning):
  s3://bucket/bronze/raw_trades/year=2026/month=6/day=4/ticker=AAPL/part-00000.snappy.parquet

  Partitioning by year/month/day/ticker lets Athena prune partitions:
  WHERE ticker='AAPL' AND day=4 reads only that folder, not the whole dataset.

COST: ~$0.88/hr (2 × G.1X workers). Job timeout MUST be set to 10 min.

GLUE JOB PARAMETERS (set in console / CLI --default-arguments):
  --KINESIS_STREAM_ARN  →  arn:aws:kinesis:us-east-1:<account-id>:stream/stock-trades-stream
  --S3_BUCKET           →  stock-streaming-pipeline-jc

GLUE JOB SETTINGS:
  Type:              Spark Streaming
  Glue version:      Glue 4.0 (Spark 3.3, Python 3)
  Worker type:       G.1X
  Number of workers: 2
  Job timeout:       10 (minutes) ← NEVER remove this
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofmonth,
    from_json,
    month,
    to_timestamp,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Job arguments ─────────────────────────────────────────────────────────────
# Injected by Glue from the job parameters. getResolvedOptions reads sys.argv —
# do NOT rename these keys without updating the Glue job parameter keys too.
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

# ── Paths ─────────────────────────────────────────────────────────────────────
S3_BRONZE = f"s3://{args['S3_BUCKET']}/bronze/raw_trades/"
CHECKPOINT = f"s3://{args['S3_BUCKET']}/glue-checkpoints/bronze-raw/"

# ── Trade schema ──────────────────────────────────────────────────────────────
# Must match the JSON emitted by src/producer.py exactly (11 fields).
# bid_price / ask_price are nullable (True) — Finnhub free tier sends null.
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

# ── Parse JSON (dynamic column detection — same approach as Hour 4) ────────────
# Glue 4.0 sometimes auto-parses JSON into columns, sometimes hands us a single
# raw-bytes column with an internal name like $json$data_infer_schema$_temporary$.
# We detect which case we're in and handle both, so the job is robust across runs.
print(f"Raw stream columns: {kinesis_stream.columns}")

EXPECTED_COLS = {
    "trade_id", "ticker", "price", "quantity", "side",
    "trade_type", "bid_price", "ask_price", "timestamp",
    "exchange", "source",
}

# Glue internal columns contain "$" — exclude them when checking for data columns
actual_data_cols = {c for c in kinesis_stream.columns if "$" not in c}

if EXPECTED_COLS.issubset(actual_data_cols):
    # Glue already parsed the JSON into columns — use them directly
    print("JSON auto-parsed by Glue — using columns directly")
    raw_parsed = kinesis_stream.select(*[col(c) for c in EXPECTED_COLS])
else:
    # Need manual JSON parsing — find the raw data column
    print("Manual JSON parsing needed — searching for raw data column")
    data_col = None
    for col_name in kinesis_stream.columns:
        if "temporary" in col_name or "data_infer" in col_name:
            data_col = col_name
            break
    if data_col is None:
        data_col = kinesis_stream.columns[0]  # last resort

    print(f"Using data column: {data_col!r}")
    raw_parsed = kinesis_stream.select(
        from_json(
            col(f"`{data_col}`").cast("string"),
            TRADE_SCHEMA,
        ).alias("t")
    ).select("t.*")

# ── Transform: add event_time, ingested_at, and partition columns ─────────────
# event_time  : ISO string → Spark TimestampType (needed to derive year/month/day)
# ingested_at : wall-clock time Glue processed the record — vs event_time, exposes
#               replay / late-data situations when the two diverge
# year/month/day : Hive partition keys for Athena partition pruning
# timestamp is dropped — superseded by the typed event_time column
parsed = (
    raw_parsed
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("ingested_at", current_timestamp())
    .withColumn("year", year("event_time"))
    .withColumn("month", month("event_time"))
    .withColumn("day", dayofmonth("event_time"))
    .drop("timestamp")
)


# ── Write each micro-batch to S3 as partitioned Parquet ───────────────────────
def write_bronze_batch(df, batch_id):
    """
    Write raw trades to the S3 Bronze layer as partitioned Parquet.

    mode("append") is critical — bronze is an append-only event log. Never
    overwrite history. Every micro-batch adds new files to existing partitions.

    Partition strategy year/month/day/ticker mirrors Delta Lake partitioning in
    Project 03 — most queries filter by date + ticker, so pruning is cheap.
    """
    count = df.count()
    if count == 0:
        print(f"[Bronze Batch {batch_id}] No new records in this window.")
        return

    df.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "ticker") \
        .parquet(S3_BRONZE)

    print(f"[Bronze Batch {batch_id}] Wrote {count:,} records to {S3_BRONZE}")

    # Extra detail on the first batch — confirm schema + ticker coverage once
    if batch_id == 0:
        print("=== DEBUG: Schema written to bronze ===")
        df.printSchema()
        print("Sample records:")
        df.show(3, truncate=False)
        print(f"Unique tickers this batch: {df.select('ticker').distinct().count()}")


# ── Start streaming query ─────────────────────────────────────────────────────
# Checkpoint in S3 records Kinesis sequence numbers so a restarted job resumes
# from the right position (exactly-once) — Glue workers are ephemeral, local
# disk would be lost, so checkpoints must live in S3.
query = (
    parsed.writeStream
    .foreachBatch(write_bronze_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

# awaitTermination(600) = up to 10 min at the code level. The Glue job timeout
# (10 min in the console/CLI) is the real infrastructure-level safety net.
query.awaitTermination(timeout=600)

# Required — Glue records the job bookmark here. Without it the job shows
# "failed" in the console even when all code ran successfully.
job.commit()

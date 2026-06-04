"""
stream_reader_test.py — Glue Streaming smoke test: Kinesis → CloudWatch logs.

PURPOSE:
  Proves the Kinesis → Glue connection works before adding S3 writes.
  This is a TEST job only — no data is persisted anywhere.
  Equivalent to Project 03's stream_reader.py console-sink smoke test.

HOW IT WORKS:
  1. Reads records from Kinesis using Glue's native Kinesis connector
  2. Detects whether Glue auto-parsed the JSON or gave us raw bytes
  3. Parses the JSON payload into our 11-field TRADE_SCHEMA
  4. Calls process_batch() every 30 seconds
  5. process_batch() prints record count + sample rows → CloudWatch logs

WHERE TO SEE OUTPUT:
  CloudWatch → Log groups → /aws-glue/jobs/output → your-job-run-stream

COST: ~$0.88/hr (2 × G.1X workers). Job timeout MUST be set to 10 min.

GLUE JOB PARAMETERS (set in console under Job details → Job parameters):
  --KINESIS_STREAM_ARN  →  arn:aws:kinesis:us-east-1:<account-id>:stream/stock-trades-stream
  --REGION             →  us-east-1
  --S3_BUCKET          →  stock-streaming-pipeline-jc

GLUE JOB SETTINGS:
  Type:            Spark Streaming
  Glue version:    Glue 4.0 (Spark 3.3, Python 3)
  Worker type:     G.1X
  Number of workers: 2
  Job timeout:     10 (minutes) ← NEVER remove this
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Job arguments ─────────────────────────────────────────────────────────────
# These are injected by Glue from the job parameters you set in the console.
# getResolvedOptions reads from sys.argv — do NOT change the key names here
# without updating the job parameter keys in the console.
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "KINESIS_STREAM_ARN",
    "REGION",
    "S3_BUCKET",
])

# ── Glue / Spark context ──────────────────────────────────────────────────────
# In local Spark (Project 03): SparkSession.builder.getOrCreate()
# In Glue: always GlueContext(SparkContext()), then use glueContext.spark_session
# The resulting spark object has the same PySpark API — from_json, groupBy, etc.
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Trade schema ──────────────────────────────────────────────────────────────
# Must match the JSON emitted by src/producer.py exactly.
# Same 11 fields as Project 03's TRADE_SCHEMA in stream_reader.py.
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
# classification: "json" + inferSchema: "true" lets Glue attempt auto-parsing.
# If Glue detects the columns directly, we skip manual from_json parsing.
# If not, we fall back to manual parsing in the section below.
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

# ── Parse JSON ────────────────────────────────────────────────────────────────
# With inferSchema=true and classification=json, Glue may auto-parse the JSON
# and expose columns directly. We check for that and fall back to manual parsing
# if needed. This handles differences across Glue versions.
print(f"Raw stream columns: {kinesis_stream.columns}")

EXPECTED_COLS = {
    "trade_id", "ticker", "price", "quantity", "side",
    "trade_type", "bid_price", "ask_price", "timestamp",
    "exchange", "source",
}

# Filter out Glue internal columns (contain $ in the name)
actual_data_cols = {c for c in kinesis_stream.columns if "$" not in c}

if EXPECTED_COLS.issubset(actual_data_cols):
    # Glue already parsed the JSON into columns — use them directly
    print("JSON auto-parsed by Glue — using columns directly")
    parsed = kinesis_stream.select(*[col(c) for c in EXPECTED_COLS])
else:
    # Need manual JSON parsing — find the raw data column
    print("Manual JSON parsing needed — searching for raw data column")
    data_col = None
    for col_name in kinesis_stream.columns:
        if "temporary" in col_name or "data_infer" in col_name:
            data_col = col_name
            break
    if data_col is None:
        # Last resort: use the first column
        data_col = kinesis_stream.columns[0]

    print(f"Using data column: {data_col!r}")
    parsed = kinesis_stream.select(
        from_json(
            col(f"`{data_col}`").cast("string"),
            TRADE_SCHEMA,
        ).alias("trade")
    ).select("trade.*")


# ── Process each micro-batch ──────────────────────────────────────────────────
def process_batch(df, batch_id):
    """
    Called by Spark every 30 seconds with a batch of new records.
    Prints stats and sample rows to stdout → CloudWatch /aws-glue/jobs/output.

    In Hours 6–7 (bronze job), this function is replaced with an S3 Parquet write.
    """
    # DEBUG on first batch — print the full schema so we can confirm parsing worked
    if batch_id == 0:
        print("=== DEBUG: Schema of parsed DataFrame ===")
        df.printSchema()
        print("=== DEBUG: Schema of raw kinesis_stream ===")
        kinesis_stream.printSchema()

    count = df.count()

    if count == 0:
        print(f"[Batch {batch_id}] No new records in this window.")
        return

    print(f"\n{'='*60}")
    print(f"[Batch {batch_id}] {count:,} records received")
    print(f"{'='*60}")

    # Sample rows — visible in CloudWatch logs
    print("Sample records:")
    df.show(5, truncate=False)

    # Ticker distribution — confirms all 25 tickers are flowing
    print("Ticker distribution (top 10):")
    df.groupBy("ticker").count().orderBy("count", ascending=False).show(10)

    # Source breakdown — simulator vs finnhub_live
    print("Source breakdown:")
    df.groupBy("source").count().show()

    print(f"Unique tickers this batch: {df.select('ticker').distinct().count()}")
    print(f"{'='*60}\n")


# ── Start streaming query ─────────────────────────────────────────────────────
# Checkpoint location in S3 — Spark records Kinesis sequence numbers here
# so it can resume from the right position if the job is restarted.
checkpoint_path = f"s3://{args['S3_BUCKET']}/glue-checkpoints/stream-reader-test/"

query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="30 seconds")   # micro-batch every 30s
    .start()
)

# awaitTermination(300) = wait up to 5 minutes.
# The Glue job timeout (10 min in console) is the real safety net — it kills
# the job even if awaitTermination is still blocking.
query.awaitTermination(timeout=300)

# job.commit() is required — Glue records the job bookmark here.
# Without it, the job shows as "failed" in the console even if all code ran.
job.commit()

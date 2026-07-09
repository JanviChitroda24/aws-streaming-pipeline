# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
infra_check.py — Lambda: verify Kinesis stream and S3 bucket are ready.

ROLE IN THE PIPELINE:
  First state in the Step Functions machine. Called BEFORE any Glue job starts.
  Fails fast if infrastructure is missing — saves $0.60+ of wasted Glue cold-start
  time when someone forgot to create the Kinesis stream.

RETURNS: {"kinesis": "ACTIVE", "s3": "OK", "status": "ALL_CHECKS_PASSED"}
RAISES:  Exception on any failure → Step Functions Catch → NotifyFailure.

IAM (execution role): AmazonKinesisReadOnlyAccess + AmazonS3ReadOnlyAccess
TIMEOUT: 30s
"""

import boto3

REGION = "us-east-1"
STREAM_NAME = "stock-trades-stream"
BUCKET = "stock-streaming-pipeline-jc"


def lambda_handler(event, context):
    kinesis = boto3.client("kinesis", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    results = {}

    # ── Kinesis: must exist AND be ACTIVE ─────────────────────────────────────
    try:
        resp = kinesis.describe_stream_summary(StreamName=STREAM_NAME)
        status = resp["StreamDescriptionSummary"]["StreamStatus"]
        if status != "ACTIVE":
            raise Exception(f"Kinesis stream status is '{status}', expected 'ACTIVE'")
        results["kinesis"] = "ACTIVE"
        print(f"✅ Kinesis stream: {status}")
    except kinesis.exceptions.ResourceNotFoundException:
        raise Exception(
            f"Kinesis stream '{STREAM_NAME}' does not exist. Create it: "
            f"aws kinesis create-stream --stream-name {STREAM_NAME} --shard-count 1 --region {REGION}"
        )

    # ── S3: bucket reachable ──────────────────────────────────────────────────
    try:
        s3.head_bucket(Bucket=BUCKET)
        results["s3"] = "OK"
        print("✅ S3 bucket: accessible")
    except Exception as e:
        raise Exception(f"S3 bucket '{BUCKET}' not accessible: {e}")

    # ── S3: medallion folder structure (informational — empty is fine first run)
    required_prefixes = [
        "bronze/raw_trades/",
        "silver/vwap_1min/",
        "silver/vwap_5min/",
        "gold/anomaly_alerts/",
        "glue-scripts/",
    ]
    for prefix in required_prefixes:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
        exists = resp.get("KeyCount", 0) > 0
        print(f"  S3 prefix {prefix}: {'exists' if exists else 'empty (ok for first run)'}")

    results["status"] = "ALL_CHECKS_PASSED"
    print(f"\n✅ Infrastructure check passed: {results}")
    return results

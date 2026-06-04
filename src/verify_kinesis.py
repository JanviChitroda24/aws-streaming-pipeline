"""
verify_kinesis.py — Hour 2 verification script.

Checks:
  1. Stream exists and is ACTIVE
  2. Write: puts a test record into the stream
  3. Read: reads it back using TRIM_HORIZON

Run:
  cd src
  python verify_kinesis.py
"""

import json
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from config import AWS_REGION, KINESIS_STREAM_NAME


def check_stream_status(kinesis) -> bool:
    try:
        resp = kinesis.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
        summary = resp["StreamDescriptionSummary"]
        status = summary["StreamStatus"]
        shards = summary["OpenShardCount"]
        print(f"✅ Stream status: {status}, Open shards: {shards}")
        return status == "ACTIVE"
    except ClientError as e:
        print(f"❌ Stream not found or access denied: {e}")
        return False


def test_write(kinesis) -> bool:
    test_record = {
        "trade_id": "verify-001",
        "ticker": "AAPL",
        "price": 150.25,
        "quantity": 100,
        "source": "verify_kinesis",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(test_record).encode("utf-8"),
            PartitionKey=test_record["ticker"],
        )
        print(f"✅ Write test passed — sent {test_record['trade_id']}")
        return True
    except ClientError as e:
        print(f"❌ Write test failed: {e}")
        return False


def test_read(kinesis) -> bool:
    # TRIM_HORIZON = read from the beginning of the shard (like Kafka's 'earliest')
    shard_iterator = kinesis.get_shard_iterator(
        StreamName=KINESIS_STREAM_NAME,
        ShardId="shardId-000000000000",
        ShardIteratorType="TRIM_HORIZON",
    )["ShardIterator"]

    time.sleep(2)  # give Kinesis a moment to make the record available

    response = kinesis.get_records(ShardIterator=shard_iterator, Limit=10)
    records = response["Records"]

    if not records:
        print("⚠️  Read test: no records found (stream may have been empty before write)")
        return False

    for record in records:
        data = json.loads(record["Data"])
        print(f"✅ Read test passed — got back: {data}")

    return True


def main():
    print("=" * 60)
    print("KINESIS STREAM VERIFICATION")
    print(f"  Stream: {KINESIS_STREAM_NAME}")
    print(f"  Region: {AWS_REGION}")
    print("=" * 60)

    kinesis = boto3.client("kinesis", region_name=AWS_REGION)

    ok = check_stream_status(kinesis)
    if not ok:
        print("\n❌ Stream is not ACTIVE. Create it in the AWS Console first.")
        return

    test_write(kinesis)
    test_read(kinesis)

    print("\n" + "=" * 60)
    print("Verification complete.")
    print("⚠️  Remember: delete the stream if done for the day.")
    print("  aws kinesis delete-stream --stream-name stock-trades-stream --region us-east-1")
    print("=" * 60)


if __name__ == "__main__":
    main()

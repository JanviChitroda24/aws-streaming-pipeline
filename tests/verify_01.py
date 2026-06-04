"""
verify_01.py — Hour 1 project setup verification.

Checks:
  1. .env exists and has all required keys
  2. src/config.py loads correctly with expected values
  3. S3 bucket exists and is accessible
  4. Required S3 folders exist (bronze, silver, gold, glue-scripts, etc.)
  5. Local folder structure is correct
  6. requirements.txt has required packages

Run from project root:
  python3 tests/verify_01.py
"""

import os
import sys

# Make src/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

results = []


def check(label: str, passed: bool, detail: str = ""):
    symbol = PASS if passed else FAIL
    msg = f"{symbol} {label}"
    if detail:
        msg += f"  ({detail})"
    print(msg)
    results.append(passed)


def section(title: str):
    print(f"\n--- {title} ---")


# ── 1. .env keys ─────────────────────────────────────────────────────────────
section("1. Environment Variables (.env)")

required_keys = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "KINESIS_STREAM_NAME",
    "S3_BUCKET",
    "FINNHUB_API_KEY",
]
for key in required_keys:
    val = os.getenv(key)
    check(key, bool(val), "set" if val else "MISSING")


# ── 2. config.py ─────────────────────────────────────────────────────────────
section("2. src/config.py")

try:
    import config

    check("config.py imports cleanly", True)
    check("AWS_REGION = us-east-1", config.AWS_REGION == "us-east-1", config.AWS_REGION)
    check("KINESIS_STREAM_NAME set", bool(config.KINESIS_STREAM_NAME), config.KINESIS_STREAM_NAME)
    check("S3_BUCKET set", bool(config.S3_BUCKET), config.S3_BUCKET)
    check("TICKERS has 25 entries", len(config.TICKERS) == 25, f"found {len(config.TICKERS)}")
    check("S3_BRONZE_RAW starts with s3://", config.S3_BRONZE_RAW.startswith("s3://"))
    check("ANOMALY_THRESHOLD_PCT = 0.02", config.ANOMALY_THRESHOLD_PCT == 0.02)
except Exception as e:
    check("config.py imports cleanly", False, str(e))


# ── 3. S3 bucket accessible ───────────────────────────────────────────────────
section("3. S3 Bucket")

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
bucket = os.getenv("S3_BUCKET")

try:
    s3.head_bucket(Bucket=bucket)
    check(f"Bucket '{bucket}' exists and is accessible", True)
except ClientError as e:
    code = e.response["Error"]["Code"]
    check(f"Bucket '{bucket}' exists and is accessible", False, f"Error {code}")


# ── 4. S3 folder structure ────────────────────────────────────────────────────
section("4. S3 Folder Structure")

required_prefixes = [
    "bronze/raw_trades/",
    "silver/vwap_1min/",
    "silver/vwap_5min/",
    "gold/anomaly_alerts/",
    "glue-scripts/",
    "glue-checkpoints/",
    "glue-temp/",
]

for prefix in required_prefixes:
    try:
        # A folder "exists" in S3 if list_objects returns any keys with that prefix
        # OR if the folder placeholder object exists
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        exists = resp.get("KeyCount", 0) > 0
        check(f"s3://{bucket}/{prefix}", exists, "found" if exists else "empty/missing — create in console")
    except ClientError as e:
        check(f"s3://{bucket}/{prefix}", False, str(e))


# ── 5. Local folder structure ─────────────────────────────────────────────────
section("5. Local Folder Structure")

project_root = os.path.join(os.path.dirname(__file__), "..")
required_folders = ["src", "glue_jobs", "lambda", "tests", "docs", "diagrams", "step_functions"]

for folder in required_folders:
    path = os.path.join(project_root, folder)
    check(f"{folder}/", os.path.isdir(path))


# ── 6. requirements.txt ───────────────────────────────────────────────────────
section("6. requirements.txt")

req_path = os.path.join(project_root, "requirements.txt")
if os.path.exists(req_path):
    with open(req_path) as f:
        content = f.read().lower()
    required_packages = ["boto3", "python-dotenv", "finnhub-python", "pytest"]
    for pkg in required_packages:
        check(pkg, pkg.replace("-", "") in content.replace("-", ""))
else:
    check("requirements.txt exists", False)


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
passed = sum(results)
total = len(results)
if passed == total:
    print(f"✅ ALL {total} CHECKS PASSED — Hour 1 setup is complete.")
else:
    print(f"⚠️  {passed}/{total} checks passed — fix the failures above before Hour 2.")
print("=" * 60)

# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
data_quality.py — Lambda: run Athena DQ checks, fail the pipeline if violations.

ROLE IN THE PIPELINE:
  Runs AFTER crawlers refresh the catalog. Executes 7 data-quality checks on the
  bronze layer via Athena. If ANY check has violations > 0, raises an exception →
  Step Functions Catch → NotifyFailure. This is the automated version of the
  manual Athena DQ query from the catalog hour.

RETURNS: {"status": "ALL_DQ_CHECKS_PASSED", "checks_run": N, "violations": 0}
RAISES:  Exception listing the failing checks.

IAM (execution role): AmazonAthenaFullAccess + AmazonS3FullAccess
                      + AWSGlueConsoleFullAccess (Athena reads the Glue Catalog)
TIMEOUT: 3 min (180s) — Athena queries take a few seconds each; this polls.
"""

import time

import boto3

REGION = "us-east-1"
DATABASE = "stock_streaming_db"
OUTPUT_LOCATION = "s3://stock-streaming-pipeline-jc/athena-results/"

athena = boto3.client("athena", region_name=REGION)

DQ_QUERY = """
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
SELECT 'duplicate_ids',  COUNT(*) - COUNT(DISTINCT trade_id)
  FROM stock_streaming_db.bronze_raw_trades
"""


def run_athena_query(query):
    """Submit a query, poll until it finishes, return the result set."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )
    query_id = response["QueryExecutionId"]
    print(f"Athena query started: {query_id}")

    for _ in range(24):  # up to ~2 min (24 × 5s)
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return athena.get_query_results(QueryExecutionId=query_id)
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown"
            )
            raise Exception(f"Athena query {state}: {reason}")
        time.sleep(5)

    raise Exception("Athena query timed out after ~2 minutes")


def lambda_handler(event, context):
    print("Running 7 data quality checks on bronze layer...")

    results = run_athena_query(DQ_QUERY)

    rows = results["ResultSet"]["Rows"][1:]  # skip header row
    violations_found = []

    for row in rows:
        check_name = row["Data"][0]["VarCharValue"]
        violations = int(row["Data"][1]["VarCharValue"])
        status = "✅ PASS" if violations == 0 else "❌ FAIL"
        print(f"  {status}  {check_name}: {violations} violations")
        if violations > 0:
            violations_found.append({"check": check_name, "violations": violations})

    if violations_found:
        error_msg = f"DQ FAILED: {len(violations_found)} checks have violations: {violations_found}"
        print(f"\n❌ {error_msg}")
        raise Exception(error_msg)

    print("\n✅ All 7 DQ checks passed — zero violations")
    return {"status": "ALL_DQ_CHECKS_PASSED", "checks_run": len(rows), "violations": 0}

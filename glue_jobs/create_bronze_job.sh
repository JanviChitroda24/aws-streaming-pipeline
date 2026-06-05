#!/usr/bin/env bash
#
# create_bronze_job.sh — register the Bronze Glue Streaming job (Hours 6–7).
#
# Run this ONCE to create the job. After it exists, you don't recreate it —
# you just start a run (see "START A RUN" below) or update the script in S3.
#
# Prereqs:
#   - Kinesis stream stock-trades-stream is ACTIVE
#   - IAM role GlueStreamingRole exists (Kinesis read + S3 write + CloudWatch)
#   - AWS CLI configured with credentials that can create Glue jobs
#
# Usage:
#   bash glue_jobs/create_bronze_job.sh
#
set -euo pipefail

# ── Config (edit here if names/account change) ────────────────────────────────
REGION="us-east-1"
ACCOUNT_ID="366447947905"
BUCKET="stock-streaming-pipeline-jc"
JOB_NAME="stock-bronze-raw-trades"
ROLE="GlueStreamingRole"
SCRIPT_LOCAL="glue_jobs/bronze_raw_trades.py"
SCRIPT_S3="s3://${BUCKET}/glue-scripts/bronze_raw_trades.py"
STREAM_ARN="arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/stock-trades-stream"

# ── 1. Upload the latest script to S3 ─────────────────────────────────────────
echo "Uploading ${SCRIPT_LOCAL} -> ${SCRIPT_S3}"
aws s3 cp "${SCRIPT_LOCAL}" "${SCRIPT_S3}"

# ── 2. Create the Glue job ────────────────────────────────────────────────────
# "Name": "gluestreaming" is what makes this a Spark Streaming job (not batch ETL).
# --timeout 10 is the infrastructure-level cost safety net — NEVER remove it.
echo "Creating Glue job ${JOB_NAME}"
aws glue create-job \
  --name "${JOB_NAME}" \
  --role "${ROLE}" \
  --command "{\"Name\":\"gluestreaming\",\"ScriptLocation\":\"${SCRIPT_S3}\",\"PythonVersion\":\"3\"}" \
  --default-arguments "{\"--KINESIS_STREAM_ARN\":\"${STREAM_ARN}\",\"--S3_BUCKET\":\"${BUCKET}\",\"--job-language\":\"python\",\"--TempDir\":\"s3://${BUCKET}/glue-temp/\"}" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X" \
  --timeout 10 \
  --region "${REGION}"

echo "Done. Job '${JOB_NAME}' created."

# ──────────────────────────────────────────────────────────────────────────────
# START A RUN (after the job exists):
#   aws glue start-job-run --job-name stock-bronze-raw-trades --region us-east-1
#
# CHECK STATUS:
#   aws glue get-job-runs --job-name stock-bronze-raw-trades --region us-east-1 \
#     --query 'JobRuns[0].[Id,JobRunState]' --output text
#
# STOP A RUN (use the Id from the command above):
#   aws glue batch-stop-job-run --job-name stock-bronze-raw-trades \
#     --job-run-ids <JobRunId> --region us-east-1
#
# UPDATE THE SCRIPT (after editing bronze_raw_trades.py locally):
#   aws s3 cp glue_jobs/bronze_raw_trades.py \
#     s3://stock-streaming-pipeline-jc/glue-scripts/bronze_raw_trades.py
#   # then just start a new run — no need to recreate the job
#
# DELETE THE JOB (rarely needed):
#   aws glue delete-job --job-name stock-bronze-raw-trades --region us-east-1
# ──────────────────────────────────────────────────────────────────────────────

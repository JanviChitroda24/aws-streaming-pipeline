#!/usr/bin/env bash
#
# create_silver_job.sh — register the Silver VWAP Glue Streaming job (Hours 11–13).
#
# Run ONCE to create the job. After it exists, just start a run (see footer),
# or re-upload the script + start a new run after editing it locally.
#
# Prereqs:
#   - Kinesis stream stock-trades-stream is ACTIVE
#   - IAM role GlueStreamingRole exists
#
# Usage:
#   bash glue_jobs/create_silver_job.sh
#
set -euo pipefail

# ── Config (edit here if names/account change) ────────────────────────────────
REGION="us-east-1"
ACCOUNT_ID="366447947905"
BUCKET="stock-streaming-pipeline-jc"
JOB_NAME="stock-silver-vwap"
ROLE="GlueStreamingRole"
SCRIPT_LOCAL="glue_jobs/silver_vwap.py"
SCRIPT_S3="s3://${BUCKET}/glue-scripts/silver_vwap.py"
STREAM_ARN="arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/stock-trades-stream"

# ── 1. Upload the latest script to S3 ─────────────────────────────────────────
echo "Uploading ${SCRIPT_LOCAL} -> ${SCRIPT_S3}"
aws s3 cp "${SCRIPT_LOCAL}" "${SCRIPT_S3}"

# ── 2. Create the Glue job ────────────────────────────────────────────────────
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
# START A RUN:
#   aws glue start-job-run --job-name stock-silver-vwap --region us-east-1
#
# CHECK STATUS:
#   aws glue get-job-runs --job-name stock-silver-vwap --region us-east-1 \
#     --query 'JobRuns[0].[Id,JobRunState]' --output text
#
# STOP A RUN:
#   aws glue batch-stop-job-run --job-name stock-silver-vwap \
#     --job-run-ids <JobRunId> --region us-east-1
#
# UPDATE THE SCRIPT (after editing silver_vwap.py):
#   aws s3 cp glue_jobs/silver_vwap.py \
#     s3://stock-streaming-pipeline-jc/glue-scripts/silver_vwap.py
#   # then start a new run — no need to recreate the job
#
# REGISTER + QUERY IN ATHENA (after silver Parquet exists):
#   aws glue create-crawler --name silver-vwap-1min-crawler --role GlueStreamingRole \
#     --database-name stock_streaming_db --table-prefix silver_ \
#     --targets '{"S3Targets":[{"Path":"s3://stock-streaming-pipeline-jc/silver/vwap_1min/"}]}' \
#     --region us-east-1
#   aws glue start-crawler --name silver-vwap-1min-crawler --region us-east-1
# ──────────────────────────────────────────────────────────────────────────────

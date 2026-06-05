#!/usr/bin/env bash
#
# create_gold_job.sh — register the Gold anomaly-detection Glue Streaming job.
#
# Run ONCE to create the job. After it exists, start a run (see footer) or
# re-upload the script + start a new run after editing it locally.
#
# Usage:
#   bash glue_jobs/create_gold_job.sh
#
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
REGION="us-east-1"
ACCOUNT_ID="366447947905"
BUCKET="stock-streaming-pipeline-jc"
JOB_NAME="stock-gold-anomaly"
ROLE="GlueStreamingRole"
SCRIPT_LOCAL="glue_jobs/gold_anomaly.py"
SCRIPT_S3="s3://${BUCKET}/glue-scripts/gold_anomaly.py"
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
#   aws glue start-job-run --job-name stock-gold-anomaly --region us-east-1
#
# CHECK STATUS:
#   aws glue get-job-runs --job-name stock-gold-anomaly --region us-east-1 \
#     --query 'JobRuns[0].[Id,JobRunState]' --output text
#
# STOP A RUN:
#   aws glue batch-stop-job-run --job-name stock-gold-anomaly \
#     --job-run-ids <JobRunId> --region us-east-1
#
# UPDATE THE SCRIPT (after editing gold_anomaly.py):
#   aws s3 cp glue_jobs/gold_anomaly.py \
#     s3://stock-streaming-pipeline-jc/glue-scripts/gold_anomaly.py
# ──────────────────────────────────────────────────────────────────────────────

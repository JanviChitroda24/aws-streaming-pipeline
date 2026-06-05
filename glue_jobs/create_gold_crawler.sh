#!/usr/bin/env bash
#
# create_gold_crawler.sh — register the gold anomaly data in the Glue Catalog.
#
# Creates crawler gold-anomaly-crawler (table-prefix gold_ → table
# gold_anomaly_alerts), runs it, waits until READY, prints the schema.
#
# Prereq: the gold job has already written Parquet to gold/anomaly_alerts/.
#
# Usage:
#   bash glue_jobs/create_gold_crawler.sh
#
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
REGION="us-east-1"
BUCKET="stock-streaming-pipeline-jc"
ROLE="GlueStreamingRole"
DATABASE="stock_streaming_db"
CRAWLER="gold-anomaly-crawler"
TABLE="gold_anomaly_alerts"
S3_TARGET="s3://${BUCKET}/gold/anomaly_alerts/"

# ── 1. Ensure database exists ─────────────────────────────────────────────────
echo "Ensuring database ${DATABASE} exists"
aws glue create-database \
  --database-input "{\"Name\":\"${DATABASE}\",\"Description\":\"AWS Streaming Pipeline — stock trade data lake\"}" \
  --region "${REGION}" 2>/dev/null || echo "  (database already exists, continuing)"

# ── 2. Create crawler ─────────────────────────────────────────────────────────
echo "Creating crawler ${CRAWLER} (ok if it already exists)"
aws glue create-crawler \
  --name "${CRAWLER}" \
  --role "${ROLE}" \
  --database-name "${DATABASE}" \
  --table-prefix "gold_" \
  --targets "{\"S3Targets\":[{\"Path\":\"${S3_TARGET}\"}]}" \
  --region "${REGION}" 2>/dev/null || echo "  (crawler already exists, continuing)"

# ── 3. Run the crawler ────────────────────────────────────────────────────────
echo "Starting crawler ${CRAWLER}"
aws glue start-crawler --name "${CRAWLER}" --region "${REGION}"

# ── 4. Poll until READY ───────────────────────────────────────────────────────
echo "Waiting for crawler to finish..."
while true; do
  STATE=$(aws glue get-crawler --name "${CRAWLER}" --region "${REGION}" \
            --query 'Crawler.State' --output text)
  echo "  state: ${STATE}"
  [ "${STATE}" = "READY" ] && break
  sleep 10
done

# ── 5. Show the resulting table schema ────────────────────────────────────────
echo "Crawler done. Table ${DATABASE}.${TABLE}:"
aws glue get-table --database-name "${DATABASE}" --name "${TABLE}" --region "${REGION}" \
  --query 'Table.{Name:Name, Columns:StorageDescriptor.Columns[*].Name, Location:StorageDescriptor.Location, PartitionKeys:PartitionKeys[*].Name}'

echo "Done. Now query gold_anomaly_alerts in Athena (db = ${DATABASE})."

# ──────────────────────────────────────────────────────────────────────────────
# RERUN:  aws glue start-crawler --name gold-anomaly-crawler --region us-east-1
# DELETE: aws glue delete-crawler --name gold-anomaly-crawler --region us-east-1
# ──────────────────────────────────────────────────────────────────────────────

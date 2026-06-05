#!/usr/bin/env bash
#
# create_silver_5min_crawler.sh — register the 5-min silver VWAP data in the Catalog.
#
# Creates crawler silver-vwap-5min-crawler (table-prefix silver_ → table
# silver_vwap_5min), runs it, waits until READY, prints the schema.
#
# Prereq: the 5-min silver job has already written Parquet to silver/vwap_5min/.
#
# Usage:
#   bash glue_jobs/create_silver_5min_crawler.sh
#
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
REGION="us-east-1"
BUCKET="stock-streaming-pipeline-jc"
ROLE="GlueStreamingRole"
DATABASE="stock_streaming_db"
CRAWLER="silver-vwap-5min-crawler"
TABLE="silver_vwap_5min"
S3_TARGET="s3://${BUCKET}/silver/vwap_5min/"

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
  --table-prefix "silver_" \
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

echo "Done. Now query silver_vwap_5min in Athena (db = ${DATABASE})."

# ──────────────────────────────────────────────────────────────────────────────
# RERUN:  aws glue start-crawler --name silver-vwap-5min-crawler --region us-east-1
# DELETE: aws glue delete-crawler --name silver-vwap-5min-crawler --region us-east-1
# ──────────────────────────────────────────────────────────────────────────────

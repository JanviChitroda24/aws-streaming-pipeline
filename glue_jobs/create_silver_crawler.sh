#!/usr/bin/env bash
#
# create_silver_crawler.sh — register the silver VWAP data in the Glue Catalog.
#
# WHAT THIS DOES:
#   1. Creates a Glue Crawler  silver-vwap-1min-crawler  (points at silver/vwap_1min/)
#   2. Runs it — scans the Parquet files, infers schema, creates table
#      silver_vwap_1min  in database stock_streaming_db
#   3. Waits until READY, then prints the resulting table schema
#
# Prereq: the silver job has already written Parquet to silver/vwap_1min/.
# (Database stock_streaming_db is assumed to exist from create_crawler.sh; if not,
#  this script creates it.)
#
# Run ONCE to create + run. On later sessions just rerun the crawler to pick up
# new data / schema changes (e.g. after adding the `source` column) — see "RERUN".
#
# Usage:
#   bash glue_jobs/create_silver_crawler.sh
#
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
REGION="us-east-1"
BUCKET="stock-streaming-pipeline-jc"
ROLE="GlueStreamingRole"
DATABASE="stock_streaming_db"
CRAWLER="silver-vwap-1min-crawler"
TABLE="silver_vwap_1min"
S3_TARGET="s3://${BUCKET}/silver/vwap_1min/"

# ── 1. Ensure database exists (ignore error if it already does) ───────────────
echo "Ensuring database ${DATABASE} exists"
aws glue create-database \
  --database-input "{\"Name\":\"${DATABASE}\",\"Description\":\"AWS Streaming Pipeline — stock trade data lake\"}" \
  --region "${REGION}" 2>/dev/null || echo "  (database already exists, continuing)"

# ── 2. Create crawler (ignore error if it already exists) ─────────────────────
# table-prefix silver_ → the crawler names the table silver_vwap_1min
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

echo "Done. Now query silver_vwap_1min in Athena (db = ${DATABASE})."

# ──────────────────────────────────────────────────────────────────────────────
# RERUN (later sessions — crawler exists, just pick up new data / schema changes):
#   aws glue start-crawler --name silver-vwap-1min-crawler --region us-east-1
#
# CHECK STATE:
#   aws glue get-crawler --name silver-vwap-1min-crawler --region us-east-1 --query 'Crawler.State'
#
# NOTE: if you changed the silver schema (e.g. added `source`), rerun this crawler
# so the table picks up the new column.
#
# DELETE (rarely needed):
#   aws glue delete-crawler --name silver-vwap-1min-crawler --region us-east-1
# ──────────────────────────────────────────────────────────────────────────────

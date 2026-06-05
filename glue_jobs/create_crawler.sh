#!/usr/bin/env bash
#
# create_crawler.sh — register the bronze data in the Glue Data Catalog.
#
# WHAT THIS DOES:
#   1. Creates the Glue database  stock_streaming_db
#   2. Creates a Glue Crawler     raw-trades-crawler  (points at bronze/raw_trades/)
#   3. Runs the crawler — it scans the Parquet files, infers schema + partitions,
#      and creates the table  bronze_raw_trades  in the catalog
#   4. Waits until the crawler is READY, then prints the resulting table schema
#
# After this, Athena can run SQL over the bronze data.
#
# Run ONCE to create db + crawler. On later sessions you only need step 3
# (rerun the crawler) to pick up new partitions — see "RERUN" at the bottom.
#
# Usage:
#   bash glue_jobs/create_crawler.sh
#
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
REGION="us-east-1"
BUCKET="stock-streaming-pipeline-jc"
ROLE="GlueStreamingRole"
DATABASE="stock_streaming_db"
CRAWLER="raw-trades-crawler"
TABLE="bronze_raw_trades"
S3_TARGET="s3://${BUCKET}/bronze/raw_trades/"

# ── 1. Create database (ignore error if it already exists) ────────────────────
echo "Creating database ${DATABASE} (ok if it already exists)"
aws glue create-database \
  --database-input "{\"Name\":\"${DATABASE}\",\"Description\":\"AWS Streaming Pipeline — stock trade data lake\"}" \
  --region "${REGION}" 2>/dev/null || echo "  (database already exists, continuing)"

# ── 2. Create crawler (ignore error if it already exists) ─────────────────────
# table-prefix bronze_ → the crawler names the table bronze_raw_trades
echo "Creating crawler ${CRAWLER} (ok if it already exists)"
aws glue create-crawler \
  --name "${CRAWLER}" \
  --role "${ROLE}" \
  --database-name "${DATABASE}" \
  --table-prefix "bronze_" \
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

echo "Done. Now set the Athena query result location and run docs/athena_validation_queries.sql"

# ──────────────────────────────────────────────────────────────────────────────
# RERUN (later sessions — db + crawler already exist, just pick up new data):
#   aws glue start-crawler --name raw-trades-crawler --region us-east-1
#
# CHECK STATE:
#   aws glue get-crawler --name raw-trades-crawler --region us-east-1 --query 'Crawler.State'
#
# DELETE (rarely needed):
#   aws glue delete-crawler  --name raw-trades-crawler  --region us-east-1
#   aws glue delete-database --name stock_streaming_db  --region us-east-1
# ──────────────────────────────────────────────────────────────────────────────

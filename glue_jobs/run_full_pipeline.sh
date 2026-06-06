#!/usr/bin/env bash
#
# run_full_pipeline.sh — Hour 12 capstone: run all 4 jobs TOGETHER
#                        (bronze + silver-1min + silver-5min + gold).
#
# WHY: each layer reads Kinesis independently. To cross-validate (batch
# reconciliation AND 1-min vs 5-min) all layers must have data from the SAME
# time period — so run all four jobs against ONE producer session.
#
# WHAT THIS DOES:
#   1. (optional) clears each job's checkpoint so they reprocess fresh data
#   2. starts the 4 streaming jobs (bronze, silver-1min, silver-5min, gold)
#   3. prints their states
#
# YOU run the producer + crawlers + queries + cleanup separately (see footer).
# This script does NOT recreate Kinesis or start the producer.
#
# Usage:
#   bash glue_jobs/run_full_pipeline.sh           # start the 3 jobs
#   CLEAR_CHECKPOINTS=1 bash glue_jobs/run_full_pipeline.sh   # also wipe checkpoints first
#
set -euo pipefail

REGION="us-east-1"
BUCKET="stock-streaming-pipeline-jc"
# All 4 streaming jobs — run together so every layer has overlapping-time data
# (enables batch reconciliation AND the 1-min vs 5-min comparison query).
JOBS=("stock-bronze-raw-trades" "stock-silver-vwap" "stock-silver-vwap-5min" "stock-gold-anomaly")
CHECKPOINTS=("bronze-raw" "silver-vwap-1min" "silver-vwap-5min" "gold-anomaly")

# ── 1. (optional) clear checkpoints so jobs reprocess from fresh ──────────────
if [ "${CLEAR_CHECKPOINTS:-0}" = "1" ]; then
  echo "Clearing checkpoints..."
  for cp in "${CHECKPOINTS[@]}"; do
    echo "  rm glue-checkpoints/${cp}/"
    aws s3 rm "s3://${BUCKET}/glue-checkpoints/${cp}/" --recursive || true
  done
fi

# ── 2. start all 3 streaming jobs ─────────────────────────────────────────────
echo "Starting jobs..."
for job in "${JOBS[@]}"; do
  RUN_ID=$(aws glue start-job-run --job-name "${job}" --region "${REGION}" \
             --query 'JobRunId' --output text)
  echo "  ${job} → run ${RUN_ID}"
done

# ── 3. report states (give them a few seconds to register) ────────────────────
sleep 5
echo "States:"
for job in "${JOBS[@]}"; do
  STATE=$(aws glue get-job-runs --job-name "${job}" --region "${REGION}" \
            --query 'JobRuns[0].JobRunState' --output text)
  echo "  ${job}: ${STATE}"
done

echo "Done. Re-run the 'States' loop in ~2 min — all four should be RUNNING."

# ──────────────────────────────────────────────────────────────────────────────
# FULL HOUR-12 SEQUENCE (run these around this script):
#
# 1. Recreate Kinesis:
#    aws kinesis create-stream --stream-name stock-trades-stream --shard-count 1 --region us-east-1
#
# 2. Start producer (10 min) in its own terminal:
#    cd src && python3 producer.py --mode simulated --eps 20 --duration 600
#
# 3. Start the jobs (this script):
#    CLEAR_CHECKPOINTS=1 bash glue_jobs/run_full_pipeline.sh
#
# 4. Wait 7-8 min. Check CloudWatch /aws-glue/jobs/output. Lambda should email on gold writes.
#
# 5. Re-crawl so Athena sees the new overlapping data (all 4 tables):
#    aws glue start-crawler --name raw-trades-crawler        --region us-east-1
#    aws glue start-crawler --name silver-vwap-1min-crawler  --region us-east-1
#    aws glue start-crawler --name silver-vwap-5min-crawler  --region us-east-1
#    aws glue start-crawler --name gold-anomaly-crawler      --region us-east-1
#
# 6. Run Athena Query 3 (batch reconciliation) + Query 6 (1-min vs 5-min) — now they have overlap.
#
# 7. CLEANUP — stop nothing needed (10-min timeout), but delete the stream:
#    aws kinesis delete-stream --stream-name stock-trades-stream --region us-east-1
#    # confirm jobs finished:
#    for j in stock-bronze-raw-trades stock-silver-vwap stock-silver-vwap-5min stock-gold-anomaly; do
#      aws glue get-job-runs --job-name "$j" --region us-east-1 --query 'JobRuns[0].JobRunState' --output text
#    done
#
# COST: 4 jobs × 2 workers × $0.44/DPU-hr × (10/60) hr ≈ $0.59 + Kinesis ~$0.01.
# ──────────────────────────────────────────────────────────────────────────────

# End of Session Checklist

Run these checks every time you finish a work session. Takes 2 minutes. Prevents surprise AWS bills.

---

## 1. Stop Glue Jobs

```bash
# List any running Glue jobs
aws glue get-job-runs --job-name stock-stream-reader-test --region us-east-1 \
  --query 'JobRuns[?JobRunState==`RUNNING`].JobRunId'

aws glue get-job-runs --job-name stock-bronze-raw-trades --region us-east-1 \
  --query 'JobRuns[?JobRunState==`RUNNING`].JobRunId'

aws glue get-job-runs --job-name stock-silver-vwap --region us-east-1 \
  --query 'JobRuns[?JobRunState==`RUNNING`].JobRunId'

aws glue get-job-runs --job-name stock-gold-anomaly --region us-east-1 \
  --query 'JobRuns[?JobRunState==`RUNNING`].JobRunId'
```

All outputs should be `[]`. If any show a job run ID, stop it:

```bash
aws glue batch-stop-job-run \
  --job-name <job-name> \
  --job-run-ids <job-run-id> \
  --region us-east-1
```

Or stop from console: **Glue → Jobs → select job → Actions → Stop run**

---

## 2. Delete Kinesis Stream

```bash
# Check if stream exists
aws kinesis list-streams --region us-east-1
# Safe to delete: should show "StreamNames": ["stock-trades-stream"]

# Delete it
aws kinesis delete-stream --stream-name stock-trades-stream --region us-east-1

# Confirm it's gone (wait ~10 seconds, then run)
aws kinesis list-streams --region us-east-1
# Should show: "StreamNames": []
```

Recreating tomorrow takes 30 seconds:
```bash
aws kinesis create-stream --stream-name stock-trades-stream --shard-count 1 --region us-east-1
```

---

## 3. Stop Local Producer

```bash
# If producer.py is still running in a terminal, Ctrl+C it
# Verify nothing is sending to Kinesis:
ps aux | grep producer.py
# Should show only the grep process itself, not a running python process
```

---

## 4. Final Verification

```bash
# All streams deleted
aws kinesis list-streams --region us-east-1
# Expected: "StreamNames": []

# No running Glue jobs (quick visual check)
# Console: Glue → Jobs → check "Last run status" column — no RUNNING entries
```

---

## 5. Check Billing (once per day)

```
AWS Console → Billing → Bills → This month
```

Expected daily spend ranges:
- Day 1–3 (setup + Kinesis + short Glue runs): < $1.00/day
- Day 4–5 (Glue streaming tests): < $2.00/day
- Total project budget: $15–18

If you see unexpected charges, check:
1. Glue → Jobs — any jobs still running?
2. Kinesis → Data Streams — any streams still active?

---

## Cost Reference (per hour while running)

| Service | Cost/hr | Notes |
|---|---|---|
| Kinesis (1 shard) | $0.015 | Charges even when idle |
| Glue G.1X × 2 workers | $0.88 | Only charges while job is RUNNING |
| S3 | ~$0 | Negligible at our scale |
| Lambda / SNS / Athena | ~$0 | Pay-per-use, tiny at our scale |

---

## Start of Next Session Checklist

```bash
# 1. Re-create Kinesis stream
aws kinesis create-stream --stream-name stock-trades-stream --shard-count 1 --region us-east-1

# 2. Wait ~10 seconds, verify ACTIVE
aws kinesis describe-stream-summary --stream-name stock-trades-stream --region us-east-1

# 3. Activate venv + load env
source .venv/bin/activate
source .env && export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION

# 4. Run verification scripts to confirm everything still works
python3 tests/verify_01.py
python3 tests/verify_02.py

# 5. You're ready to continue
```

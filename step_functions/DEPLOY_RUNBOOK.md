# Step Functions — Deploy & Execution Runbook (Hour 13)

Ordered runbook for deploying the full orchestration: 3 helper Lambdas + the
`pipeline_definition.json` state machine. Console steps + CLI. Revisit this for
the exact order — the sequence matters (Lambdas must exist & be tested before the
state machine references them).

Files: `lambda/{infra_check,run_crawlers,data_quality}.py`, `step_functions/pipeline_definition.json`.

---

## Part 1 — Create the 3 Lambda functions

For each: Lambda → Create function → Author from scratch → Python 3.11 → x86_64 →
paste the code into `lambda_function.py` → **Deploy** → attach role policies → set timeout.

| # | Function | Code | Role policies | Timeout |
|---|----------|------|---------------|---------|
| 1 | `stock-infra-check`   | `infra_check.py`  | `AmazonKinesisReadOnlyAccess`, `AmazonS3ReadOnlyAccess` | 30s |
| 2 | `stock-run-crawlers`  | `run_crawlers.py` | `AWSGlueConsoleFullAccess` + inline `glue:*` (name `GlueFullAccess`) | 5 min (300s) |
| 3 | `stock-data-quality`  | `data_quality.py` | `AmazonAthenaFullAccess`, `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess` | 3 min (180s) |

Inline `glue:*` policy (Lambda 2):
```json
{ "Version": "2012-10-17",
  "Statement": [ { "Effect": "Allow", "Action": "glue:*", "Resource": "*" } ] }
```

To attach policies: Configuration → Permissions → click the Role name → IAM → Add permissions → Attach policies.
Set timeout: Configuration → General configuration → Edit.

---

## Part 2 — Test each Lambda individually (before wiring the state machine)

```bash
# infra check needs the stream to exist:
aws kinesis create-stream --stream-name stock-trades-stream --shard-count 1 --region us-east-1
```

Lambda → Test tab → new event `{}`:

| Lambda | Expected result | Notes |
|---|---|---|
| `stock-infra-check`  | `"status": "ALL_CHECKS_PASSED"` | fast |
| `stock-run-crawlers` | `"status": "ALL_CRAWLERS_READY"` | takes 1–2 min |
| `stock-data-quality` | `"status": "ALL_DQ_CHECKS_PASSED"` | needs bronze table populated |

---

## Part 3 — Create the state machine

1. Step Functions → Create state machine → **Write your workflow in code** → Standard
2. Paste `step_functions/pipeline_definition.json`
3. Confirm the visual graph: `Author_Janvi_Chitroda` → `CheckInfrastructure` → `ProcessStreams` (4 parallel) → `RunCrawlers` → `DataQualityChecks` → `NotifySuccess` (+ `NotifyFailure` catch)
4. Name `stock-streaming-pipeline` → Create a new role → Create

**Add permissions to the auto-created Step Functions role** (IAM → Roles → search `stock-streaming-pipeline` → Attach policies):
- `AWSGlueConsoleFullAccess`
- `AmazonSNSFullAccess`
- `AWSLambda_FullAccess`
- inline `iam:PassRole`:
```json
{ "Version": "2012-10-17",
  "Statement": [ { "Effect": "Allow", "Action": "iam:PassRole", "Resource": "*" } ] }
```

---

## Part 4 — Full end-to-end execution

```bash
# 1. clear all 4 checkpoints (fresh reprocess)
for cp in bronze-raw silver-vwap-1min silver-vwap-5min gold-anomaly; do
  aws s3 rm s3://stock-streaming-pipeline-jc/glue-checkpoints/$cp/ --recursive
done

# 2. producer (10 min) — terminal 1
python3 src/producer.py --mode simulated --eps 20 --duration 600

# 3. trigger the state machine — terminal 2
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:366447947905:stateMachine:stock-streaming-pipeline \
  --region us-east-1
```

4. Watch: Step Functions → stock-streaming-pipeline → Executions → click the running execution. Each box turns green as it completes.
5. Full run ~15–20 min (Glue cold start + 10-min job timeout + crawlers + DQ). Expect email "✅ Stock Pipeline — Complete Success".
6. Screenshot the green graph for the README.

---

## Part 5 — Cleanup + commit

```bash
aws kinesis delete-stream --stream-name stock-trades-stream --region us-east-1

# confirm no jobs running
for j in stock-bronze-raw-trades stock-silver-vwap stock-silver-vwap-5min stock-gold-anomaly; do
  echo "$j:" && aws glue get-job-runs --job-name "$j" --region us-east-1 --query 'JobRuns[0].JobRunState' --output text
done

git add -A
git commit -m "13: Step Functions — full production orchestration with infra check, crawlers, automated DQ"
git push origin main
```

---

## Why this order matters

- **Lambdas before state machine** — the state machine references the 3 functions by name; they must exist (and be tested) first or the execution fails at the first `lambda:invoke`.
- **Test each Lambda with `{}` first** — isolates IAM/permission problems per function before the orchestrated run, where a failure is harder to attribute.
- **Producer before start-execution** — jobs read Kinesis at `LATEST`; data must be flowing when `ProcessStreams` starts.
- **`iam:PassRole`** on the Step Functions role — required because Step Functions passes a role to Glue when it calls `startJobRun`.

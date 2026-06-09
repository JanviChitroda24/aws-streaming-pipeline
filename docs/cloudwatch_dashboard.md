# CloudWatch Dashboard — `StockStreamingPipeline`

A single-page health view of the pipeline, built from **AWS built-in metrics**
(no custom instrumentation added to the Glue jobs). Recreate it in the console
via **CloudWatch → Dashboards → `StockStreamingPipeline` → Add widget**.

| # | Widget | Type | Source metric(s) | What it answers |
|---|--------|------|------------------|-----------------|
| 1 | Title card | Text | — | Pipeline + settings banner |
| 2 | Kinesis — Incoming Records | Line | `AWS/Kinesis` → `IncomingRecords` (StreamName `stock-trades-stream`) | Is the producer sending data? |
| 3 | Kinesis — Incoming Bytes | Line | `AWS/Kinesis` → `IncomingBytes` | Approaching the 1 MB/s shard limit? |
| 4 | Glue — Resource Usage | Line | `Glue` → `ResourceUsage` (4 jobs) | Are workers overloaded (>80%)? |
| 5 | Lambda — Invocations | Number | `AWS/Lambda` → `Invocations` (4 fns) | Did the notifier fire? Did SFN call all Lambdas? |
| 6 | Lambda — Errors | Number | `AWS/Lambda` → `Errors` (4 fns) | Any Lambda broken? (should be 0) |
| 7 | Lambda — Duration | Line | `AWS/Lambda` → `Duration` (4 fns) | Lambdas slowing toward timeout? |

**Functions monitored (widgets 5–7):** `stock-anomaly-notifier`, `stock-infra-check`,
`stock-run-crawlers`, `stock-data-quality`.

**Glue jobs monitored (widget 4):** `stock-bronze-raw-trades`, `stock-silver-vwap`,
`stock-silver-vwap-5min`, `stock-gold-anomaly`.

### Title card markdown (widget 1)

```markdown
# 📊 Stock Streaming Pipeline — Live Monitor
**Architecture:** Producer → Kinesis → Glue Streaming (Bronze/Silver/Gold) → S3 → Lambda → SNS

| Setting | Value |
|---|---|
| Tickers | 25 mega-cap stocks |
| Throughput | 20 events/sec |
| Micro-batch | 30 seconds |
| Watermark | 10 seconds |
| Anomaly threshold | ±1% deviation |
| Orchestrator | Step Functions |
```

### Suggested layout

```
Row 1: [ Title card — full width ]
Row 2: [ Kinesis Records ]      [ Kinesis Bytes ]
Row 3: [ Glue Resource Usage — full width ]
Row 4: [ Lambda Invocations ]   [ Lambda Errors ]   [ Lambda Duration ]
```

### Notes

- **Sparse data is expected.** The jobs run ~10 min per session, not 24/7, so widgets
  read `--` / "No data" when idle. Set the dashboard time range to **1w** to see prior
  runs; during a live run the charts populate ~3–4 min after start.
- **Built-in metrics only** — no `cloudwatch.put_metric_data()` in the Glue jobs.
  Custom per-layer metrics (e.g. `BronzeRecordsWritten`, `AnomaliesDetected`) are a
  possible enhancement but would require editing and re-uploading every Glue job.
- **Live-run order:** open the dashboard → create Kinesis → start producer →
  start Step Functions → watch widgets populate.

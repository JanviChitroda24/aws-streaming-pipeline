# AWS Real-Time Stock Trading Pipeline

**A production-grade real-time streaming pipeline built on AWS, processing 25 stock tickers through a medallion architecture with automated orchestration, data quality validation, and email alerting.**

**Stack:** Kinesis → Glue Streaming (PySpark) → S3 (Parquet) → Athena → Lambda → SNS → Step Functions

**Companion project:** [Kafka + Spark Streaming Pipeline](https://github.com/JanviChitroda24/kafka-spark-streaming), same domain, same logic, local infrastructure. This project translates those skills to AWS-native services.

---

## Architecture

```
                                    ┌──────────────────────────────────────────────────┐
                                    │            Step Functions Orchestrator            │
                                    │   (Infra Check → Glue Jobs → Crawlers → DQ)      │
                                    └──────────────────────┬───────────────────────────┘
                                                           │ orchestrates
                                                           ▼
┌──────────┐    ┌─────────────┐    ┌──────────────────────────────────────────────────┐
│  Trade   │───▶│   Kinesis    │───▶│              AWS Glue Streaming (PySpark)         │
│ Producer │    │  Data Stream │    │                                                    │
│ (Python) │    │  (1 shard)   │    │  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌────┐ │
└──────────┘    └─────────────┘    │  │ Bronze  │  │Silver    │  │Silver    │  │Gold│ │
  25 tickers                        │  │ Raw     │  │1-Min     │  │5-Min     │  │Ano-│ │
  20 events/sec                     │  │ Trades  │  │VWAP      │  │VWAP      │  │maly│ │
                                    │  └────┬────┘  └────┬─────┘  └────┬─────┘  └─┬──┘ │
                                    └───────┼───────────┼────────────┼──────────┼──────┘
                                            │           │            │          │
                                            ▼           ▼            ▼          ▼
                                    ┌──────────────────────────────────────────────────┐
                                    │                Amazon S3 (Parquet)                │
                                    │  bronze/raw_trades/   silver/vwap_1min/           │
                                    │  silver/vwap_5min/    gold/anomaly_alerts/        │
                                    └──────────┬──────────────────────────┬─────────────┘
                                               │                          │
                                    ┌──────────▼──────────┐    ┌─────────▼──────────┐
                                    │   Glue Data Catalog  │    │   Lambda + SNS     │
                                    │   (Schema Registry)  │    │   (Email Alerts)   │
                                    └──────────┬──────────┘    └────────────────────┘
                                               │                  S3 PutObject trigger
                                    ┌──────────▼──────────┐    → reads anomaly Parquet
                                    │   Amazon Athena      │    → summarizes per ticker
                                    │   (SQL Analytics)    │    → sends alert email
                                    └─────────────────────┘
```

---

## Why This Project Exists

I built a [Kafka + Spark Streaming pipeline](https://github.com/JanviChitroda24/kafka-spark-streaming) using local infrastructure (Redpanda, Spark, Delta Lake, Dagster). This project rebuilds the same pipeline on AWS to demonstrate that I can translate data engineering skills across platforms. The business logic is identical; what changes is the infrastructure layer.

### Kafka → AWS Migration Map

| Local Stack (Project 03) | AWS Equivalent (This Project) | Trade-off |
|---|---|---|
| Redpanda (Kafka) | Kinesis Data Streams | Less control, zero ops, native IAM |
| Spark Structured Streaming | Glue Streaming (PySpark) | Same API, managed workers, pay-per-DPU |
| Delta Lake on local disk | S3 Parquet + Glue Catalog | No ACID transactions, but serverless SQL via Athena |
| Dagster orchestration | Step Functions state machine | Visual workflow, native AWS integration, pay-per-transition |
| Console output alerts | Lambda + SNS email alerts | Event-driven, zero-polling, automatic triggers |
| Manual DQ in notebooks | Automated Athena DQ in Lambda | Pipeline fails if DQ checks fail, no silent bad data |

---

## Key Metrics Computed

**VWAP (Volume-Weighted Average Price)** at two granularities:
- **1-minute windows**: real-time monitoring, anomaly detection baseline
- **5-minute windows**: trend analysis, institutional trading benchmark

**VWAP Formula:** `sum(price × quantity) / sum(quantity)`. This weights trades by volume so large institutional trades have proportionally more influence than small retail trades.

**Anomaly Detection:** Flags trades where price deviates >1% from the per-ticker batch average. Detected within 30 seconds of the trade occurring.

**Additional metrics per window:** total_volume, trade_count, low_price, high_price, buy_ratio (fraction of buy vs sell trades).

---

## Verification Results

| Test | Result | Method |
|---|---|---|
| Batch reconciliation | **0.0% diff** across ALL rows | Recomputed VWAP from bronze via batch GROUP BY, joined with streaming silver, identical to 4 decimal places |
| Duplicate check | **0 duplicates** | `GROUP BY trade_id HAVING COUNT(*) > 1` returns empty |
| Data quality (7 checks) | **0 violations** | null_trade_id, null_ticker, negative_price, zero_quantity, invalid_side, null_timestamp, duplicate_ids |
| Pipeline latency | **~11.76 seconds** avg | Measured as `ingested_at - event_time` across all bronze records |
| Anomaly detection | **~2-5%** of trades flagged | Consistent with ±0.2% simulator noise vs 1% threshold |
| Step Functions orchestration | **All steps green** | Infra check → 4 parallel Glue jobs → crawlers → DQ → success notification |

The batch reconciliation result (0.0% difference) is the strongest validation. It proves the streaming VWAP formula, watermark handling, and data completeness are mathematically correct.

---

## Pipeline Components

### Producer (`src/producer.py`)
Dual-mode trade producer: simulated random-walk pricing for 25 mega-cap tickers, or live Finnhub WebSocket feed. Sends to Kinesis via `put_records` batch API with exponential backoff retry. 20 events/sec sustained throughput.

### Bronze Layer (`glue_jobs/bronze_raw_trades.py`)
Reads from Kinesis, parses JSON with dynamic column detection (handles Glue version differences), adds `event_time` + `ingested_at` timestamps, partitions by `year/month/day/ticker`, writes to S3 as Parquet.

### Silver Layer: 1-Min VWAP (`glue_jobs/silver_vwap.py`)
Computes VWAP over 1-minute tumbling windows per ticker with 10-second watermarks for late data handling. Outputs: window_start, window_end, ticker, vwap, total_volume, trade_count, low_price, high_price, buy_ratio, source.

### Silver Layer: 5-Min VWAP (`glue_jobs/silver_vwap_5min.py`)
Identical computation with 5-minute windows. ~5x more trades per window for smoother trend analysis.

### Gold Layer: Anomaly Detection (`glue_jobs/gold_anomaly.py`)
Uses foreachBatch self-join: computes per-ticker average within each 30-second micro-batch, joins back to individual trades, flags deviations >1%. Only anomalies written to S3, a massive data reduction.

### Lambda Alerts (`lambda/anomaly_notifier.py`)
S3 PutObject trigger on `gold/anomaly_alerts/`. Reads Parquet with pandas, summarizes per ticker, publishes to SNS. Email delivered within 2-5 seconds of file creation.

### Step Functions (`step_functions/pipeline_definition.json`)
Full production orchestrator: CheckInfrastructure (Lambda) → ProcessStreams (4 parallel Glue jobs) → RunCrawlers (Lambda) → DataQualityChecks (Lambda) → NotifySuccess/Failure (SNS). Every step has error catching with failure notification.

### Monitoring (`docs/cloudwatch_dashboard.md`)
A CloudWatch dashboard (`StockStreamingPipeline`) gives single-page pipeline health from AWS built-in metrics, with no custom instrumentation in the jobs. Seven widgets cover **Kinesis** (incoming records/bytes: is data flowing? near the shard limit?), **Glue** (resource usage across all 4 jobs), and **Lambda** (invocations, errors, duration for all 4 functions). Combined with SNS email alerts (anomalies + Step Functions success/failure) and CloudWatch logs for per-batch detail, this provides layered observability across the pipeline.

---

## Monitoring & Observability

Three complementary layers:

| Layer | Mechanism | Surfaces |
|---|---|---|
| **At-a-glance health** | CloudWatch dashboard (`docs/cloudwatch_dashboard.md`) | Kinesis throughput, Glue resource usage, Lambda invocations/errors/duration |
| **Event alerts** | Lambda + SNS email | Anomaly summaries (per-ticker), Step Functions success/failure |
| **Deep investigation** | CloudWatch Logs (`/aws-glue/jobs/output`) | Per-batch record counts, sample rows, stack traces |

The dashboard is built from **built-in AWS metrics**. Kinesis, Glue, and Lambda emit these automatically, so monitoring required no changes to the streaming jobs.

---

## AWS Services Used

| Service | Purpose | Cost Model |
|---|---|---|
| Kinesis Data Streams | Real-time event ingestion (1 shard) | $0.015/shard-hour |
| AWS Glue Streaming | PySpark streaming jobs (4 jobs × 2 workers) | $0.44/DPU-hour |
| Amazon S3 | Medallion data lake (bronze/silver/gold) | $0.023/GB-month |
| Glue Data Catalog | Schema registry (4 tables, 4 crawlers) | Free (first 1M objects) |
| Amazon Athena | Serverless SQL analytics | $5/TB scanned |
| AWS Lambda | Event-driven alerting + DQ checks (4 functions) | Free (first 1M requests) |
| Amazon SNS | Email notifications | Free (first 1,000 emails) |
| Step Functions | Pipeline orchestration | $0.025/1,000 transitions |
| CloudWatch | Logs and monitoring | Free tier |
| IAM | Access control (2 roles, 1 user) | Free |

**Total project cost: ~$5-8** (using $100 AWS credits)

---

## Project Structure

```
aws-streaming-pipeline/
├── src/
│   ├── config.py               # Centralized configuration
│   ├── trade_simulator.py      # Random walk price generator for 25 tickers
│   ├── producer.py             # Dual-mode Kinesis producer (simulated/Finnhub)
│   └── verify_kinesis.py       # Quick Kinesis connectivity check
├── glue_jobs/
│   ├── stream_reader_test.py   # Kinesis → CloudWatch smoke test
│   ├── bronze_raw_trades.py    # Raw trades → S3 Parquet
│   ├── silver_vwap.py          # 1-min VWAP with watermarks
│   ├── silver_vwap_5min.py     # 5-min VWAP
│   ├── gold_anomaly.py         # Anomaly detection (foreachBatch self-join)
│   ├── create_bronze_job.sh    # Create-job helpers (upload script + register Glue job)
│   ├── create_silver_job.sh
│   ├── create_silver_5min_job.sh
│   ├── create_gold_job.sh
│   ├── create_crawler.sh       # Crawler helpers (create + run + poll READY)
│   ├── create_silver_crawler.sh
│   ├── create_silver_5min_crawler.sh
│   ├── create_gold_crawler.sh
│   └── run_full_pipeline.sh    # Run all 4 jobs together off one producer
├── lambda/
│   ├── anomaly_notifier.py     # S3 trigger → SNS email alert
│   ├── infra_check.py          # Verify Kinesis + S3 before pipeline run
│   ├── run_crawlers.py         # Start + poll all 4 crawlers
│   └── data_quality.py         # Automated Athena DQ checks
├── step_functions/
│   ├── pipeline_definition.json  # Full orchestration state machine
│   └── DEPLOY_RUNBOOK.md         # Ordered deploy + execution runbook
├── docs/
│   ├── athena_validation_queries.sql
│   └── athena_analytics_queries.sql
├── tests/
│   ├── verify_01.py            # Hour 1: setup / config / S3 checks
│   ├── verify_02.py            # Hour 2: Kinesis stream health
│   └── verify_03.py            # Hour 3: producer + schema round-trip
├── requirements.txt
└── README.md
```

---

## How to Run

### Prerequisites
- AWS account with $100 credits
- AWS CLI configured with `kinesis-producer-user` credentials
- Python 3.11+ with boto3, finnhub-python

### Quick Start (Step Functions, one command)

```bash
# 1. Create Kinesis stream
aws kinesis create-stream --stream-name stock-trades-stream --shard-count 1 --region us-east-1

# 2. Clear checkpoints
aws s3 rm s3://stock-streaming-pipeline-jc/glue-checkpoints/ --recursive

# 3. Start producer (10 minutes)
python3 src/producer.py --mode simulated --eps 20 --duration 600

# 4. Execute the full pipeline via Step Functions
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:366447947905:stateMachine:stock-streaming-pipeline \
  --region us-east-1

# 5. Watch execution in Step Functions console, all steps should turn green
# 6. Check email for "✅ Stock Pipeline: Complete Success" notification
# 7. Query results in Athena (database: stock_streaming_db)

# 8. Cleanup
aws kinesis delete-stream --stream-name stock-trades-stream --region us-east-1
```

> **Note:** the 4 Glue jobs run with a **15-minute timeout** while `awaitTermination` is 10 minutes. The job stops itself gracefully (status `SUCCEEDED`) before the timeout kill switch, which is what lets Step Functions proceed past `ProcessStreams`. See `step_functions/DEPLOY_RUNBOOK.md`.

### Manual Run (individual jobs)

```bash
# Start producer
python3 src/producer.py --mode simulated --eps 20 --duration 300

# Start individual Glue jobs
aws glue start-job-run --job-name stock-bronze-raw-trades --region us-east-1
aws glue start-job-run --job-name stock-silver-vwap --region us-east-1
aws glue start-job-run --job-name stock-silver-vwap-5min --region us-east-1
aws glue start-job-run --job-name stock-gold-anomaly --region us-east-1
```

---

## Technical Decisions & Trade-offs

| Decision | Why | Alternative Considered |
|---|---|---|
| Kinesis Provisioned (1 shard) | Predictable cost at low volume ($0.015/hr) | On-Demand (2-3x more expensive for our throughput) |
| Glue Streaming over EMR | Zero cluster management, pay-per-use | EMR (cheaper at scale, more operational burden) |
| foreachBatch for anomaly detection | 30s latency, low memory, reliable on 2 workers | Stream-stream join (textbook but resource-heavy) |
| 10-second watermark | Balances late data inclusion vs memory usage | 60s (catches more late data, higher latency) |
| 1% anomaly threshold | Catches ~2-5% of trades with simulator noise | 2% (too rare with ±0.2% gaussian noise) |
| S3 Parquet over Delta Lake | Native Athena support, no extra dependencies | Delta Lake on S3 (ACID but needs Spark for reads) |
| Glue Catalog over Hive Metastore | Managed, integrates with Athena/Glue natively | Standalone Hive (more control, operational overhead) |
| Glue timeout 15 min > awaitTermination 10 min | Job stops gracefully (`SUCCEEDED`) before the kill switch, so Step Functions proceeds | Timeout = awaitTermination (job killed as `TIMEOUT` = orchestrator failure) |

---

## License
All rights reserved. See [LICENSE](LICENSE) for usage terms.

---

*Built by Janvi Chitroda · MS Information Systems, Northeastern University*
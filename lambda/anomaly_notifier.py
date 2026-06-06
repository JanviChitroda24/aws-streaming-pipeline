"""
anomaly_notifier.py — Lambda: S3 event → read anomaly Parquet → SNS email alert.

TRIGGER:
  S3 PutObject event on gold/anomaly_alerts/ prefix.
  Every time the Glue gold job writes a new Parquet file, this Lambda fires.

WHAT IT DOES:
  1. Receives S3 event (bucket + key of the new file)
  2. Reads the Parquet file with pandas (via AWSSDKPandas layer)
  3. Summarizes anomalies per ticker (count, avg deviation, max deviation)
  4. Publishes summary to SNS topic → email alert

WHY LAMBDA (not Glue sending SNS directly):
  Separation of concerns — Glue owns stream processing, Lambda owns the
  notification side effect. Event-driven: S3 PutObject triggers Lambda, no
  polling, no wasted compute, near-zero latency between file write and email.

COST: Effectively free. Lambda free tier = 1M requests/month; ~20 invocations
  per pipeline run. SNS email free for the first 1,000 messages.

ENVIRONMENT VARIABLES (set in Lambda console):
  SNS_TOPIC_ARN: arn:aws:sns:us-east-1:366447947905:stock-anomaly-alerts

LAMBDA SETTINGS:
  Runtime:  Python 3.11
  Handler:  lambda_function.lambda_handler  (paste this code into lambda_function.py
            in the console editor, or set the handler to anomaly_notifier.lambda_handler)
  Memory:   256 MB
  Timeout:  30 seconds
  Layer:    AWSSDKPandas-Python311 (provides pandas + pyarrow)
  Trigger:  S3 → bucket: stock-streaming-pipeline-jc
            → prefix: gold/anomaly_alerts/ → suffix: .parquet
            → event: s3:ObjectCreated:*
"""

import os
from io import BytesIO

import boto3
import pandas as pd

s3 = boto3.client("s3")
sns = boto3.client("sns")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):
    """
    Triggered by S3 PutObject on gold/anomaly_alerts/.
    Reads the new Parquet, summarizes per ticker, sends an SNS alert.
    """
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # Skip folder markers / empty objects (Spark sometimes writes _SUCCESS etc.)
        if key.endswith("/") or record["s3"]["object"].get("size", 0) == 0:
            continue

        print(f"Processing: s3://{bucket}/{key}")

        # Read the Parquet file
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
        except Exception as e:
            print(f"Error reading Parquet: {e}")
            return {"statusCode": 500, "body": str(e)}

        if df.empty:
            print("Empty file — no alert needed.")
            return {"statusCode": 200, "body": "No anomalies"}

        # Summarize anomalies per ticker
        summary = df.groupby("ticker").agg(
            count=("trade_id", "count"),
            avg_deviation=("deviation_pct", "mean"),
            max_deviation=("deviation_pct", "max"),
            avg_price=("price", "mean"),
            avg_window_price=("window_avg_price", "mean"),
        ).reset_index()

        total_anomalies = len(df)
        tickers_affected = len(summary)

        # Build the alert message
        message = (
            f"Hi Janvi, We detected {total_anomalies} anomalous trades across {tickers_affected} tickers in the latest batch.\n\n"
            f"🚨 STOCK PRICE ANOMALY ALERT\n"
            f"{'='*50}\n\n"
            f"Total anomalies:   {total_anomalies}\n"
            f"Tickers affected:  {tickers_affected}\n"
            f"Source file:       {key}\n\n"
            f"{'─'*50}\n"
            f"Per-Ticker Breakdown:\n"
            f"{'─'*50}\n\n"
        )

        for _, row in summary.iterrows():
            direction_hint = "above" if row["avg_price"] > row["avg_window_price"] else "below"
            message += (
                f"  {row['ticker']}:\n"
                f"    Anomalies:      {row['count']}\n"
                f"    Avg deviation:  {row['avg_deviation']:.2%}\n"
                f"    Max deviation:  {row['max_deviation']:.2%}\n"
                f"    Trade price:    ${row['avg_price']:.2f} "
                f"({direction_hint} avg ${row['avg_window_price']:.2f})\n\n"
            )

        message += (
            f"{'─'*50}\n"
            f"Pipeline: aws-streaming-pipeline\n"
            f"Layer: Gold (anomaly detection)\n"
            f"Threshold: ±1% from batch average\n"
        )

        # SNS subject is capped at 100 chars
        subject = f"🚨 Stock Anomaly Alert — {total_anomalies} trades flagged across {tickers_affected} tickers"
        if len(subject) > 100:
            subject = f"Stock Anomaly Alert — {total_anomalies} trades flagged"

        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
        print(f"✅ Alert sent: {total_anomalies} anomalies across {tickers_affected} tickers")

    return {"statusCode": 200, "body": f"Processed {len(event.get('Records', []))} events"}

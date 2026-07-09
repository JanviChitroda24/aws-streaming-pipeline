# Author: Janvi Chitroda | github.com/JanviChitroda24
"""Centralized config — all AWS settings in one place."""
import os
from dotenv import load_dotenv
load_dotenv()

# AWS
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "stock-trades-stream")
S3_BUCKET = os.getenv("S3_BUCKET")

# S3 paths
S3_BRONZE_RAW = f"s3://{S3_BUCKET}/bronze/raw_trades/"
S3_SILVER_VWAP_1MIN = f"s3://{S3_BUCKET}/silver/vwap_1min/"
S3_SILVER_VWAP_5MIN = f"s3://{S3_BUCKET}/silver/vwap_5min/"
S3_GOLD_ANOMALY = f"s3://{S3_BUCKET}/gold/anomaly_alerts/"
S3_CHECKPOINT_BASE = f"s3://{S3_BUCKET}/glue-checkpoints/"
S3_TEMP_DIR = f"s3://{S3_BUCKET}/glue-temp/"

# Stream settings
TICKERS = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","BRK-B","JPM",
    "JNJ","V","PG","UNH","HD","MA","DIS","PYPL","BAC","NFLX","ADBE",
    "CRM","INTC","VZ","CMCSA","PEP"
]
ANOMALY_THRESHOLD_PCT = 0.02  # +/-2% from window average
WATERMARK_SECONDS = 10
VWAP_1MIN_SECONDS = 60
VWAP_5MIN_SECONDS = 300

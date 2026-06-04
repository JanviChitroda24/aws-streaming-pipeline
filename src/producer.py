"""
producer.py — Kinesis trade producer (simulated + Finnhub dual-mode).

Sends stock trade events to Kinesis Data Streams using the batch PutRecords API.
Supports two data sources that produce identical JSON schemas:

  simulated — Synthetic trades from trade_simulator.py. No API key needed.
              Use for development, testing, and demos. Works 24/7.

  finnhub   — Real market trades via Finnhub WebSocket. Requires FINNHUB_API_KEY.
              Use to prove the pipeline handles real data. Market hours only.

Same dual-mode pattern as our Kafka project (Project 03) — downstream Glue jobs
parse one schema regardless of which producer ran.

Usage:
  cd src
  python producer.py --mode simulated --eps 50 --duration 120
  python producer.py --mode finnhub --duration 300

Cost note:
  Kinesis PUTs cost $0.014 per 1M payload units (25KB each).
  At 50 eps × ~250 bytes = 12.5 KB/s → ~0.5 units/s → effectively $0.
  The shard itself ($0.015/hr) is the dominant cost — not the PUT calls.
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import boto3

# Allow running standalone from src/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import AWS_REGION, KINESIS_STREAM_NAME, TICKERS
from trade_simulator import simulate_stream

# ── Graceful shutdown ────────────────────────────────────────────────────────
# Ctrl+C sets this flag. The main loop checks it between batches so the
# current batch finishes cleanly before exit — no partial writes.
_shutdown = False


def _handle_sigint(sig, frame):
    global _shutdown
    print("\n🛑 Shutdown requested — finishing current batch before exit...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_sigint)

# ── Kinesis client ────────────────────────────────────────────────────────────
kinesis = boto3.client("kinesis", region_name=AWS_REGION)


def put_records_batch(records: list[dict], max_retries: int = 3) -> tuple[int, int]:
    """
    Send a batch of trade records to Kinesis using the PutRecords API.

    Why PutRecords (batch) instead of PutRecord (single)?
      - PutRecord: one HTTP round trip per record → 100 records = 100 round trips
      - PutRecords: one HTTP round trip per 500 records → 100 records = 1 round trip
      - Batch API is ~100x more efficient at our throughput

    Kinesis limits per PutRecords call:
      - Max 500 records
      - Max 5 MB total payload
      - Max 1 MB per individual record
    Our records are ~250 bytes each → 500 × 250B = 125KB → well under both limits.

    Partial failure handling:
      PutRecords can partially succeed — some records go through, others fail
      (usually due to shard throughput throttling). The response includes a
      FailedRecordCount and per-record error codes. We retry failed records
      with exponential backoff (1s → 2s → 4s), which is the production pattern.

    Args:
        records: Trade event dicts to send (each will be JSON-serialized)
        max_retries: Retry attempts for failed records (default 3)

    Returns:
        tuple (successful_count, failed_count)
    """
    total_sent = 0
    total_failed = 0

    # Chunk into 500-record slices to respect the Kinesis per-call limit
    for chunk_start in range(0, len(records), 500):
        chunk = records[chunk_start : chunk_start + 500]

        # Build Kinesis record format: Data (bytes) + PartitionKey (string)
        # PartitionKey = ticker → same ticker always lands on the same shard
        # → ordering per ticker guaranteed (same as Kafka message key)
        kinesis_records = [
            {
                "Data": json.dumps(r).encode("utf-8"),
                "PartitionKey": r["ticker"],
            }
            for r in chunk
        ]

        pending = kinesis_records
        for attempt in range(max_retries):
            resp = kinesis.put_records(
                StreamName=KINESIS_STREAM_NAME,
                Records=pending,
            )
            failed_count = resp.get("FailedRecordCount", 0)

            if failed_count == 0:
                # All records succeeded — move on
                total_sent += len(pending)
                break

            # Isolate only the failed records for retry.
            # Succeeded records are already durably stored — don't re-send them.
            pending = [
                pending[i]
                for i, result in enumerate(resp["Records"])
                if "ErrorCode" in result
            ]
            total_sent += len(chunk) - failed_count

            if attempt < max_retries - 1:
                # Exponential backoff: 1s, 2s, 4s
                # Gives Kinesis time to recover from transient throttling
                wait_sec = 2 ** attempt
                print(
                    f"  ⚠️  {failed_count} records failed "
                    f"(attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait_sec}s..."
                )
                time.sleep(wait_sec)
            else:
                total_failed += failed_count
                print(
                    f"  ❌ {failed_count} records dropped after {max_retries} attempts"
                )

    return total_sent, total_failed


def run_simulated(eps: int, duration: int) -> None:
    """
    Run the trade simulator and push events to Kinesis at the target rate.

    Calls simulate_stream() which yields one batch of `eps` records per second.
    Each batch is sent as a single PutRecords call.

    Args:
        eps: Events per second (controls batch size and data rate)
        duration: How long to run in seconds
    """
    print(f"🚀 Simulated mode | {eps} eps × {duration}s = ~{eps * duration:,} records")
    print(f"   Stream: {KINESIS_STREAM_NAME} | Region: {AWS_REGION}")
    print(f"   Press Ctrl+C to stop early (finishes current batch)\n")

    total_sent = 0
    total_failed = 0
    start_time = time.time()
    batch_num = 0

    for batch in simulate_stream(eps, duration):
        if _shutdown:
            break

        sent, failed = put_records_batch(batch)
        total_sent += sent
        total_failed += failed
        batch_num += 1

        elapsed = time.time() - start_time
        actual_rate = total_sent / elapsed if elapsed > 0 else 0
        print(
            f"  ✅ Batch {batch_num:>3}: {sent} sent | "
            f"Total: {total_sent:>6,} | "
            f"Rate: {actual_rate:>5.0f}/s | "
            f"Elapsed: {elapsed:.0f}s"
        )

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"  📊 Producer Summary")
    print(f"     Total sent:   {total_sent:,}")
    print(f"     Total failed: {total_failed:,}")
    print(f"     Duration:     {elapsed:.1f}s")
    print(f"     Avg rate:     {total_sent / elapsed:.0f} records/sec")
    print(f"{'='*60}")


def run_finnhub(duration: int) -> None:
    """
    Connect to Finnhub WebSocket and forward real trades to Kinesis.

    Subscribes to the first 10 tickers (Finnhub free tier limit).
    Buffers incoming trades and flushes to Kinesis every 50 records
    to keep latency low while still batching for efficiency.

    Schema differences vs simulator:
      - side = "unknown" (Finnhub free tier doesn't provide aggressor side)
      - bid_price / ask_price = None (not available on free tier)
      - source = "finnhub_live" (allows filtering in Glue jobs)
      - exchange = "Finnhub" (not exchange-specific on free tier)

    Args:
        duration: How long to run in seconds
    """
    try:
        import websocket
    except ImportError:
        print("❌ websocket-client not installed. Run: pip install websocket-client")
        sys.exit(1)

    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        print("❌ FINNHUB_API_KEY not set in .env")
        sys.exit(1)

    buffer: list[dict] = []
    total_sent = 0
    start_time = time.time()

    # Subscribe to first 10 tickers — Finnhub free tier rate-limits subscriptions
    subscribed = TICKERS[:10]
    print(f"🚀 Finnhub mode | real market data for {duration}s")
    print(f"   Stream: {KINESIS_STREAM_NAME}")
    print(f"   Tickers: {', '.join(subscribed)}")
    print(f"   Note: Only works during US market hours (9:30 AM–4:00 PM ET)\n")

    def on_message(ws, message):
        nonlocal buffer, total_sent

        data = json.loads(message)
        if data.get("type") != "trade":
            return

        for t in data.get("data", []):
            record = {
                # Finnhub trade fields: s=symbol, p=price, v=volume, t=timestamp(ms)
                "trade_id": f"fh-{t['t']}-{t['s']}",
                "ticker": t["s"],
                "price": t["p"],
                "quantity": t["v"],
                "side": "unknown",        # not available on free tier
                "trade_type": "market",
                "bid_price": None,        # not available on free tier
                "ask_price": None,        # not available on free tier
                "timestamp": datetime.fromtimestamp(
                    t["t"] / 1000, tz=timezone.utc
                ).isoformat(),
                "exchange": "Finnhub",
                "source": "finnhub_live",
            }
            buffer.append(record)

            # Flush buffer every 50 records — balances latency vs API efficiency
            if len(buffer) >= 50:
                sent, _ = put_records_batch(buffer.copy())
                total_sent += sent
                elapsed = time.time() - start_time
                print(f"  ✅ Sent {sent} real trades | Total: {total_sent:,} | {elapsed:.0f}s")
                buffer.clear()

        # Auto-stop after duration
        if time.time() - start_time > duration:
            ws.close()

    def on_open(ws):
        for ticker in subscribed:
            ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
        print(f"  📡 Subscribed to {len(subscribed)} tickers — waiting for trades...")

    def on_error(ws, error):
        print(f"  ❌ WebSocket error: {error}")

    def on_close(ws, close_status, close_msg):
        nonlocal total_sent
        # Flush any remaining buffered records before closing
        if buffer:
            sent, _ = put_records_batch(buffer.copy())
            total_sent += sent
            buffer.clear()
        elapsed = time.time() - start_time
        print(f"\n📊 Finnhub Summary: {total_sent:,} records in {elapsed:.1f}s")

    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={api_key}",
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kinesis trade producer — simulated or real Finnhub data"
    )
    parser.add_argument(
        "--mode",
        choices=["simulated", "finnhub"],
        default="simulated",
        help="Data source: simulated (default) or finnhub (real market data, market hours only)",
    )
    parser.add_argument(
        "--eps",
        type=int,
        default=50,
        help="Events per second — simulated mode only (default: 50)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Run duration in seconds (default: 60)",
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Kinesis Trade Producer")
    print(f"  Mode: {args.mode} | Stream: {KINESIS_STREAM_NAME}")
    print(f"{'='*60}\n")

    if args.mode == "simulated":
        run_simulated(args.eps, args.duration)
    else:
        run_finnhub(args.duration)

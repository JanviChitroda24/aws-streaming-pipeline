"""
verify_03.py — Hour 3: Trade Producer verification.

Checks:
  1. All modules import cleanly (config, trade_simulator, producer)
  2. generate_trade() returns all required schema fields with correct types
  3. bid_price < price < ask_price (spread is valid)
  4. simulate_stream() yields batches at the expected rate
  5. Kinesis stream is ACTIVE before attempting writes
  6. put_records_batch() successfully writes 5 test records
  7. Records read back from the stream with correct schema
  8. Record size is within Kinesis limits + cost estimate printed

Prerequisites:
  - verify_01.py and verify_02.py must pass
  - Kinesis stream must be ACTIVE

Run from project root (venv active):
  source .env && export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION
  python3 tests/verify_03.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

# Make src/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# ── Console formatting ────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

_passed = 0
_failed = 0


def check(description: str, condition: bool, fix_hint: str = "") -> bool:
    global _passed, _failed
    if condition:
        print(f"  {GREEN}✅ PASS{RESET}  {description}")
        _passed += 1
    else:
        print(f"  {RED}❌ FAIL{RESET}  {description}")
        if fix_hint:
            print(f"           {YELLOW}💡 Fix: {fix_hint}{RESET}")
        _failed += 1
    return condition


def section(title: str):
    print(f"\n{BOLD}{title}{RESET}")


# ─── 1. Module imports ────────────────────────────────────────────────────────
section("1. Module Imports")

try:
    from config import TICKERS, KINESIS_STREAM_NAME, AWS_REGION
    check("config.py imports", True)
except ImportError as e:
    check("config.py imports", False, str(e))
    sys.exit(1)

try:
    from trade_simulator import generate_trade, simulate_stream, PRICE_RANGES
    check("trade_simulator.py imports", True)
except ImportError as e:
    check("trade_simulator.py imports", False, f"Check src/trade_simulator.py exists: {e}")
    sys.exit(1)

try:
    from producer import put_records_batch
    check("producer.py imports", True)
except ImportError as e:
    check("producer.py imports", False, f"Check src/producer.py exists: {e}")
    sys.exit(1)


# ─── 2. Trade schema validation ───────────────────────────────────────────────
section("2. Trade Schema Validation (generate_trade)")

# The schema must match exactly what downstream Glue jobs expect to parse
REQUIRED_FIELDS = [
    "trade_id", "ticker", "price", "quantity", "side",
    "trade_type", "bid_price", "ask_price", "timestamp",
    "exchange", "source",
]

trade = generate_trade("AAPL")

check(
    f"All {len(REQUIRED_FIELDS)} required fields present",
    all(f in trade for f in REQUIRED_FIELDS),
    f"Missing: {[f for f in REQUIRED_FIELDS if f not in trade]}",
)
check(
    f"ticker = 'AAPL'",
    trade.get("ticker") == "AAPL",
)
check(
    f"price is a positive float (got: {trade.get('price')})",
    isinstance(trade.get("price"), float) and trade["price"] > 0,
)
check(
    f"quantity is a positive int (got: {trade.get('quantity')})",
    isinstance(trade.get("quantity"), int) and trade["quantity"] > 0,
)
check(
    f"side is 'buy' or 'sell' (got: '{trade.get('side')}')",
    trade.get("side") in ("buy", "sell"),
)
check(
    f"source = 'simulator' (got: '{trade.get('source')}')",
    trade.get("source") == "simulator",
)
# Spread validity: bid < price < ask — ensures simulated market data is coherent
check(
    f"bid < price < ask ({trade.get('bid_price')} < {trade.get('price')} < {trade.get('ask_price')})",
    (trade.get("bid_price", 0) < trade.get("price", 0) < trade.get("ask_price", 0)),
)
# Timestamp must parse as ISO 8601 — Glue uses to_timestamp() on this field
try:
    datetime.fromisoformat(trade["timestamp"])
    check("timestamp parses as ISO 8601", True)
except (ValueError, KeyError):
    check("timestamp parses as ISO 8601", False, "Must be ISO 8601 UTC string")

check(
    f"Price ranges defined for all {len(TICKERS)} tickers",
    all(t in PRICE_RANGES for t in TICKERS),
    f"Missing: {[t for t in TICKERS if t not in PRICE_RANGES]}",
)


# ─── 3. Random walk — price continuity ───────────────────────────────────────
section("3. Random Walk Price Continuity")

# Generate 5 consecutive AAPL trades and verify prices stay correlated.
# If prices were pure random, consecutive trades could jump wildly.
# Random walk keeps them within ~1% of each other per tick.
prices = [generate_trade("AAPL")["price"] for _ in range(5)]
max_jump_pct = max(
    abs(prices[i] - prices[i - 1]) / prices[i - 1] * 100
    for i in range(1, len(prices))
)
check(
    f"5 consecutive AAPL prices stay within 2% of each other (max jump: {max_jump_pct:.3f}%)",
    max_jump_pct < 2.0,
    "Random walk should produce ±0.2% per tick — larger jumps suggest broken state",
)


# ─── 4. Batch generation ─────────────────────────────────────────────────────
section("4. Batch Generation (simulate_stream)")

# Run simulator for 2 seconds at 10 eps — expect 2 batches of 10 records each
batches_collected = []
for batch in simulate_stream(eps=10, duration=2):
    batches_collected.append(batch)

check(
    f"simulate_stream yielded {len(batches_collected)} batch(es) in 2s",
    len(batches_collected) >= 1,
)
check(
    f"Each batch has 10 records (got: {[len(b) for b in batches_collected]})",
    all(len(b) == 10 for b in batches_collected),
)

# Verify ticker distribution — with 100 records we should see several tickers
big_batch = next(iter(simulate_stream(eps=100, duration=1)))
unique_tickers = set(r["ticker"] for r in big_batch)
check(
    f"100-record batch covers multiple tickers ({len(unique_tickers)} unique tickers seen)",
    len(unique_tickers) >= 5,
    "Random sampling should distribute across 25 tickers",
)


# ─── 5. Kinesis stream health check ──────────────────────────────────────────
section("5. Kinesis Stream Status")

import boto3
from botocore.exceptions import ClientError

kinesis = boto3.client("kinesis", region_name=AWS_REGION)

try:
    resp = kinesis.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
    status = resp["StreamDescriptionSummary"]["StreamStatus"]
    stream_ok = check(
        f"Stream '{KINESIS_STREAM_NAME}' is ACTIVE (status: {status})",
        status == "ACTIVE",
        "Create the stream first: aws kinesis create-stream --stream-name stock-trades-stream --shard-count 1 --region us-east-1",
    )
except ClientError as e:
    stream_ok = check("Stream exists", False, str(e))

if not stream_ok:
    print(f"\n  {RED}Cannot run write/read tests without an active stream. Exiting.{RESET}\n")
    sys.exit(1)


# ─── 6. Kinesis write test ────────────────────────────────────────────────────
section("6. Kinesis Write (put_records_batch)")

# Send 5 records — one for each of the top 5 tickers
test_trades = [generate_trade(t) for t in ["AAPL", "MSFT", "GOOGL", "NVDA", "AMZN"]]
try:
    sent, failed_count = put_records_batch(test_trades)
    check(
        f"put_records_batch sent {sent}/5 records",
        sent == 5 and failed_count == 0,
        f"{failed_count} records failed — check shard throughput or IAM permissions",
    )
except Exception as e:
    check("put_records_batch", False, str(e))


# ─── 7. Schema round-trip: write → read → validate ───────────────────────────
section("7. Schema Round-Trip (write → Kinesis → read → validate)")

# Use LATEST shard iterator so we only see records sent after this point
shard_iter = kinesis.get_shard_iterator(
    StreamName=KINESIS_STREAM_NAME,
    ShardId="shardId-000000000000",
    ShardIteratorType="LATEST",
)["ShardIterator"]

# Send a uniquely-tagged marker record so we can identify it in the read
marker = generate_trade("TSLA")
marker["trade_id"] = f"verify-03-{int(time.time())}"
marker["source"] = "verify_03"

kinesis.put_record(
    StreamName=KINESIS_STREAM_NAME,
    Data=json.dumps(marker).encode("utf-8"),
    PartitionKey="TSLA",
)

# Brief wait — Kinesis propagation is typically <1s but we give it 2s to be safe
time.sleep(2)

records_raw = kinesis.get_records(ShardIterator=shard_iter, Limit=20)["Records"]
check(
    f"Read back {len(records_raw)} record(s) from stream",
    len(records_raw) >= 1,
    "Records may not have propagated yet — try running again",
)

if records_raw:
    # Validate schema on the first record read back
    read_trade = json.loads(records_raw[0]["Data"])

    check(
        f"All {len(REQUIRED_FIELDS)} schema fields survive JSON round-trip",
        all(f in read_trade for f in REQUIRED_FIELDS),
        f"Missing after round-trip: {[f for f in REQUIRED_FIELDS if f not in read_trade]}",
    )
    check(
        f"price type preserved as numeric after JSON (type: {type(read_trade.get('price')).__name__})",
        isinstance(read_trade.get("price"), (int, float)),
    )
    check(
        f"quantity type preserved as int after JSON (type: {type(read_trade.get('quantity')).__name__})",
        isinstance(read_trade.get("quantity"), int),
    )


# ─── 8. Record size + cost estimate ──────────────────────────────────────────
section("8. Record Size + Cost Estimate")

sample_bytes = len(json.dumps(generate_trade("AAPL")).encode("utf-8"))
check(
    f"Record size: {sample_bytes} bytes (Kinesis max: 1,048,576 bytes per record)",
    sample_bytes < 1_048_576,
)

# Cost math at our target throughput
target_eps = 50
run_minutes = 5
total_records = target_eps * run_minutes * 60
total_bytes = total_records * sample_bytes
put_units = total_bytes / 25_000          # 1 PUT unit = 25 KB
cost_usd = put_units * 0.014 / 1_000_000 # $0.014 per 1M units

print(f"           📊 Cost at {target_eps} eps × {run_minutes} min:")
print(f"              Records:   {total_records:,}")
print(f"              Data:      {total_bytes / 1024:.1f} KB")
print(f"              PUT units: {put_units:.1f}")
print(f"              PUT cost:  ${cost_usd:.6f} (negligible)")
print(f"              Shard cost: $0.015/hr (this is the real cost)")


# ─── Cost reminder ────────────────────────────────────────────────────────────
print(f"\n{YELLOW}⚠️  COST REMINDER: Kinesis stream is running at $0.015/shard/hour.{RESET}")
print(f"{YELLOW}   If done for the day:{RESET}")
print(f"{YELLOW}   aws kinesis delete-stream --stream-name {KINESIS_STREAM_NAME} --region us-east-1{RESET}")
print(f"{YELLOW}   If continuing to Hour 4: keep it running.{RESET}")


# ─── Summary ──────────────────────────────────────────────────────────────────
print(f"\n{BOLD}{'='*60}{RESET}")
total = _passed + _failed
if _failed == 0:
    print(f"  {GREEN}{BOLD}✅ ALL {total} CHECKS PASSED — Producer is working. Ready for Hour 4 (Glue).{RESET}")
else:
    print(f"  {YELLOW}⚠️  {_passed}/{total} passed, {_failed} failed — fix failures before Hour 4.{RESET}")
print(f"{BOLD}{'='*60}{RESET}\n")

sys.exit(0 if _failed == 0 else 1)

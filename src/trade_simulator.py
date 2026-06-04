"""
trade_simulator.py — Synthetic stock trade event generator.

Generates realistic trade events for 25 mega-cap tickers without needing
a real market data feed. Used by producer.py in simulated mode.

Key design decisions:
  - Random walk pricing (not pure random) so consecutive trades for the
    same ticker have correlated prices — closer to real market microstructure.
  - Bid-ask spread narrows/widens slightly each trade to simulate liquidity.
  - All 25 tickers sampled uniformly so every ticker gets coverage.

Schema matches our Kafka project (Project 03) exactly — downstream Glue jobs
parse the same JSON structure regardless of whether data comes from the
simulator or Finnhub.
"""

import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

# Allow running standalone from src/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import TICKERS

# Base price band per ticker — gives each stock a realistic starting range.
# AAPL starts at $100-$115, MSFT at $115-$130, etc.
# In production these would come from a reference data service.
PRICE_RANGES = {t: (100 + i * 15, 150 + i * 15) for i, t in enumerate(TICKERS)}

# Mutable state: tracks the last traded price per ticker.
# Persists across calls so the random walk is continuous — price at tick N
# depends on price at tick N-1, not a fresh random sample.
_last_price: dict[str, float] = {}


def generate_trade(ticker: str) -> dict:
    """
    Generate a single realistic trade event for the given ticker.

    Pricing uses a Gaussian random walk (±0.2% std dev per tick).
    This means AAPL won't jump from $150 to $200 in one tick — it drifts
    gradually, just like real tick-by-tick price movement.

    Args:
        ticker: Stock symbol from TICKERS list (e.g., "AAPL")

    Returns:
        dict matching the unified trade schema:
            trade_id   — UUID, unique per event
            ticker     — stock symbol
            price      — current trade price (random walk from last)
            quantity   — shares traded (1–500)
            side       — "buy" or "sell"
            trade_type — "market" or "limit"
            bid_price  — best bid (price - small spread)
            ask_price  — best ask (price + small spread)
            timestamp  — UTC ISO 8601
            exchange   — hardcoded "NASDAQ" for simulator
            source     — "simulator" (lets Glue distinguish from real data)
    """
    # Seed the random walk on first call for this ticker
    if ticker not in _last_price:
        low, high = PRICE_RANGES[ticker]
        _last_price[ticker] = random.uniform(low, high)

    # Random walk: ±0.2% Gaussian — tight enough to stay realistic,
    # wide enough to generate variation across a 5-minute run
    price = round(_last_price[ticker] * random.gauss(1, 0.002), 2)
    _last_price[ticker] = price

    quantity = random.randint(1, 500)

    # Spread: $0.01–$0.05 cents. High-volume tickers would be tighter
    # in a more realistic simulator, but uniform spread is fine here.
    spread = random.uniform(0.01, 0.05)

    return {
        "trade_id": str(uuid.uuid4()),
        "ticker": ticker,
        "price": price,
        "quantity": quantity,
        "side": random.choice(["buy", "sell"]),
        "trade_type": random.choice(["market", "limit"]),
        "bid_price": round(price - spread, 2),
        "ask_price": round(price + spread, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "exchange": "NASDAQ",
        "source": "simulator",
    }


def simulate_stream(eps: int = 50, duration: int = 120):
    """
    Generator that yields one batch of trade events per second.

    Each batch contains `eps` records randomly distributed across all 25 tickers.
    The generator sleeps for the remainder of each second after building the batch,
    so it runs at approximately `eps` events/second wall-clock rate.

    Args:
        eps: Events per second (default 50). Each second yields one batch of this size.
        duration: Total duration in seconds (default 120). Generator stops after this.

    Yields:
        list[dict] — one batch of trade events (len == eps)

    Example:
        for batch in simulate_stream(eps=20, duration=30):
            send_to_kinesis(batch)   # 20 records/sec for 30 seconds = 600 total
    """
    end_time = time.time() + duration

    while time.time() < end_time:
        batch_start = time.time()

        batch = [generate_trade(random.choice(TICKERS)) for _ in range(eps)]
        yield batch

        # Sleep for the remainder of the second to maintain the target rate.
        # If batch generation took 0.05s, sleep 0.95s. If it took >1s (overloaded
        # system), don't sleep — just yield the next batch immediately.
        elapsed = time.time() - batch_start
        sleep_time = max(0, 1.0 - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

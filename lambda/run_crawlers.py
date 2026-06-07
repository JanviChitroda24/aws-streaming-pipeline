"""
run_crawlers.py — Lambda: start all 4 Glue crawlers and wait until they finish.

ROLE IN THE PIPELINE:
  Runs AFTER the Glue jobs write new Parquet. Crawlers refresh the Glue Data
  Catalog so Athena (and the downstream DQ check) see the new data/partitions.

RETURNS: {"status": "ALL_CRAWLERS_READY", "crawlers": [...]}
RAISES:  Exception if crawlers don't reach READY within the wait window.

IAM (execution role): AWSGlueConsoleFullAccess (or inline glue:*)
TIMEOUT: 5 min (300s) — crawlers take time; this Lambda polls until READY.
"""

import time

import boto3

REGION = "us-east-1"
CRAWLERS = [
    "raw-trades-crawler",
    "silver-vwap-1min-crawler",
    "silver-vwap-5min-crawler",
    "gold-anomaly-crawler",
]

glue = boto3.client("glue", region_name=REGION)


def lambda_handler(event, context):
    # ── Start every crawler (tolerate ones already running) ───────────────────
    started = []
    for crawler_name in CRAWLERS:
        try:
            glue.start_crawler(Name=crawler_name)
            print(f"✅ Started crawler: {crawler_name}")
            started.append(crawler_name)
        except glue.exceptions.CrawlerRunningException:
            print(f"⚠️ Crawler {crawler_name} already running, will still wait on it")
            started.append(crawler_name)
        except Exception as e:
            print(f"❌ Failed to start {crawler_name}: {e}")
            raise

    # ── Poll until all are READY (cap below the Lambda timeout) ───────────────
    max_wait = 270  # seconds — stay under the 300s Lambda timeout
    start_time = time.time()

    while time.time() - start_time < max_wait:
        states = {c: glue.get_crawler(Name=c)["Crawler"]["State"] for c in started}
        if all(state == "READY" for state in states.values()):
            print(f"\n✅ All {len(started)} crawlers READY")
            return {"status": "ALL_CRAWLERS_READY", "crawlers": started}
        print(f"  waiting… {states}")
        time.sleep(15)

    raise Exception(f"Crawlers did not all reach READY within {max_wait}s: {states}")

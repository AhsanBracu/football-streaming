"""Measure end-to-end latency while a replay runs.

Latency = when an updated window reaches us - when the producer sent the
newest event in that window (last_sent_at). It covers the whole pipeline:
producer -> Kafka -> Spark -> MongoDB -> Change Stream.

Start this, then the producer; press Ctrl+C about 30 s after the producer has
finished. The summary is printed and appended to evaluation/results.csv:

    python3 evaluation/latency.py --label "1 match, speed 10"
"""
import argparse
import csv
import statistics
import time
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient

# Our MongoDB container, seen from the Mac (Homebrew's mongod has 27017).
MONGO_URI = "mongodb://localhost:27018/?directConnection=true"
RESULTS = Path(__file__).parent / "results.csv"


def summarize(latencies_ms):
    """Median, 95th percentile and max, in seconds."""
    s = sorted(x / 1000 for x in latencies_ms)
    # "inclusive": stay within the measured values (the default can extrapolate above the max)
    p95 = statistics.quantiles(s, n=20, method="inclusive")[18] if len(s) >= 2 else s[0]
    return {"updates": len(s), "median_s": round(statistics.median(s), 1),
            "p95_s": round(p95, 1), "max_s": round(s[-1], 1)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True, help='e.g. "1 match, speed 10"')
    args = parser.parse_args()

    windows = MongoClient(MONGO_URI).football.window_stats
    latencies = []
    print(f"[{args.label}] watching window_stats, start the producer now (Ctrl+C to stop)")
    try:
        # Spark replaces whole documents, so each change carries the full window.
        with windows.watch() as changes:
            for change in changes:
                w = change.get("fullDocument")
                if not w:
                    continue
                latencies.append(time.time() * 1000 - w["last_sent_at"])
                if len(latencies) % 100 == 0:
                    print(f"  {len(latencies)} updates, last latency {latencies[-1] / 1000:.1f} s")
    except KeyboardInterrupt:
        pass

    if not latencies:
        print("no updates seen: was Spark running, and did the producer start after this?")
        return
    result = {"label": args.label, **summarize(latencies), "time": datetime.now().strftime("%Y-%m-%d %H:%M")}
    print("\n" + "  ".join(f"{k}={v}" for k, v in result.items()))

    new_file = not RESULTS.exists()
    with RESULTS.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(result))
        if new_file:
            writer.writeheader()
        writer.writerow(result)
    print(f"appended to {RESULTS}")


if __name__ == "__main__":
    main()

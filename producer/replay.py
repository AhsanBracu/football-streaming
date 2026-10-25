"""Replay StatsBomb matches from HDFS into Kafka, as if they were happening live.

Each event becomes one Kafka message on the topic "football-events", keyed by
match_id so every match stays in order inside one partition.

    docker compose run --rm producer                          # all matches, 30x speed
    docker compose run --rm producer --speed 60 --match 3857271
"""
import argparse
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from confluent_kafka import Producer

# Addresses inside the Docker network (see docker-compose.yml).
HDFS_URL = os.environ.get("HDFS_URL", "http://namenode:9870/webhdfs/v1")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
EVENTS_DIR = "/statsbomb/events"
TOPIC = "football-events"


# ---------- Reading from HDFS (WebHDFS is plain HTTP) ----------

def hdfs_list(path):
    """File names in an HDFS folder."""
    r = requests.get(f"{HDFS_URL}{path}", params={"op": "LISTSTATUS"})
    r.raise_for_status()
    return [f["pathSuffix"] for f in r.json()["FileStatuses"]["FileStatus"]]


def hdfs_read_json(path):
    """Read a JSON file from HDFS. The namenode answers with a redirect to the
    datanode that holds the data; requests follows it automatically."""
    r = requests.get(f"{HDFS_URL}{path}", params={"op": "OPEN"})
    r.raise_for_status()
    return r.json()


# ---------- The match clock ----------

def to_seconds(timestamp):
    """'00:07:15.957' -> 435.957"""
    h, m, s = timestamp.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)


def with_elapsed_time(events):
    """Pair each event with its elapsed match time in seconds.

    StatsBomb's timestamp restarts at 00:00 every half, and "minute" jumps back
    to 45 after a long first half (England-Iran: 1st half ends at minute 59).
    Spark's windows need a clock that only moves forward, so we add up the
    lengths of the earlier halves. Half-time itself is skipped.
    """
    period_length = {}
    for e in events:
        p = e["period"]
        period_length[p] = max(period_length.get(p, 0.0), to_seconds(e["timestamp"]))

    period_start, total = {}, 0.0
    for p in sorted(period_length):
        period_start[p] = total
        total += period_length[p]

    return [(period_start[e["period"]] + to_seconds(e["timestamp"]), e) for e in events]


# ---------- Replaying ----------

def send(producer, key, value):
    """Send one message. If the producer's buffer is full (very high speeds),
    wait for Kafka to catch up and try again."""
    while True:
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=report_error)
            producer.poll(0)  # handle delivery reports from earlier messages
            return
        except BufferError:
            producer.poll(0.5)


def report_error(err, msg):
    if err is not None:
        print(f"Delivery failed for match {msg.key().decode()}: {err}")


def replay_match(match_id, producer, speed, kickoff, progress):
    events = with_elapsed_time(hdfs_read_json(f"{EVENTS_DIR}/{match_id}.json"))
    start = time.monotonic()

    for i, (elapsed, event) in enumerate(events, 1):
        # Wait until this event's moment comes. Planning from the start time
        # (instead of sleeping the gap after each send) means small delays
        # don't add up over the match. Events at the same moment go together.
        wait = start + elapsed / speed - time.monotonic()
        if wait > 0:
            time.sleep(wait)

        message = {
            "match_id": match_id,
            # The clock Spark's windows use: match time, not divided by speed.
            "event_time": (kickoff + timedelta(seconds=elapsed))
                          .isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "match_minute": event["minute"],              # for display
            "sent_at": int(time.time() * 1000),           # for measuring latency
            "event": event,                               # the original, unchanged
        }
        send(producer, str(match_id), json.dumps(message))
        progress[match_id] = (i, len(events), event["minute"])


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--speed", type=float, default=30,
                        help="how many times faster than real time (default 30)")
    parser.add_argument("--match", type=int, action="append",
                        help="match id to replay; repeat for several (default: all in HDFS)")
    args = parser.parse_args()

    match_ids = args.match or sorted(int(f.removesuffix(".json")) for f in hdfs_list(EVENTS_DIR))
    print(f"Replaying {len(match_ids)} match(es) at {args.speed:g}x into '{TOPIC}': {match_ids}")

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 5,  # wait at most 5 ms to batch messages: good throughput, low latency
    })

    # All matches kick off together, at the same event_time.
    kickoff = datetime.now(timezone.utc)
    progress = {m: (0, 0, 0) for m in match_ids}
    threads = [threading.Thread(target=replay_match,
                                args=(m, producer, args.speed, kickoff, progress))
               for m in match_ids]
    for t in threads:
        t.start()

    # Print progress every 5 seconds until every match is done.
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=5 / len(threads))
        print("  ".join(f"{m}: {sent}/{total} min {minute}"
                        for m, (sent, total, minute) in progress.items()))

    producer.flush()  # make sure everything still buffered reaches Kafka
    print(f"Done: {sum(sent for sent, _, _ in progress.values())} events sent.")


if __name__ == "__main__":
    main()

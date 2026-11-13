"""Week 8: replay recorded GBFS station_status into a Redpanda topic (provided).

    docker compose -f docker/redpanda.yml up -d
    uv run python -m cascade.stream.producer --speedup 40      # 2 hours in about 3 minutes

Each line of the replay file becomes one message:
    key   = the GBFS station_id      (all readings of a station stay in order)
    value = the JSON line as recorded (includes snapshot_time, the event time)

The recording has one snapshot per minute. Between snapshots the producer
waits (real gap / speedup) seconds, so the consumer sees data arrive the way
it would from a live feed, just faster. --speedup 0 sends everything at once.
"""

import argparse
import gzip
import json
import sys
import time
from datetime import datetime
from itertools import groupby
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from cascade import config

REPLAY_FILE = config.SAMPLE_DIR / "gbfs" / "station_status_replay.jsonl.gz"
DEFAULT_BOOTSTRAP = "localhost:19092"
DEFAULT_TOPIC = "station_status"


def ensure_topic(bootstrap: str, topic: str) -> None:
    """Create the topic with ONE partition if it does not exist yet.

    One partition keeps every message in a single, time-ordered sequence, which
    keeps the consumer simple. A real system would use several partitions
    (keyed by station) and one window state per partition.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    for future in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).values():
        future.result()
    print(f"Created topic {topic}")


def read_snapshots(path: Path):
    """Yield (snapshot_time, [lines]) in recorded order."""
    with gzip.open(path, "rt") as f:
        records = (json.loads(line) for line in f)
        for snapshot_time, group in groupby(records, key=lambda r: r["snapshot_time"]):
            yield snapshot_time, list(group)


def replay(path: Path, bootstrap: str, topic: str, speedup: float, max_snapshots: int | None = None) -> int:
    ensure_topic(bootstrap, topic)
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50})
    sent = 0
    previous = None
    for i, (snapshot_time, records) in enumerate(read_snapshots(path)):
        if max_snapshots is not None and i >= max_snapshots:
            break
        current = datetime.fromisoformat(snapshot_time)
        if previous is not None and speedup > 0:
            time.sleep((current - previous).total_seconds() / speedup)
        previous = current

        for record in records:
            producer.produce(topic, key=record["station_id"], value=json.dumps(record))
            sent += 1
        producer.poll(0)                     # let the client send in the background
        print(f"snapshot {snapshot_time}: {len(records)} messages (total {sent:,})")
    producer.flush()                         # wait until every message is acknowledged
    return sent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay recorded GBFS station_status into Redpanda.")
    parser.add_argument("--file", type=Path, default=REPLAY_FILE)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--speedup", type=float, default=40.0, help="40 = 2 hours in 3 minutes; 0 = no waiting")
    parser.add_argument("--max-snapshots", type=int, help="stop after this many snapshots")
    args = parser.parse_args(argv)
    sent = replay(args.file, args.bootstrap, args.topic, args.speedup, args.max_snapshots)
    print(f"Done: {sent:,} messages sent to {args.topic}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

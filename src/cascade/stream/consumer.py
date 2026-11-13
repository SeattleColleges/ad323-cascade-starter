"""Week 8: station availability consumer.

YOU WRITE: window_start, StationWindow.add, write_window, and the consumer's
           handle / close_window / flush (TODO(week08)).
PROVIDED:  the poll loop (run), commit(), the Kafka connection and the
           command line. Tests: uv run pytest -m week08 (no Redpanda needed).

    uv run python -m cascade.stream.consumer                     # keeps running; Ctrl+C to stop
    uv run python -m cascade.stream.consumer --exit-when-idle    # stop once the replay is done

What it does, per 5-minute window (by snapshot_time, the event time):
    for each station: average bikes, e-bikes and docks available, the lowest
    bike count, how many readings showed 0 bikes, and is_empty (0 bikes in
    EVERY reading of the window).

Where it writes: one Parquet file per window,
    <data_root>/stream/station_availability/2026-11-12T16-30.parquet
NOT into warehouse.duckdb. DuckDB allows one writer at a time; a consumer that
runs all day would hold that lock and dbt could never build. Files avoid the
fight, and dbt reads them as a source (model fct_station_availability).

The three rules that make it safe to crash and restart:
  1. A window is written only when it is complete: when the first message of
     the NEXT window arrives (or the stream goes quiet for --idle-seconds).
  2. Each window always goes to the same file name, written to a temp file and
     renamed. Writing a window twice gives the same file, not a duplicate.
  3. The offset is committed only AFTER the file is written, and it points at
     the first message of the still-open window. After a crash, the consumer
     re-reads exactly the messages of the windows that were not committed yet,
     recomputes them, and overwrites their files with identical content.
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from cascade import config

log = logging.getLogger("cascade.stream.consumer")

WINDOW_MINUTES = 5
GROUP_ID = "cascade-availability"
OUTPUT_SUBDIR = Path("stream") / "station_availability"


class SimulatedCrash(Exception):
    """Raised on purpose by --crash-after-windows to practise recovery."""


def window_start(ts: datetime) -> datetime:
    """The start of the WINDOW_MINUTES-long window a timestamp falls in (16:37:12 -> 16:35:00)."""
    raise NotImplementedError("TODO(week08): window_start")


@dataclass
class StationWindow:
    """Running totals for one station in one window."""
    readings: int = 0
    bikes: int = 0
    ebikes: int = 0
    docks: int = 0
    min_bikes: int | None = None
    empty_readings: int = 0

    def add(self, record: dict) -> None:
        """Add one station_status message (num_bikes_available, num_ebikes_available,
        num_docks_available; num_ebikes_available can be null) to the totals."""
        raise NotImplementedError("TODO(week08): StationWindow.add")


def write_window(out_dir: Path, start: datetime, stations: dict[str, StationWindow]) -> Path:
    """Write one window to <out_dir>/<YYYY-MM-DDTHH-MM>.parquet, atomically. Return the path.

    One row per station, sorted by station_id, with these columns:
      window_start, window_end   TIMESTAMP (window_end = start + WINDOW_MINUTES)
      station_id                 VARCHAR
      readings                   INTEGER
      avg_bikes_available, avg_ebikes_available, avg_docks_available   DOUBLE, 2 decimals
      min_bikes_available        INTEGER
      empty_readings             INTEGER  readings with 0 bikes
      is_empty                   BOOLEAN  0 bikes in EVERY reading of the window
    Same input -> byte-identical file. Write to a temp file, then rename it.
    """
    raise NotImplementedError("TODO(week08): write_window")


class AvailabilityConsumer:
    """Windowing logic, separate from Kafka so it can be tested with a fake consumer."""

    def __init__(self, consumer, out_dir: Path, crash_after_windows: int | None = None):
        self.consumer = consumer
        self.out_dir = Path(out_dir)
        self.crash_after_windows = crash_after_windows
        self.open_start: datetime | None = None          # the window we are filling
        self.stations: dict[str, StationWindow] = {}
        self.last_position = None                        # (topic, partition, offset) of the last message
        self.windows_written = 0
        self.late_messages = 0

    def handle(self, msg) -> None:
        """Process one message. msg.value() is JSON with snapshot_time (event time,
        ISO format) and station_id; msg.topic(), msg.partition(), msg.offset() say where it is.

        * The first message of a LATER window means the open window is complete:
          close it, committing the offset of THIS message (where a restart must resume).
        * A message for a window that was already written is late: count it in
          self.late_messages, log a warning, and ignore it.
        * Otherwise add it to its station's StationWindow and remember its position.
        """
        raise NotImplementedError("TODO(week08): handle")

    def close_window(self, commit_at: tuple[str, int, int]) -> None:
        """Write the open window, THEN commit `commit_at`, then start empty totals.

        Practice crash: if crash_after_windows equals the number of windows
        written so far (count this one), raise SimulatedCrash AFTER writing and
        BEFORE committing. That is exactly the case the three rules protect.
        """
        raise NotImplementedError("TODO(week08): close_window")

    def commit(self, topic: str, partition: int, offset: int) -> None:
        from confluent_kafka import TopicPartition
        self.consumer.commit(offsets=[TopicPartition(topic, partition, offset)], asynchronous=False)

    def flush(self) -> None:
        """The stream went quiet: if a window is open, close it and commit PAST its last message."""
        raise NotImplementedError("TODO(week08): flush")

    def run(self, idle_seconds: float = 15.0, exit_when_idle: bool = False) -> None:
        quiet_since = time.monotonic()
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - quiet_since >= idle_seconds:
                    self.flush()
                    if exit_when_idle:
                        return
                    quiet_since = time.monotonic()
                continue
            if msg.error():
                raise RuntimeError(f"Kafka error: {msg.error()}")
            quiet_since = time.monotonic()
            self.handle(msg)


def make_kafka_consumer(bootstrap: str, topic: str, group: str = GROUP_ID):
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,                    # Redpanda remembers this group's committed offsets
        "auto.offset.reset": "earliest",      # first run ever: start at the beginning of the topic
        "enable.auto.commit": False,          # we commit ourselves, only after writing
    })
    consumer.subscribe([topic])
    return consumer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Consume station_status into 5-minute availability windows.")
    parser.add_argument("--data-root", type=Path, default=config.DEFAULT_DATA_ROOT)
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--topic", default="station_status")
    parser.add_argument("--idle-seconds", type=float, default=15.0,
                        help="write the open window after this many quiet seconds")
    parser.add_argument("--exit-when-idle", action="store_true", help="stop after the stream goes quiet")
    parser.add_argument("--crash-after-windows", type=int, help="practice: crash after writing N windows")
    parser.add_argument("--group", default=GROUP_ID,
                        help="consumer group; a new name starts again from the beginning of the topic")
    args = parser.parse_args(argv)

    config.setup_logging()
    kafka = make_kafka_consumer(args.bootstrap, args.topic, args.group)
    app = AvailabilityConsumer(kafka, args.data_root / OUTPUT_SUBDIR, args.crash_after_windows)
    try:
        app.run(args.idle_seconds, args.exit_when_idle)
    except SimulatedCrash as err:
        log.error("%s", err)
        return 1
    except KeyboardInterrupt:
        log.info("Stopped. The open window was not written; it will be recomputed on restart.")
    finally:
        kafka.close()
    log.info("Windows written: %d, late messages ignored: %d", app.windows_written, app.late_messages)
    return 0


if __name__ == "__main__":
    sys.exit(main())

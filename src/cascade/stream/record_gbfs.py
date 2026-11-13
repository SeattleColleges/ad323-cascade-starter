"""Record Divvy's live GBFS station_status feed into a replay file (instructor tool).

    uv run python -m cascade.stream.record_gbfs --polls 121 --interval 60

Students never need the live feed: they replay this recording in Week 8. Polling
the live feed yourself is an optional extension.

Writes to data/sample/gbfs/:
    station_status_replay.jsonl.gz   one line per station per poll (trimmed fields)
    station_information.json.gz      one snapshot of the station list, which maps
                                     GBFS station_id to short_name (= trip station ID)

Each line looks like:
    {"snapshot_time": "2026-11-12T16:05:03", "station_id": "1933998036428004326",
     "num_bikes_available": 3, "num_ebikes_available": 3, "num_docks_available": 2,
     "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": 1794521336}

snapshot_time is the feed's own last_updated time in Chicago local time (no
offset), the same convention as the trip data. If the feed has not changed
since the previous poll, that poll is skipped, so no snapshot appears twice.
"""

import argparse
import gzip
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from cascade import config

log = logging.getLogger("cascade.stream.record_gbfs")

FEED_ROOT = "https://gbfs.lyft.com/gbfs/1.1/chi/en"
OUT_DIR = config.SAMPLE_DIR / "gbfs"
STATUS_FIELDS = ["station_id", "num_bikes_available", "num_ebikes_available", "num_docks_available",
                 "is_installed", "is_renting", "is_returning", "last_reported"]
INFO_FIELDS = ["station_id", "short_name", "name", "lat", "lon", "capacity", "station_type"]
CHICAGO = ZoneInfo(config.TIMEZONE)


def fetch(feed: str) -> dict:
    request = urllib.request.Request(f"{FEED_ROOT}/{feed}.json", headers={"User-Agent": "cascade-mobility-pipeline"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def chicago_time(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, CHICAGO).replace(tzinfo=None).isoformat(timespec="seconds")


def record(polls: int, interval: float, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    info = fetch("station_information")
    stations = [{k: s.get(k) for k in INFO_FIELDS} for s in info["data"]["stations"]]
    with gzip.open(out_dir / "station_information.json.gz", "wt") as f:
        json.dump({"snapshot_time": chicago_time(info["last_updated"]), "stations": stations}, f)
    log.info("Saved station_information: %d stations", len(stations))

    final = out_dir / "station_status_replay.jsonl.gz"
    tmp = out_dir / "station_status_replay.jsonl.gz.tmp"
    last_seen = None
    lines = written_polls = 0
    start = time.monotonic()
    with gzip.open(tmp, "wt") as out:
        for i in range(polls):
            # Fixed schedule: poll i happens at start + i*interval, however long fetches take.
            time.sleep(max(0.0, start + i * interval - time.monotonic()))
            try:
                feed = fetch("station_status")
            except Exception as err:  # a missed poll is a gap in the data, not a reason to stop
                log.warning("Poll %d failed: %s", i + 1, err)
                continue
            if feed["last_updated"] == last_seen:
                log.info("Poll %d: feed unchanged, skipped", i + 1)
                continue
            last_seen = feed["last_updated"]
            snapshot_time = chicago_time(feed["last_updated"])
            for s in feed["data"]["stations"]:
                out.write(json.dumps({"snapshot_time": snapshot_time, **{k: s.get(k) for k in STATUS_FIELDS}}) + "\n")
                lines += 1
            written_polls += 1
            out.flush()
            log.info("Poll %d/%d: snapshot %s, %d stations", i + 1, polls, snapshot_time, len(feed["data"]["stations"]))
    os.replace(tmp, final)
    log.info("Done: %d snapshots, %d lines, %.1f MB -> %s",
             written_polls, lines, final.stat().st_size / 1e6, final)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record GBFS station_status for replay.")
    parser.add_argument("--polls", type=int, default=121, help="number of polls (121 at 60 s = 2 hours)")
    parser.add_argument("--interval", type=float, default=60.0, help="seconds between polls (feed ttl is 60)")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args(argv)
    config.setup_logging()
    record(args.polls, args.interval, args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())

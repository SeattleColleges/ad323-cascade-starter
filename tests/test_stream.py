"""Week 8 tests: windowing, and that a crash + restart gives identical output.

No Redpanda needed: FakeConsumer below behaves like a Kafka consumer for one
partition: poll() hands out messages in order, commit() remembers an offset,
and a "restarted" consumer starts again from the last committed offset.
"""

import gzip
import json
from datetime import datetime, timedelta

import duckdb
import pytest

from cascade.stream import consumer as stream
from conftest import SAMPLE_DIR

pytestmark = pytest.mark.week08

REPLAY = SAMPLE_DIR / "gbfs" / "station_status_replay.jsonl.gz"


class FakeMessage:
    def __init__(self, value: str, offset: int):
        self._value, self._offset = value.encode(), offset

    def value(self): return self._value
    def offset(self): return self._offset
    def topic(self): return "station_status"
    def partition(self): return 0
    def error(self): return None


class FakeBroker:
    """One topic partition plus the committed offset of one consumer group."""

    def __init__(self, records: list[dict]):
        self.messages = [FakeMessage(json.dumps(r), i) for i, r in enumerate(records)]
        self.committed = 0          # nothing committed yet = start at the beginning

    def consumer(self):
        return FakeConsumer(self)


class FakeConsumer:
    def __init__(self, broker: FakeBroker):
        self.broker = broker
        self.position = broker.committed        # a restarted consumer resumes here

    def poll(self, timeout):
        if self.position >= len(self.broker.messages):
            return None
        msg = self.broker.messages[self.position]
        self.position += 1
        return msg

    def commit(self, offsets, asynchronous):
        self.broker.committed = offsets[0].offset


def synthetic_records(minutes=23):
    """3 stations, one snapshot per minute from 16:00. Station C is always empty."""
    start = datetime(2026, 11, 12, 16, 0)
    records = []
    for m in range(minutes):
        t = (start + timedelta(minutes=m)).isoformat()
        for station, bikes in [("A", m % 4), ("B", 10), ("C", 0)]:
            records.append({"snapshot_time": t, "station_id": station, "num_bikes_available": bikes,
                            "num_ebikes_available": 1, "num_docks_available": 20 - bikes})
    return records


def run_to_end(broker, out_dir, crash_after=None):
    app = stream.AvailabilityConsumer(broker.consumer(), out_dir, crash_after_windows=crash_after)
    app.run(idle_seconds=0, exit_when_idle=True)
    return app


def read_windows(out_dir):
    return duckdb.sql(f"SELECT * FROM '{out_dir}/*.parquet' ORDER BY window_start, station_id").fetchall()


# ----- Windowing ----------------------------------------------------------------------------


def test_window_start_rounds_down_to_five_minutes():
    assert stream.window_start(datetime(2026, 11, 12, 16, 37, 12)) == datetime(2026, 11, 12, 16, 35)
    assert stream.window_start(datetime(2026, 11, 12, 16, 35, 0)) == datetime(2026, 11, 12, 16, 35)


def test_one_file_per_window_with_correct_averages(tmp_path):
    run_to_end(FakeBroker(synthetic_records()), tmp_path)
    files = sorted(p.name for p in tmp_path.glob("*.parquet"))
    assert files == [f"2026-11-12T16-{m:02d}.parquet" for m in (0, 5, 10, 15, 20)]   # 16:20 window is partial

    con = duckdb.connect()
    first = con.execute(f"""
        SELECT station_id, readings, avg_bikes_available, min_bikes_available, empty_readings, is_empty
        FROM '{tmp_path}/2026-11-12T16-00.parquet' ORDER BY station_id""").fetchall()
    # Station A has bikes 0, 1, 2, 3, 0 in minutes 0-4: average 1.2, lowest 0,
    # two empty readings, but not empty for the whole window.
    assert first == [
        ("A", 5, 1.2, 0, 2, False),
        ("B", 5, 10.0, 10, 0, False),
        ("C", 5, 0.0, 0, 5, True),       # 0 bikes in every reading -> is_empty
    ]


def test_offsets_are_committed_only_after_writing(tmp_path):
    broker = FakeBroker(synthetic_records())
    with pytest.raises(stream.SimulatedCrash):
        run_to_end(broker, tmp_path, crash_after=1)
    # The 16:00 window was written, but the crash came before its commit.
    assert (tmp_path / "2026-11-12T16-00.parquet").exists()
    assert broker.committed == 0


# ----- Crash and restart -------------------------------------------------------------------------


@pytest.mark.parametrize("crash_after", [1, 3])
def test_crash_and_restart_gives_identical_files(tmp_path, crash_after):
    records = synthetic_records()

    clean_dir = tmp_path / "no_crash"
    run_to_end(FakeBroker(records), clean_dir)

    crash_dir = tmp_path / "crash"
    broker = FakeBroker(records)
    with pytest.raises(stream.SimulatedCrash):
        run_to_end(broker, crash_dir, crash_after=crash_after)
    run_to_end(broker, crash_dir)          # restart: same group, resumes from the committed offset

    names = sorted(p.name for p in clean_dir.glob("*.parquet"))
    assert names == sorted(p.name for p in crash_dir.glob("*.parquet"))
    for name in names:                     # byte-for-byte identical, not just similar
        assert (clean_dir / name).read_bytes() == (crash_dir / name).read_bytes()
    assert not list(crash_dir.glob("*.tmp"))


@pytest.mark.skipif(not REPLAY.exists(), reason="replay file not recorded yet")
def test_real_replay_crash_and_restart(tmp_path):
    with gzip.open(REPLAY, "rt") as f:
        records = [json.loads(line) for _, line in zip(range(2079 * 12), f)]   # first ~12 snapshots
    clean_dir, crash_dir = tmp_path / "a", tmp_path / "b"
    run_to_end(FakeBroker(records), clean_dir)
    broker = FakeBroker(records)
    with pytest.raises(stream.SimulatedCrash):
        run_to_end(broker, crash_dir, crash_after=1)
    run_to_end(broker, crash_dir)
    assert read_windows(clean_dir) == read_windows(crash_dir)

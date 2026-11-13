"""Week 7 tests: the DuckDB side of the Spark comparison, run on the small sample.

The real lab uses a full year; these only check that your three queries and
the partitioned write give the right answers, quickly, on sample silver.
"""

import importlib.util

import duckdb
import pytest

from conftest import BATCHES, REPO_ROOT, land_silver

pytestmark = pytest.mark.week07

spec = importlib.util.spec_from_file_location("week07_duckdb", REPO_ROOT / "spark" / "week07_duckdb.py")
week07 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(week07)

VALID_TRIPS = 97_626


@pytest.fixture(scope="module")
def data_root(tmp_path_factory):
    data_root = tmp_path_factory.mktemp("week07") / "data"
    for batch_id in BATCHES:
        land_silver(batch_id, data_root)
    return data_root


@pytest.fixture(scope="module")
def con(data_root):
    con = week07.connect(data_root)
    yield con
    con.close()


def test_q1_counts_every_valid_trip_once(con):
    rows = week07.run_query(con, "q1_trips_by_month_and_member").fetchall()
    assert len(rows) == 4                                         # 2 months x member/casual
    assert sum(trips for *_, trips in rows) == VALID_TRIPS


def test_q2_top10_start_stations(con):
    rows = week07.run_query(con, "q2_top10_start_stations").fetchall()
    assert len(rows) == 10
    assert rows[0] == ("Navy Pier", 980)
    assert [t for _, t in rows] == sorted((t for _, t in rows), reverse=True)


def test_q3_avg_minutes_by_hour_and_bike(con):
    rel = week07.run_query(con, "q3_avg_minutes_by_hour_and_bike")
    assert rel.columns == ["start_hour", "rideable_type", "trips", "avg_minutes"]
    rows = rel.fetchall()
    assert sum(r[2] for r in rows) == VALID_TRIPS
    assert {r[0] for r in rows} == set(range(24))


def test_station_daily_is_partitioned_by_month(con, data_root):
    out_dir = data_root / "engine_out" / "duckdb" / "station_daily"
    assert week07.write_station_daily(con, out_dir) == 30_087
    assert sorted(p.name for p in out_dir.iterdir()) == ["year_month=2026-09", "year_month=2026-10"]
    assert week07.write_station_daily(con, out_dir) == 30_087     # a rerun replaces, not appends
    total = duckdb.sql(f"SELECT sum(trips) FROM read_parquet('{out_dir}/*/*.parquet')").fetchone()[0]
    assert total == duckdb.sql(f"""SELECT count(*) FROM read_parquet('{data_root}/silver/trips/*/*.parquet')
                                   WHERE is_valid AND start_station_id IS NOT NULL""").fetchone()[0]

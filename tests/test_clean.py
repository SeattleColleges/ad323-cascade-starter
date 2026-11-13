"""Week 3 tests: bronze -> silver standardizes, deduplicates, and flags (not drops).

Most tests build a small "source" folder: the real sample file plus a few
hand-made rows that each break exactly one rule. We ingest that folder into
bronze, clean it, and check what silver says about each planted row.
"""

import json
import shutil

import duckdb
import pytest

from cascade import clean, config
from conftest import SAMPLE_DIR, land_bronze

pytestmark = pytest.mark.week03

BATCH = "2026-09"
TRIPS_NAME = config.trips_filename(BATCH)


def csv_line(ride_id="TESTGOOD", start="2026-09-10 08:00:00", end="2026-09-10 08:15:00",
             start_id="CHI00001", end_id="CHI00002",
             start_lat="41.88", start_lng="-87.63", end_lat="41.89", end_lng="-87.62"):
    """One trip in Divvy's CSV format. The defaults describe a perfectly valid trip."""
    return (f'"{ride_id}","classic_bike","{start}","{end}","A St","{start_id}","B St","{end_id}",'
            f'{start_lat},{start_lng},{end_lat},{end_lng},"member"\r\n')  # Divvy files use CRLF


def run_pipeline(tmp_path, extra_lines=()):
    """Copy the sample batch, append `extra_lines`, land it in bronze, clean it.

    Returns (data_root, dq_report).
    """
    source = tmp_path / "source"
    shutil.copytree(SAMPLE_DIR / BATCH, source / BATCH)
    with open(source / BATCH / TRIPS_NAME, "a", newline="") as f:  # newline="": write \r\n as-is
        f.writelines(extra_lines)

    data_root = tmp_path / "data"
    land_bronze(BATCH, data_root, source)          # bronze without your Week 2 code
    report = clean.clean_batch(BATCH, data_root)
    return data_root, report


def silver(data_root, dataset="trips"):
    """A DuckDB relation over all silver partitions of a dataset."""
    return duckdb.sql(f"SELECT * FROM read_parquet('{data_root}/silver/{dataset}/*/*.parquet')")


def silver_row(data_root, ride_id):
    rows = silver(data_root).filter(f"ride_id = '{ride_id}'")
    records = [dict(zip(rows.columns, r)) for r in rows.fetchall()]
    assert len(records) == 1, f"expected exactly one silver row for {ride_id}, got {len(records)}"
    return records[0]


# ----- The sample itself ------------------------------------------------------


def test_sample_cleans_to_one_row_per_trip(bronze_root):
    for batch_id in ["2026-09", "2026-10"]:
        clean.clean_batch(batch_id, bronze_root)
    counts = silver(bronze_root).aggregate("year_month, count(*), count(DISTINCT ride_id)").order("year_month")
    assert counts.fetchall() == [("2026-09", 50_000, 50_000), ("2026-10", 50_000, 50_000)]


def test_silver_types_are_declared_not_guessed(bronze_root):
    # October's ride_ids look like numbers; DuckDB's sniffer guesses BIGINT.
    clean.clean_batch("2026-10", bronze_root)
    types = dict(zip(silver(bronze_root).columns, map(str, silver(bronze_root).types)))
    assert types["ride_id"] == "VARCHAR"
    assert types["start_station_id"] == "VARCHAR"
    assert types["started_at"] == "TIMESTAMP"
    assert types["start_lat"] == "DOUBLE"
    assert types["is_valid"] == "BOOLEAN"
    assert types["cleaned_at"] == "TIMESTAMP"          # when clean ran (UTC); Week 10 freshness reads it


def test_weather_silver_has_one_row_per_hour(bronze_root):
    clean.clean_batch(BATCH, bronze_root)
    rel = silver(bronze_root, "weather")
    assert rel.aggregate("count(*), count(DISTINCT observed_at), count(temp_c)").fetchone() == (720, 720, 720)


# ----- Deduplication ------------------------------------------------------------


def test_planted_duplicates_are_removed(tmp_path):
    with open(SAMPLE_DIR / BATCH / TRIPS_NAME, newline="") as f:  # keep the original \r\n endings
        sample_lines = f.readlines()
    first, second = sample_lines[1], sample_lines[2]
    # Same ride again in lower case with stray spaces: a duplicate only
    # *after* standardizing the ID. This is why we standardize before dedup.
    ride_id = first.split(",")[0].strip('"')
    messy_first = first.replace(f'"{ride_id}"', f'" {ride_id.lower()} "', 1)

    data_root, report = run_pipeline(tmp_path, [first, first, second, messy_first])

    assert report["trips"]["bronze_rows"] == 50_004
    assert report["trips"]["duplicates_removed"] == 4
    assert report["trips"]["silver_rows"] == 50_000
    assert silver(data_root).aggregate("count(DISTINCT ride_id)").fetchone()[0] == 50_000


def test_rows_without_ride_id_are_dropped_and_counted(tmp_path):
    data_root, report = run_pipeline(tmp_path, [csv_line(ride_id=""), csv_line(ride_id="   ")])
    assert report["trips"]["dropped_missing_ride_id"] == 2
    assert report["trips"]["silver_rows"] == 50_000


def test_ids_and_text_are_standardized(tmp_path):
    data_root, _ = run_pipeline(tmp_path, [csv_line(ride_id="  testcase01 ", start_id=" chi00001 ")])
    row = silver_row(data_root, "TESTCASE01")
    assert row["start_station_id"] == "CHI00001"
    assert row["rideable_type"] == "classic_bike"


# ----- Validation rules: flagged, never dropped -----------------------------------

# (ride_id, csv overrides, the flags we expect to be True)
CASES = [
    ("T_VALID",       {},                                                   set()),
    ("T_30_SECONDS",  {"end": "2026-09-10 08:00:30"},                       {"flag_too_short"}),
    ("T_NEGATIVE",    {"end": "2026-09-10 07:50:00"},                       {"flag_too_short"}),
    ("T_ONE_MINUTE",  {"end": "2026-09-10 08:01:00"},                       set()),  # boundary: allowed
    ("T_25_HOURS",    {"end": "2026-09-11 09:00:00"},                       {"flag_too_long"}),
    ("T_24_HOURS",    {"end": "2026-09-11 08:00:00"},                       set()),  # boundary: allowed
    ("T_NO_START_ID", {"start_id": ""},                                     {"flag_missing_station"}),
    ("T_NO_END_ID",   {"end_id": ""},                                       {"flag_missing_station"}),
    ("T_ZERO_COORDS", {"end_lat": "0", "end_lng": "0"},                     {"flag_bad_coords"}),
    ("T_NULL_COORDS", {"end_lat": "", "end_lng": ""},                       {"flag_bad_coords"}),
    ("T_TEXT_COORDS", {"start_lat": '"n/a"'},                               {"flag_bad_coords"}),
    ("T_OUT_OF_AREA", {"start_lat": "40.71", "start_lng": "-74.01"},        {"flag_bad_coords"}),
    ("T_BAD_TIME",    {"end": "not a time"},                                {"flag_bad_timestamp"}),
    ("T_TWO_ISSUES",  {"end": "2026-09-10 08:00:10", "start_id": ""},       {"flag_too_short", "flag_missing_station"}),
]


@pytest.fixture(scope="module")
def validated_root(tmp_path_factory):
    """Run the pipeline once with every test case planted, shared by the rule tests."""
    lines = [csv_line(ride_id=ride_id, **overrides) for ride_id, overrides, _ in CASES]
    data_root, _ = run_pipeline(tmp_path_factory.mktemp("rules"), lines)
    return data_root


@pytest.mark.parametrize("ride_id, overrides, expected_flags", CASES, ids=[c[0] for c in CASES])
def test_validation_rule(validated_root, ride_id, overrides, expected_flags):
    row = silver_row(validated_root, ride_id)  # still present: flagged, not dropped
    raised = {flag for flag in clean.FLAG_COLUMNS if row[flag]}
    assert raised == expected_flags
    # Only these flags make a trip invalid; a missing station alone does not.
    assert row["is_valid"] == (not expected_flags & set(clean.INVALID_FLAGS))


def test_flag_counts_in_report_match_silver(tmp_path):
    data_root, report = run_pipeline(tmp_path)
    rel = silver(data_root)
    for flag in clean.FLAG_COLUMNS:
        assert report["trips"]["flags"][flag] == rel.filter(flag).count("*").fetchone()[0]
    assert report["trips"]["valid_rows"] == rel.filter("is_valid").count("*").fetchone()[0]
    any_invalid_flag = " OR ".join(clean.INVALID_FLAGS)
    assert report["trips"]["invalid_rows"] == rel.filter(any_invalid_flag).count("*").fetchone()[0]


def test_missing_station_alone_is_still_valid(validated_root):
    # Dockless e-bike trips are real trips: flagged for counting, but valid.
    for ride_id in ["T_NO_START_ID", "T_NO_END_ID"]:
        row = silver_row(validated_root, ride_id)
        assert row["flag_missing_station"] is True
        assert row["is_valid"] is True


# ----- Idempotency and safety --------------------------------------------------------


def test_rerun_replaces_the_partition(bronze_root):
    clean.clean_batch(BATCH, bronze_root)
    clean.clean_batch(BATCH, bronze_root)
    files = list((bronze_root / "silver" / "trips").rglob("*.parquet"))
    assert [f.parent.name for f in files] == [f"year_month={BATCH}"]
    assert silver(bronze_root).count("*").fetchone()[0] == 50_000


def test_report_reconciles_and_is_written(bronze_root):
    report = clean.clean_batch(BATCH, bronze_root)
    t = report["trips"]
    assert t["bronze_rows"] == t["silver_rows"] + t["duplicates_removed"] + t["dropped_missing_ride_id"]
    assert t["valid_rows"] + t["invalid_rows"] == t["silver_rows"]
    saved = json.loads((bronze_root / "silver" / "_dq" / f"{BATCH}.json").read_text())
    assert saved == report
    assert (bronze_root / "silver" / "_dq" / f"{BATCH}.md").exists()


def test_clean_needs_ingested_bronze(data_root):
    with pytest.raises(clean.CleanError, match="run cascade.ingest first"):
        clean.clean_batch(BATCH, data_root)


def test_clean_refuses_modified_bronze(bronze_root):
    with open(bronze_root / "bronze" / BATCH / TRIPS_NAME, "a", newline="") as f:
        f.write(csv_line())
    with pytest.raises(clean.CleanError, match="manifest hash"):
        clean.clean_batch(BATCH, bronze_root)

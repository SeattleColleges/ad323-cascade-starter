"""Week 3: cleaning. Turn one bronze batch into typed, deduplicated silver Parquet.

YOU WRITE: the three trip queries, build_report() and clean_batch()
           (everything marked TODO(week03)).
PROVIDED:  the rules below (the tests check exactly these), WEATHER_SQL as a
           worked example, and the helpers for bronze files, partitions and
           the Markdown report.

Run it from the repo root after ingesting:

    uv run python -m cascade.clean --batch 2026-09

Output for a batch:

    data_root/silver/trips/year_month=2026-09/trips.parquet
    data_root/silver/weather/year_month=2026-09/weather.parquet
    data_root/silver/_dq/2026-09.json     data-quality report (for code and CI)
    data_root/silver/_dq/2026-09.md       the same report for people

Silver rules:
  * Types are declared, never guessed. (DuckDB guesses October's ride_id is
    a number; it is an ID, so it stays text.)
  * One row per ride_id. Copies are removed and counted.
  * Invalid trips are FLAGGED, not dropped, so totals still reconcile with
    bronze. Later layers decide what to exclude. is_valid is false for bad
    timestamps, too-short or too-long trips, and bad coordinates. A missing
    station is flagged but still valid (dockless e-bike trips are real).
  * Rerunning a batch replaces that batch's partition: no duplicates.

Silver trips columns, in this order (later weeks rely on them):
    ride_id, rideable_type, member_casual, started_at, ended_at, duration_min,
    start_station_id, start_station_name, end_station_id, end_station_name,
    start_lat, start_lng, end_lat, end_lng,
    flag_bad_timestamp, flag_too_short, flag_too_long, flag_missing_station,
    flag_bad_coords, is_valid, cleaned_at
Silver weather columns: observed_at, temp_c, precipitation_mm, wind_kmh,
    weather_code, cleaned_at
cleaned_at is the UTC time clean ran (no time zone), the same for every row
of the batch. Week 10's freshness check reads it.

The `year_month` partition is the batch id (the source file's month). It is
stored in the folder name, not inside the file; DuckDB reads it back as a
column. Read all of silver with:

    SELECT * FROM read_parquet('data/silver/trips/*/*.parquet', hive_partitioning = true)
"""

import argparse
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from cascade import config
from cascade.ingest import load_manifest, sha256_of, utc_now

log = logging.getLogger("cascade.clean")

# ----- Validation rules (one place, easy to discuss and change) ---------------

MIN_DURATION_MIN = 1          # shorter trips are usually false starts or re-docks
MAX_DURATION_MIN = 24 * 60    # longer trips are usually lost or stolen bikes

# A generous box around the Divvy service area (Chicago and Evanston).
# Anything outside, including the 0,0 values some systems write for "unknown",
# is a bad coordinate.
MIN_LAT, MAX_LAT = 41.6, 42.1
MIN_LNG, MAX_LNG = -88.0, -87.5

FLAG_COLUMNS = [
    "flag_bad_timestamp",    # start or end time missing or not a timestamp
    "flag_too_short",        # under 1 minute, including negative durations
    "flag_too_long",         # over 24 hours
    "flag_missing_station",  # start or end station id missing
    "flag_bad_coords",       # a coordinate is missing or outside the box
]

# The flags that make a trip invalid. flag_missing_station is NOT one of them:
# those are dockless e-bike trips that really happened. We still flag them so
# we can count them, and gold gives them an "unknown station" key.
INVALID_FLAGS = [flag for flag in FLAG_COLUMNS if flag != "flag_missing_station"]


class CleanError(Exception):
    """Something the person running the pipeline must fix."""


# =============================================================================
# SQL: each step is one readable query
# =============================================================================

# Step 1. Standardize: read the bronze CSV with every column as text
# (all_varchar = true), then trim text, turn empty strings into NULL, set the
# case of IDs and categories, and cast to real types. Use TRY_CAST, so an
# unreadable value becomes NULL (and gets flagged in step 3) instead of crashing.
STANDARDIZE_SQL = """
-- TODO(week03): CREATE OR REPLACE TEMP TABLE standardized AS SELECT ... FROM read_csv(?, ...)
"""

# Step 2. Deduplicate: one row per ride_id. Standardize BEFORE you deduplicate
# (" abc " and "ABC" are the same ride). Rows with no ride_id are dropped and
# counted; everything else is kept.
DEDUPLICATE_SQL = """
-- TODO(week03): CREATE OR REPLACE TEMP TABLE deduplicated AS ...
"""

# Step 3. Validate: add duration_min (minutes, 2 decimals) and one true/false
# column per rule in FLAG_COLUMNS, then is_valid. Flag, never drop.
VALIDATE_SQL = """
-- TODO(week03): CREATE OR REPLACE TEMP TABLE validated AS ...
"""

# WORKED EXAMPLE.
# Weather: Open-Meteo returns one list per variable. UNNEST on several lists
# in the same SELECT lines them up position by position: one row per hour.
WEATHER_SQL = """
CREATE OR REPLACE TEMP TABLE weather AS
SELECT
    unnest(hourly.time)::TIMESTAMP       AS observed_at,      -- local Chicago time
    unnest(hourly.temperature_2m)        AS temp_c,
    unnest(hourly.precipitation)         AS precipitation_mm,
    unnest(hourly.wind_speed_10m)        AS wind_kmh,
    unnest(hourly.weather_code)::INTEGER AS weather_code      -- WMO weather code
FROM read_json(?)
"""


# =============================================================================
# Helpers
# =============================================================================


def bronze_file(batch_id: str, data_root: Path, dataset: str) -> Path:
    """Find a bronze file through the manifest and check it is unchanged."""
    entry = load_manifest(data_root).get(batch_id, {}).get(dataset)
    if entry is None:
        raise CleanError(f"No {dataset} for {batch_id} in the bronze manifest; run cascade.ingest first")
    path = Path(data_root) / "bronze" / entry["file"]
    if not path.exists() or sha256_of(path) != entry["sha256"]:
        raise CleanError(f"{path} is missing or does not match its manifest hash")
    return path


def write_partition(con: duckdb.DuckDBPyConnection, table: str, data_root: Path,
                    dataset: str, batch_id: str) -> Path:
    """Replace one year_month partition with the contents of `table`.

    We delete the old partition first, so rerunning a batch replaces it
    instead of adding a second copy next to it.
    """
    partition = Path(data_root) / "silver" / dataset / f"year_month={batch_id}"
    if partition.exists():
        shutil.rmtree(partition)
    partition.mkdir(parents=True)
    target = partition / f"{dataset}.parquet"
    tmp = partition / f"{dataset}.parquet.tmp"
    con.execute(f"COPY {table} TO '{tmp}' (FORMAT parquet)")
    os.replace(tmp, target)
    return target


def count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return con.execute(sql).fetchone()[0]


# =============================================================================
# Data-quality report
# =============================================================================


def build_report(con: duckdb.DuckDBPyConnection, batch_id: str, trips_path: Path) -> dict:
    """Count what happened to the batch. The tests and later weeks read exactly these keys:

    {
      "batch_id": ..., "generated_at": utc_now(),
      "bronze_file": trips_path.name, "bronze_sha256": sha256_of(trips_path),
      "trips": {
        "bronze_rows", "dropped_missing_ride_id", "duplicates_removed", "silver_rows",
        "flags": {<flag>: <rows with that flag> for every flag in FLAG_COLUMNS},
        "valid_rows", "invalid_rows", "pct_valid"   (percent, 2 decimals)
      },
      "weather": {"hours", "rows_with_missing_values"}
    }

    It must reconcile: bronze_rows = silver_rows + duplicates_removed + dropped_missing_ride_id,
    and valid_rows + invalid_rows = silver_rows.
    """
    raise NotImplementedError("TODO(week03): build_report")


def report_markdown(report: dict) -> str:
    t = report["trips"]
    lines = [
        f"# Data-quality report: {report['batch_id']}",
        "",
        f"Source: `{report['bronze_file']}` (sha256 `{report['bronze_sha256'][:12]}…`), "
        f"generated {report['generated_at']}.",
        "",
        "| Step | Rows |",
        "|---|---:|",
        f"| Bronze rows | {t['bronze_rows']:,} |",
        f"| Dropped: missing ride_id | {t['dropped_missing_ride_id']:,} |",
        f"| Removed: duplicate ride_id | {t['duplicates_removed']:,} |",
        f"| **Silver rows** | **{t['silver_rows']:,}** |",
        f"| Valid | {t['valid_rows']:,} ({t['pct_valid']}%) |",
        f"| Invalid (bad timestamp, too short, too long, or bad coordinates) | {t['invalid_rows']:,} |",
        "",
        "| Flag | Rows | % of silver | Makes trip invalid? |",
        "|---|---:|---:|---|",
    ]
    for flag, n in t["flags"].items():
        pct = 100.0 * n / t["silver_rows"] if t["silver_rows"] else 0.0
        invalid = "yes" if flag in INVALID_FLAGS else "no (kept as unknown station)"
        lines.append(f"| {flag} | {n:,} | {pct:.2f}% | {invalid} |")
    w = report["weather"]
    lines += ["", f"Weather: {w['hours']} hourly rows, {w['rows_with_missing_values']} with missing values.", ""]
    return "\n".join(lines)


# =============================================================================
# Stage entry point
# =============================================================================


def clean_batch(batch_id: str, data_root: Path = config.DEFAULT_DATA_ROOT) -> dict:
    """Bronze -> silver for one batch. Returns the data-quality report.

    1. Check the batch id; find both bronze files with bronze_file().
    2. In an in-memory DuckDB, run STANDARDIZE_SQL, DEDUPLICATE_SQL, VALIDATE_SQL
       and WEATHER_SQL. Add cleaned_at to the trips and weather tables.
    3. build_report(), and check it reconciles.
    4. write_partition() for trips and weather (a rerun replaces the partition).
    5. Save the report as silver/_dq/<batch_id>.json and, with report_markdown(),
       silver/_dq/<batch_id>.md. Log a one-line summary.
    """
    raise NotImplementedError("TODO(week03): clean_batch")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Clean bronze batches into silver Parquet.")
    parser.add_argument("--batch", nargs="+", required=True, help="one or more batch ids, e.g. 2026-09")
    parser.add_argument("--data-root", type=Path, default=config.DEFAULT_DATA_ROOT)
    args = parser.parse_args(argv)

    config.setup_logging()
    try:
        for batch_id in args.batch:
            clean_batch(batch_id, args.data_root)
    except (CleanError, ValueError) as err:
        log.error("%s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

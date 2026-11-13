"""Week 7: the DuckDB side of the Spark vs. DuckDB comparison.

YOU WRITE: the SQL marked TODO(week07). Q1 is a finished example. The tests
(uv run pytest -m week07) run your SQL on the small sample.

    uv run python spark/week07_duckdb.py                         # reads data/runs/full-year/silver
    uv run python spark/week07_duckdb.py --data-root data/runs/full-year

Runs the SAME three aggregations as spark/week07_spark_lab.ipynb, times each
one, and writes the same partitioned Parquet output, so the two engines can be
compared fairly. Copy the printed timings into your WEEK_07.md.

DuckDB runs inside this Python process on your laptop. We cap it at 2 GB of
memory so the comparison is closer to the Spark container's limit.
"""

import argparse
import shutil
import time
from pathlib import Path

import duckdb

from cascade import todo

# ----- The three questions (the Spark notebook asks exactly the same) -----------

QUERIES = {
    # WORKED EXAMPLE.
    "q1_trips_by_month_and_member": """
        SELECT year_month, member_casual, count(*) AS trips
        FROM silver
        WHERE is_valid
        GROUP BY year_month, member_casual
        ORDER BY year_month, member_casual
    """,
    # TODO(week07): the same Question 2 as the notebook, in SQL. Columns: start_station_name, trips.
    "q2_top10_start_stations": """
        -- TODO(week07): top 10 start stations by valid trips (busiest first; ties by name)
    """,
    # TODO(week07): Question 3. Columns: start_hour, rideable_type, trips, avg_minutes.
    "q3_avg_minutes_by_hour_and_bike": """
        -- TODO(week07): valid trips and average minutes (2 decimals) by start hour and bike type
    """,
}

# The table we write back out: trips per start station per day, partitioned by month.
# Columns: start_station_id, trip_date, year_month, trips (valid trips with a start station).
STATION_DAILY = """
    -- TODO(week07): trips per start station per day
"""


def connect(data_root: Path) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with a view `silver` over all silver trip partitions."""
    silver = Path(data_root) / "silver" / "trips" / "*" / "*.parquet"
    con = duckdb.connect()
    con.execute("SET memory_limit = '2GB'")
    con.execute(f"CREATE VIEW silver AS SELECT * FROM read_parquet('{silver}', hive_partitioning = true)")
    return con


def run_query(con: duckdb.DuckDBPyConnection, name: str) -> duckdb.DuckDBPyRelation:
    """Run one of QUERIES by name."""
    todo.raise_if_todo(QUERIES[name], name)
    return con.sql(QUERIES[name])


def write_station_daily(con: duckdb.DuckDBPyConnection, out_dir: Path) -> int:
    """Write STATION_DAILY as Parquet partitioned by year_month. Returns the rows written."""
    todo.raise_if_todo(STATION_DAILY, "STATION_DAILY")
    if out_dir.exists():                      # delete old output first so a rerun replaces it
        shutil.rmtree(out_dir)
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({STATION_DAILY}) TO '{out_dir}' (FORMAT parquet, PARTITION_BY (year_month))")
    return con.execute(f"SELECT count(*) FROM read_parquet('{out_dir}/*/*.parquet')").fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB side of the Week 7 comparison.")
    parser.add_argument("--data-root", type=Path, default=Path("data/runs/full-year"))
    args = parser.parse_args()

    out_dir = args.data_root / "engine_out" / "duckdb" / "station_daily"
    con = connect(args.data_root)

    timings = {}
    start = time.perf_counter()
    rows = con.execute("SELECT count(*) FROM silver").fetchone()[0]
    timings["count_all_rows"] = time.perf_counter() - start
    print(f"Silver rows: {rows:,}")

    for name in QUERIES:
        start = time.perf_counter()
        run_query(con, name).show(max_rows=30)
        timings[name] = time.perf_counter() - start

    start = time.perf_counter()
    written = write_station_daily(con, out_dir)
    timings["write_station_daily"] = time.perf_counter() - start
    print(f"Wrote {written:,} station-day rows to {out_dir}")

    print("\nTimings (seconds):")
    for name, seconds in timings.items():
        print(f"  {name:36s} {seconds:7.2f}")


if __name__ == "__main__":
    main()

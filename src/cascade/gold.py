"""Week 4: build the star schema from silver with plain SQL files.

    uv run python -m cascade.gold                      # uses data/
    uv run python -m cascade.gold --data-root data/runs/sample

The SQL lives in src/cascade/gold_sql/, one file per table, run in name order
(dimensions first, then the fact that points at them). Tables go into the
`gold_sql` schema of <data_root>/warehouse.duckdb.

This is a full rebuild: every run recreates every table from all of silver.
In Week 5 dbt takes over and makes the fact table incremental. The Week 4
tables stay as a reference the dbt port is checked against.

After building, we check that gold reconciles with the silver data-quality
reports. If the numbers don't add up, the build fails loudly.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import duckdb

from cascade import config, todo

log = logging.getLogger("cascade.gold")

SQL_DIR = Path(__file__).parent / "gold_sql"
SCHEMA = "gold_sql"
TABLES = ["dim_date", "dim_station", "dim_weather", "fct_trips"]

# Each foreign key in the fact and the dimension column it must match.
FOREIGN_KEYS = [
    ("start_station_key", "dim_station", "station_key"),
    ("end_station_key", "dim_station", "station_key"),
    ("date_key", "dim_date", "date_key"),
    ("weather_key", "dim_weather", "weather_key"),
]


class GoldError(Exception):
    """Gold could not be built, or it does not reconcile with silver."""


def warehouse_path(data_root: Path) -> Path:
    return Path(data_root) / "warehouse.duckdb"


def create_silver_views(con: duckdb.DuckDBPyConnection, data_root: Path) -> None:
    """Point two temporary views at the silver Parquet files.

    The SQL files only ever say `silver_trips` and `silver_weather`, so they
    work for any data root. TEMP views disappear when the connection closes,
    so no file paths get saved inside the warehouse.
    """
    silver = Path(data_root).resolve() / "silver"
    for name in ["trips", "weather"]:
        files = list((silver / name).glob("year_month=*/*.parquet"))
        if not files:
            raise GoldError(f"No silver {name} in {silver / name}; run cascade.clean first")
        pattern = str(silver / name / "*" / "*.parquet").replace("'", "''")
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW silver_{name} AS
            SELECT * FROM read_parquet('{pattern}', hive_partitioning = true)
        """)


def scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


def reconcile(con: duckdb.DuckDBPyConnection, data_root: Path) -> dict:
    """Check gold against silver and the data-quality reports. Raise if anything is off."""
    batches = [row[0] for row in con.execute(
        "SELECT DISTINCT year_month FROM silver_trips ORDER BY 1").fetchall()]

    # 1. Every valid silver trip is in the fact table, and nothing else is.
    expected = 0
    for batch_id in batches:
        report_file = Path(data_root) / "silver" / "_dq" / f"{batch_id}.json"
        expected += json.loads(report_file.read_text())["trips"]["valid_rows"]
    fact_rows = scalar(con, f"SELECT count(*) FROM {SCHEMA}.fct_trips")

    # 2. Dockless trips were kept (as unknown station), not lost.
    unknown_expected = scalar(con, "SELECT count(*) FROM silver_trips WHERE is_valid AND start_station_id IS NULL")
    unknown_actual = scalar(con, f"""
        SELECT count(*) FROM {SCHEMA}.fct_trips f
        JOIN {SCHEMA}.dim_station s ON s.station_key = f.start_station_key
        WHERE s.is_unknown""")

    # 3. Every key in the fact has a matching dimension row ("no orphans").
    orphans = {}
    for fact_col, dim, dim_col in FOREIGN_KEYS:
        orphans[fact_col] = scalar(con, f"""
            SELECT count(*) FROM {SCHEMA}.fct_trips f
            LEFT JOIN {SCHEMA}.{dim} d ON d.{dim_col} = f.{fact_col}
            WHERE d.{dim_col} IS NULL""")

    result = {
        "batches": batches,
        "dq_valid_rows": expected,
        "fct_trips_rows": fact_rows,
        "unknown_start_station_trips": unknown_actual,
        "unknown_weather_trips": scalar(con, f"SELECT count(*) FROM {SCHEMA}.fct_trips WHERE weather_key = -1"),
        "orphan_keys": orphans,
    }
    problems = []
    if fact_rows != expected:
        problems.append(f"fct_trips has {fact_rows} rows but the DQ reports count {expected} valid trips")
    if unknown_actual != unknown_expected:
        problems.append(f"{unknown_actual} trips use the unknown start station, expected {unknown_expected}")
    problems += [f"{n} fct_trips.{col} values have no dimension row" for col, n in orphans.items() if n]
    if problems:
        raise GoldError("Gold does not reconcile: " + "; ".join(problems))
    return result


def build_gold(data_root: Path = config.DEFAULT_DATA_ROOT) -> dict:
    """Stage entry point: rebuild the gold_sql star schema. Returns row counts and checks."""
    data_root = Path(data_root)
    if not list((data_root / "silver" / "trips").glob("year_month=*/*.parquet")):
        raise GoldError(f"No silver trips under {data_root / 'silver'}; run cascade.clean first")
    sql_files = sorted(SQL_DIR.glob("*.sql"))
    todo.raise_if_unfinished(sql_files)
    try:
        con = duckdb.connect(str(warehouse_path(data_root)))
    except duckdb.IOException as err:
        raise GoldError(f"Cannot open {warehouse_path(data_root)}. Is another program using it? ({err})") from err

    try:
        create_silver_views(con, data_root)
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        for sql_file in sql_files:
            log.info("Running %s", sql_file.name)
            con.execute(sql_file.read_text())

        counts = {table: scalar(con, f"SELECT count(*) FROM {SCHEMA}.{table}") for table in TABLES}
        checks = reconcile(con, data_root)
    finally:
        con.close()

    for table, n in counts.items():
        log.info("  %-12s %9d rows", table, n)
    log.info("Reconciled: %d valid trips in silver = %d rows in fct_trips (%d dockless, %d without weather)",
             checks["dq_valid_rows"], checks["fct_trips_rows"],
             checks["unknown_start_station_trips"], checks["unknown_weather_trips"])
    return {"row_counts": counts, **checks}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the Week 4 star schema (gold_sql) from silver.")
    parser.add_argument("--data-root", type=Path, default=config.DEFAULT_DATA_ROOT)
    args = parser.parse_args(argv)

    config.setup_logging()
    try:
        build_gold(args.data_root)
    except GoldError as err:
        log.error("%s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

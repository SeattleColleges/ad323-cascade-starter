"""Week 5 tests: the dbt gold layer, its incremental fact, and the Parquet export.

dbt runs for real here (on the sample), so this file takes a few seconds longer
than the others.
"""

import duckdb
import pytest

from cascade import transform
from conftest import add_silver_trip, land_silver

pytestmark = pytest.mark.week05

SEPT_VALID = 48_766   # valid trips in the 2026-09 sample (from the DQ report)
OCT_VALID = 48_860    # valid trips in the 2026-10 sample


def fact_by_batch(data_root):
    """{year_month: (row_count, min loaded_at, max loaded_at)} from the dbt fact table."""
    with duckdb.connect(str(data_root / "warehouse.duckdb"), read_only=True) as con:
        rows = con.execute("""
            SELECT year_month, count(*), min(loaded_at), max(loaded_at)
            FROM gold.fct_trips GROUP BY year_month ORDER BY year_month""").fetchall()
    return {ym: (n, first, last) for ym, n, first, last in rows}


@pytest.fixture(scope="module")
def incremental_story(tmp_path_factory):
    """Build gold step by step and record what fct_trips looked like after each step."""
    data_root = tmp_path_factory.mktemp("dbt") / "data"
    steps = {}

    # Step 1: only September is in silver. First build creates the table.
    land_silver("2026-09", data_root)
    transform.transform_batch(None, data_root)
    steps["sept_only"] = fact_by_batch(data_root)

    # Step 2: October arrives. A normal incremental run should load only October.
    land_silver("2026-10", data_root)
    transform.transform_batch(None, data_root)
    steps["after_oct"] = fact_by_batch(data_root)

    # Step 3: a run with no new batch should change nothing.
    transform.transform_batch(None, data_root)
    steps["no_new_batch"] = fact_by_batch(data_root)

    # Step 4: rerun October on purpose (e.g. after re-cleaning it).
    transform.transform_batch("2026-10", data_root)
    steps["rerun_oct"] = fact_by_batch(data_root)

    return data_root, steps


# ----- Incremental fct_trips --------------------------------------------------------------


def test_first_build_loads_september(incremental_story):
    _, steps = incremental_story
    assert list(steps["sept_only"]) == ["2026-09"]
    assert steps["sept_only"]["2026-09"][0] == SEPT_VALID


def test_adding_october_processes_only_october(incremental_story):
    _, steps = incremental_story
    after = steps["after_oct"]
    assert after["2026-10"][0] == OCT_VALID
    # September rows were not rewritten: same count, same loaded_at timestamps.
    assert after["2026-09"] == steps["sept_only"]["2026-09"]
    # October rows were written after every September row.
    assert after["2026-10"][1] > steps["sept_only"]["2026-09"][2]


def test_run_without_new_batch_changes_nothing(incremental_story):
    _, steps = incremental_story
    assert steps["no_new_batch"] == steps["after_oct"]


def test_rerunning_a_batch_replaces_it_without_duplicates(incremental_story):
    _, steps = incremental_story
    rerun = steps["rerun_oct"]
    assert rerun["2026-10"][0] == OCT_VALID                         # replaced, not doubled
    assert rerun["2026-10"][1] > steps["after_oct"]["2026-10"][2]   # freshly written
    assert rerun["2026-09"] == steps["sept_only"]["2026-09"]        # September untouched


# ----- Export --------------------------------------------------------------------------------------


def test_every_gold_table_is_exported(incremental_story):
    data_root, _ = incremental_story
    for table in transform.GOLD_TABLES:
        exported = data_root / "gold" / f"{table}.parquet"
        with duckdb.connect(str(data_root / "warehouse.duckdb"), read_only=True) as con:
            in_warehouse = con.execute(f"SELECT count(*) FROM gold.{table}").fetchone()[0]
        assert duckdb.sql(f"SELECT count(*) FROM '{exported}'").fetchone()[0] == in_warehouse


def test_agg_station_daily_adds_up(incremental_story):
    data_root, _ = incremental_story
    starts, ends, net = duckdb.sql(f"""
        SELECT sum(trips_started), sum(trips_ended), sum(net_flow)
        FROM '{data_root}/gold/agg_station_daily.parquet'""").fetchone()
    assert starts == ends == SEPT_VALID + OCT_VALID   # every trip starts once and ends once
    assert net == 0                                   # bikes are never created or destroyed


# ----- Failing dbt tests block the export -------------------------------------------------------


def test_failed_dbt_test_stops_the_build(tmp_path):
    # A member type dbt's accepted_values test has never heard of.
    data_root = tmp_path / "data"
    land_silver("2026-09", data_root)
    add_silver_trip(data_root, "2026-09", "T_SUBSCRIBER", member_casual="subscriber")
    with pytest.raises(transform.TransformError, match="dbt build failed"):
        transform.transform_batch(None, data_root)
    assert not (data_root / "gold").exists()  # nothing exported

"""Week 4 tests: the star schema has the right grain, keys, and totals."""

import duckdb
import pytest

from cascade import gold
from conftest import BATCHES, add_silver_trip, land_silver

pytestmark = pytest.mark.week04


@pytest.fixture(scope="module")
def gold_root(tmp_path_factory):
    """Silver for both sample batches (checkpoint-03 or your Week 3), with gold built once."""
    data_root = tmp_path_factory.mktemp("gold") / "data"
    for batch_id in BATCHES:
        land_silver(batch_id, data_root)
    gold.build_gold(data_root)
    return data_root


def query(data_root, sql):
    with duckdb.connect(str(gold.warehouse_path(data_root)), read_only=True) as con:
        return con.execute(sql).fetchall()


def one(data_root, sql):
    return query(data_root, sql)[0][0]


# ----- Totals reconcile ---------------------------------------------------------


def test_fact_has_exactly_the_valid_trips(gold_root):
    # From the DQ reports: 48,766 valid in 2026-09 + 48,860 in 2026-10.
    assert one(gold_root, "SELECT count(*) FROM gold_sql.fct_trips") == 97_626


def test_invalid_trips_are_excluded(gold_root):
    silver = f"read_parquet('{gold_root}/silver/trips/*/*.parquet')"
    leaked = one(gold_root, f"""
        SELECT count(*) FROM gold_sql.fct_trips f
        JOIN {silver} s USING (ride_id) WHERE NOT s.is_valid""")
    assert leaked == 0


# ----- Grain: one row per key ---------------------------------------------------------


@pytest.mark.parametrize("table, key", [
    ("fct_trips", "ride_id"),
    ("dim_station", "station_key"),
    ("dim_station", "station_id"),
    ("dim_date", "date_key"),
    ("dim_weather", "weather_key"),
])
def test_keys_are_unique_and_not_null(gold_root, table, key):
    rows, distinct, nulls = query(gold_root, f"""
        SELECT count(*), count(DISTINCT {key}), count(*) FILTER (WHERE {key} IS NULL)
        FROM gold_sql.{table}""")[0]
    assert rows == distinct
    assert nulls == 0


@pytest.mark.parametrize("fact_col, dim, dim_col", gold.FOREIGN_KEYS)
def test_every_fact_key_has_a_dimension_row(gold_root, fact_col, dim, dim_col):
    orphans = one(gold_root, f"""
        SELECT count(*) FROM gold_sql.fct_trips f
        LEFT JOIN gold_sql.{dim} d ON d.{dim_col} = f.{fact_col}
        WHERE d.{dim_col} IS NULL""")
    assert orphans == 0


# ----- Unknown members --------------------------------------------------------------------


def test_dockless_trips_point_at_the_unknown_station(gold_root):
    unknown = query(gold_root, "SELECT station_key, station_name FROM gold_sql.dim_station WHERE is_unknown")
    assert unknown == [(one(gold_root, "SELECT md5('UNKNOWN')"), "Unknown station (dockless)")]
    dockless = one(gold_root, f"SELECT count(*) FROM gold_sql.fct_trips WHERE start_station_key = '{unknown[0][0]}'")
    assert dockless == 19_325


def test_trip_without_weather_gets_unknown_weather_key(tmp_path):
    # A trip in the September file that started on Aug 31: no weather for that hour.
    data_root = tmp_path / "data"
    land_silver("2026-09", data_root)
    add_silver_trip(data_root, "2026-09", "T_AUG31",
                    started_at="2026-08-31 23:50:00", ended_at="2026-09-01 00:05:00")
    gold.build_gold(data_root)
    assert one(data_root, "SELECT weather_key FROM gold_sql.fct_trips WHERE ride_id = 'T_AUG31'") == -1
    assert one(data_root, "SELECT date_key FROM gold_sql.fct_trips WHERE ride_id = 'T_AUG31'") == 20260831


# ----- Dimension contents ---------------------------------------------------------------------


def test_dim_date_flags(gold_root):
    labor_day = query(gold_root, """
        SELECT day_name, is_weekend, is_holiday, holiday_name
        FROM gold_sql.dim_date WHERE date_key = 20260907""")
    assert labor_day == [("Monday", False, True, "Labor Day")]
    # Sep 1 - Oct 31 has no gaps
    assert one(gold_root, "SELECT max(full_date) - min(full_date) + 1 = count(*) FROM gold_sql.dim_date")


def test_weather_matches_trip_start_hour(gold_root):
    mismatched = one(gold_root, """
        SELECT count(*) FROM gold_sql.fct_trips f
        JOIN gold_sql.dim_weather w USING (weather_key)
        WHERE w.observed_at <> date_trunc('hour', f.started_at)""")
    assert mismatched == 0


def test_distance_is_sensible(gold_root):
    # Trips that return to the same real station are short (station coordinates
    # vary a little with e-bike GPS). Skip the unknown station: two dockless
    # ends share that key but can be kilometres apart.
    assert one(gold_root, """
        SELECT max(distance_km) FROM gold_sql.fct_trips
        WHERE start_station_key = end_station_key
          AND start_station_key <> md5('UNKNOWN')""") < 1
    # Nothing crosses the whole service area (~55 km north to south).
    assert one(gold_root, "SELECT max(distance_km) FROM gold_sql.fct_trips") < 60


# ----- Rerun and errors ---------------------------------------------------------------------------


def test_rebuild_is_idempotent(gold_root):
    before = gold.build_gold(gold_root)["row_counts"]
    after = gold.build_gold(gold_root)["row_counts"]
    assert before == after


def test_gold_needs_silver(data_root):
    with pytest.raises(gold.GoldError, match="run cascade.clean first"):
        gold.build_gold(data_root)

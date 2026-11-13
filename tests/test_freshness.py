"""Week 10 tests: dbt checks how fresh silver is, warning after 1 day and failing after 7."""

import datetime as dt

import duckdb
import pytest
import yaml

from cascade import todo, transform
from conftest import REPO_ROOT, land_silver

pytestmark = pytest.mark.week10

SOURCES = REPO_ROOT / "dbt_project" / "models" / "staging" / "_sources.yml"


def silver_tables():
    todo.raise_if_unfinished([SOURCES])
    sources = yaml.safe_load(SOURCES.read_text())["sources"]
    silver = next(s for s in sources if s["name"] == "silver")
    return silver


def test_silver_has_freshness_rules():
    silver = silver_tables()
    freshness = silver.get("config", {}).get("freshness") or silver.get("freshness")
    assert freshness["warn_after"] == {"count": 1, "period": "day"}
    assert freshness["error_after"] == {"count": 7, "period": "day"}
    loaded_at = silver.get("config", {}).get("loaded_at_field") or silver.get("loaded_at_field")
    assert loaded_at == "cleaned_at"


def set_cleaned_at(data_root, when):
    for path in (data_root / "silver").rglob("*.parquet"):
        con = duckdb.connect()
        con.execute(f"COPY (SELECT * REPLACE (TIMESTAMP '{when:%Y-%m-%d %H:%M:%S}' AS cleaned_at) "
                    f"FROM read_parquet('{path}', hive_partitioning = false)) TO '{path}.tmp' (FORMAT parquet)")
        con.close()
        path.with_name(path.name + ".tmp").replace(path)


def test_fresh_silver_passes_and_stale_silver_fails(tmp_path):
    silver_tables()
    data_root = tmp_path / "data"
    land_silver("2026-10", data_root)
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    set_cleaned_at(data_root, now)
    transform.check_freshness(data_root)                          # fresh: passes
    set_cleaned_at(data_root, now - dt.timedelta(days=8))
    with pytest.raises(transform.TransformError):
        transform.check_freshness(data_root)                      # 8 days old: fails

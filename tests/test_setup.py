"""Week 1 tests (provided): your environment works and the frozen sample is intact.

If these pass on your laptop and in GitHub Actions, Week 1's setup is done.
"""

import sys
from importlib.metadata import version

import duckdb
import pytest

from cascade import config
from conftest import BATCHES, SAMPLE_DIR

pytestmark = pytest.mark.week01


def test_python_and_duckdb_versions():
    assert sys.version_info[:2] == (3, 12)               # uv installs exactly this Python
    assert version("duckdb") == "1.5.6"                 # pinned in pyproject.toml / uv.lock


@pytest.mark.parametrize("batch_id", BATCHES)
def test_sample_trips_are_intact(batch_id):
    path = SAMPLE_DIR / batch_id / config.trips_filename(batch_id)
    rows = duckdb.sql(f"SELECT count(*) FROM read_csv('{path}', header = true, all_varchar = true)")
    assert rows.fetchone()[0] == 50_000


@pytest.mark.parametrize("batch_id, hours", [("2026-09", 30 * 24), ("2026-10", 31 * 24)])
def test_sample_weather_covers_every_hour(batch_id, hours):
    path = SAMPLE_DIR / batch_id / config.WEATHER_FILENAME
    count = duckdb.sql(f"SELECT len(hourly.time) FROM read_json('{path}')").fetchone()[0]
    assert count == hours


def test_machine_check_runs():
    from cascade import doctor
    results = {check: status for status, check, _ in doctor.checks(need_docker=False)}
    assert results["Python"] == "OK"
    assert results["Sample data"] == "OK"

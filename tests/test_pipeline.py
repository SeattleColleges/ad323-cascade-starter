"""Week 6 tests: the Prefect flow runs the spine, fails visibly, and recovers on rerun.

prefect_test_harness starts a throwaway Prefect server for these tests, so they
never touch your own Prefect history.
"""

import json

import duckdb
import pytest
from prefect.testing.utilities import prefect_test_harness

from cascade import pipeline
from conftest import SAMPLE_DIR, checkpoint, land_silver, skip_if_stub

pytestmark = pytest.mark.week06

# With checkpoint-03 in checkpoints/, the flow starts from that silver
# (--start silver), so Week 6 doesn't need your Weeks 2-3. Otherwise it runs
# the whole spine with your own ingest and clean.
START = "silver" if checkpoint("checkpoint-03") else "bronze"


@pytest.fixture(scope="module", autouse=True)
def prefect_server():
    with prefect_test_harness():
        yield


def raise_if_todo(state):
    """A flow that failed on a TODO stub should count as "not implemented", not as a failure."""
    if state.is_failed():
        error = state.result(raise_on_failure=False)
        if isinstance(error, NotImplementedError) and str(error).startswith("TODO(week"):
            raise error
    return state


def run(batch_id, data_root):
    """Run the flow for one batch and return its final Prefect state."""
    if START == "silver":
        land_silver(batch_id, data_root)
    return raise_if_todo(pipeline.cascade_pipeline(batch_id, str(data_root), str(SAMPLE_DIR), START,
                                                   return_state=True))


def fact_rows(data_root):
    return duckdb.sql(f"SELECT count(*) FROM '{data_root}/gold/fct_trips.parquet'").fetchone()[0]


def test_flow_runs_the_whole_spine(tmp_path):
    data_root = tmp_path / "data"
    state = run("2026-09", data_root)
    assert state.is_completed()
    result = state.result()
    assert result["dq"]["valid_rows"] == 48_766
    assert result["gold_row_counts"]["fct_trips"] == 48_766
    assert fact_rows(data_root) == 48_766


def test_backfill_runs_batches_oldest_first(tmp_path):
    data_root = tmp_path / "data"
    if START == "silver":
        for batch_id in ["2026-09", "2026-10"]:
            land_silver(batch_id, data_root)
    # Given out of order on purpose: the incremental fact needs time order.
    state = raise_if_todo(pipeline.backfill(["2026-10", "2026-09"], str(data_root), str(SAMPLE_DIR), START,
                                            return_state=True))
    assert state.is_completed()
    assert [r["batch_id"] for r in state.result()] == ["2026-09", "2026-10"]
    assert fact_rows(data_root) == 48_766 + 48_860


def test_intentional_failure_then_recovery(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    assert run("2026-09", data_root).is_completed()
    switch = "CASCADE_FAIL_CLEAN" if START == "bronze" else "CASCADE_FAIL_DBT"

    # 1. Switch the failure on: October's run fails on purpose.
    monkeypatch.setenv(switch, "1")
    failed = run("2026-10", data_root)
    assert failed.is_failed()
    assert fact_rows(data_root) == 48_766           # gold still holds only September
    manifest = data_root / "bronze" / "_manifest.json"
    manifest_after_failure = manifest.read_text() if START == "bronze" else None

    # 2. Fix the cause and rerun the same batch.
    monkeypatch.delenv(switch)
    recovered = run("2026-10", data_root)
    assert recovered.is_completed()
    assert fact_rows(data_root) == 48_766 + 48_860

    # Ingest already succeeded during the failed run, so the rerun skipped it.
    if START == "bronze":
        assert json.loads(manifest.read_text()) == json.loads(manifest_after_failure)


def test_rerunning_a_batch_is_idempotent(tmp_path):
    data_root = tmp_path / "data"
    assert run("2026-09", data_root).is_completed()
    assert run("2026-09", data_root).is_completed()
    assert fact_rows(data_root) == 48_766


def test_ingest_task_retries():
    skip_if_stub(pipeline.ingest_task)
    assert pipeline.ingest_task.retries == 3
    assert pipeline.ingest_task.retry_delay_seconds == 10


def test_bad_batch_id_fails_the_flow(tmp_path):
    skip_if_stub(pipeline.cascade_pipeline)
    assert pipeline.cascade_pipeline("2026-13", str(tmp_path / "data"), str(SAMPLE_DIR),
                                     return_state=True).is_failed()

"""Shared test fixtures and helpers (provided; you don't need to change this file).

Every test gets its own empty temporary `data_root`, so tests never touch the
real data/ folder and never depend on each other. Data comes from the frozen
sample, so no test needs the network.

Each week's tests are marked with that week:   uv run pytest -m week03

Unfinished work is skipped, not failed. A function that still raises
NotImplementedError("TODO(week03): ...") turns its tests into SKIPPED, so CI
stays green while later weeks are still TODO. To prove a week is done:

    uv run pytest -m week03 --require-implemented     # a TODO now FAILS

Every week can start from a checkpoint. The tests for Week N never need your
code from earlier weeks if the matching checkpoint is in checkpoints/:
  * bronze for Week 3 is copied straight from data/sample (no Week 2 code needed);
  * silver for Weeks 4-9 comes from checkpoints/checkpoint-03/ if it is there,
    otherwise from your own Week 3 clean;
  * gold for Week 11 comes from checkpoints/checkpoint-10/ if it is there,
    otherwise from your own pipeline.
"""

import inspect
import shutil
from pathlib import Path

import pytest

from cascade import config, ingest

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = REPO_ROOT / "data" / "sample"
CHECKPOINTS_DIR = REPO_ROOT / "checkpoints"
BATCHES = ["2026-09", "2026-10"]


# ----- TODO -> skipped -----------------------------------------------------------------


def pytest_addoption(parser):
    parser.addoption("--require-implemented", action="store_true",
                     help="treat TODO(weekNN) NotImplementedErrors as failures, not skips")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if call.excinfo is None or not call.excinfo.errisinstance(NotImplementedError):
        return
    message = str(call.excinfo.value)
    if not message.startswith("TODO(week") or item.config.getoption("--require-implemented"):
        return
    report.outcome = "skipped"
    report.longrepr = (str(item.path), item.location[1] or 0, f"not implemented yet: {message}")


def skip_if_stub(func) -> None:
    """For tests that inspect a function instead of calling it: a stub counts as TODO."""
    source = inspect.getsource(getattr(func, "fn", func))   # .fn: the function inside a Prefect task
    for line in source.splitlines():
        if "TODO(week" in line:
            raise NotImplementedError(line[line.index("TODO(week"):].rstrip("\"')"))


def checkpoint(name: str) -> Path | None:
    """checkpoints/<name>/ if you have added that checkpoint, else None."""
    path = CHECKPOINTS_DIR / name
    return path if path.is_dir() else None


# ----- Building the layers a test starts from ------------------------------------------


def land_bronze(batch_id: str, data_root: Path, source_dir: Path = SAMPLE_DIR) -> None:
    """Copy one batch into bronze and record it in the manifest, without your Week 2 code.

    This is what a correct ingest produces for a local folder; tests for later
    weeks use it so they never depend on Week 2.
    """
    manifest = ingest.load_manifest(data_root)
    for dataset, filename in [("trips", config.trips_filename(batch_id)), ("weather", config.WEATHER_FILENAME)]:
        target = Path(data_root) / "bronze" / batch_id / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(Path(source_dir) / batch_id / filename, target)
        rows = ingest.count_csv_rows(target) if dataset == "trips" else ingest.count_weather_rows(target)
        manifest.setdefault(batch_id, {})[dataset] = {
            "file": f"{batch_id}/{filename}", "source": str(Path(source_dir) / batch_id / filename),
            "sha256": ingest.sha256_of(target), "rows": rows, "bytes": target.stat().st_size,
            "loaded_at": ingest.utc_now(),
        }
    ingest.save_manifest(data_root, manifest)


def land_silver(batch_id: str, data_root: Path) -> None:
    """Silver for one batch: from checkpoint-03 if present, else bronze + your Week 3 clean."""
    source = checkpoint("checkpoint-03")
    if source is None:
        from cascade import clean
        land_bronze(batch_id, data_root)
        clean.clean_batch(batch_id, data_root)
        return
    silver = Path(data_root) / "silver"
    for dataset in ["trips", "weather"]:
        shutil.copytree(source / "silver" / dataset / f"year_month={batch_id}",
                        silver / dataset / f"year_month={batch_id}", dirs_exist_ok=True)
    (silver / "_dq").mkdir(parents=True, exist_ok=True)
    for suffix in [".json", ".md"]:
        shutil.copyfile(source / "silver" / "_dq" / f"{batch_id}{suffix}", silver / "_dq" / f"{batch_id}{suffix}")


def add_silver_trip(data_root: Path, batch_id: str, ride_id: str, **changes) -> None:
    """Plant one extra trip in a batch's silver: a copy of a valid docked trip with `changes`.

    The batch's DQ report is updated to match, so gold still reconciles.
    """
    import json

    import duckdb

    partition = Path(data_root) / "silver" / "trips" / f"year_month={batch_id}"
    files = sorted(partition.glob("*.parquet"))
    assert len(files) == 1, f"expected one Parquet file in {partition}, found {len(files)}"
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{files[0]}', hive_partitioning = false)")
    types = dict(con.execute("SELECT column_name, column_type FROM (DESCRIBE t)").fetchall())
    values = {"ride_id": ride_id, **changes}
    replace = ", ".join(f"CAST(? AS {types[column]}) AS {column}" for column in values)
    con.execute(f"""
        INSERT INTO t SELECT * REPLACE ({replace}) FROM (
            SELECT * FROM t WHERE is_valid AND start_station_id IS NOT NULL AND end_station_id IS NOT NULL
            ORDER BY ride_id LIMIT 1)""", list(values.values()))
    tmp = files[0].with_suffix(".tmp")
    con.execute(f"COPY t TO '{tmp}' (FORMAT parquet)")
    con.close()
    tmp.replace(files[0])

    report_path = Path(data_root) / "silver" / "_dq" / f"{batch_id}.json"
    report = json.loads(report_path.read_text())
    report["trips"]["silver_rows"] += 1
    report["trips"]["valid_rows"] += 1
    report_path.write_text(json.dumps(report, indent=2))


def land_gold(data_root: Path, before_transform=None) -> None:
    """Gold Parquet for both sample batches: from checkpoint-10 if present, else your pipeline.

    `before_transform(data_root)` runs just before dbt, e.g. to add stream windows.
    """
    source = checkpoint("checkpoint-10")
    if source is not None:
        shutil.copytree(source / "gold", Path(data_root) / "gold", dirs_exist_ok=True)
        return
    from cascade import transform
    for batch_id in BATCHES:
        land_silver(batch_id, data_root)
    if before_transform is not None:
        before_transform(data_root)
    transform.transform_batch(None, data_root)


# ----- Fixtures ------------------------------------------------------------------------


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    """An empty data root for one test."""
    return tmp_path / "data"


@pytest.fixture
def bronze_root(data_root: Path) -> Path:
    """A data root whose bronze layer already holds both sample batches."""
    for batch_id in BATCHES:
        land_bronze(batch_id, data_root)
    return data_root

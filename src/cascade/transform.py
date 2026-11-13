"""Week 5: build gold with dbt, then export it to Parquet for the dashboard.

    uv run python -m cascade.transform                     # load any new batches
    uv run python -m cascade.transform --batch 2026-10     # (re)load exactly this batch
    uv run python -m cascade.transform --full-refresh      # rebuild fct_trips from scratch
    uv run python -m cascade.transform --data-root data/runs/sample

What happens:
  1. `dbt build` runs every model, seed, and test in dbt_project/ in dependency
     order. If any test fails, the build fails and nothing is exported.
  2. Each gold table is copied to <data_root>/gold/<table>.parquet. The
     dashboard reads these files, never the warehouse, so it can't collide
     with a pipeline run (DuckDB allows only one writer at a time).

dbt runs as a separate process (the same as typing `dbt build` yourself).
That matters: DuckDB locks the warehouse file while a process has it open, and
dbt's Python API keeps it open until the whole program exits. A separate
process releases the lock as soon as dbt finishes, so you can query the
warehouse from another terminal while Prefect (Week 6) keeps running.
"""

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

from cascade import config, todo

log = logging.getLogger("cascade.transform")

REPO_ROOT = config.REPO_ROOT
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"
GOLD_SCHEMA = "gold"
GOLD_TABLES = ["fct_trips", "dim_station", "dim_date", "dim_weather", "agg_station_daily"]
# Built only when their inputs exist: Week 8 stream windows, Week 9 riders file.
OPTIONAL_TABLES = ["fct_station_availability", "dim_rider"]
SALT_PATTERN = re.compile(r"^[A-Za-z0-9_-]{16,}$")
# The model files behind each optional tag (paths inside dbt_project/).
OPTIONAL_MODELS = {
    "stream": ["models/stream"],
    "pii": ["models/restricted", "models/marts/dim_rider.sql"],
}


class TransformError(Exception):
    """dbt failed (a model error or a failing test) or the export failed."""


def run_dbt(args: list[str], data_root: Path) -> None:
    """Run one dbt command against the warehouse in `data_root`."""
    # The dbt installed in this project's virtual environment (dbt.exe on Windows).
    dbt = shutil.which("dbt", path=str(Path(sys.executable).parent))
    if dbt is None:
        raise TransformError("dbt is not installed in this environment; run `uv sync`")
    command = [dbt, *args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
    # profiles.yml and the sources read these variables to find the data.
    # Values from .env come first, so a real environment variable wins.
    env = {**config.load_env_file(), **os.environ,
           "CASCADE_DATA_ROOT": str(Path(data_root).resolve()),
           "CASCADE_SAMPLE_DIR": str(REPO_ROOT / config.SAMPLE_DIR)}

    log.info("dbt %s", " ".join(args))
    result = subprocess.run(command, env=env)
    if result.returncode != 0:
        raise TransformError(f"dbt {args[0]} failed (exit code {result.returncode}); see the dbt output above")


def build(batch_id: str | None = None, data_root: Path = config.DEFAULT_DATA_ROOT,
          full_refresh: bool = False) -> None:
    """Run `dbt build`. With a batch_id, fct_trips (re)loads exactly that batch."""
    args = ["build"]
    if batch_id is not None:
        config.parse_batch_id(batch_id)
        args += ["--vars", json.dumps({"batch_id": batch_id})]
    if full_refresh:
        args.append("--full-refresh")
    # Skip optional models whose input files don't exist yet, or that still
    # have TODO markers, instead of failing.
    excluded = []
    if not has_stream_output(data_root):
        excluded.append("tag:stream")        # Week 8 not run
    elif todo.unfinished(optional_model_files("stream")):
        log.warning("Skipping the stream models: they still have TODO(week08) markers")
        excluded.append("tag:stream")
    if not has_riders(data_root):
        excluded.append("tag:pii")           # riders not generated
    elif todo.unfinished(optional_model_files("pii")):
        log.warning("Skipping the PII models: they still have TODO(week09) markers")
        excluded.append("tag:pii")
    else:
        check_pii_salt()                     # Week 9: refuse to hash PII without a proper salt
    todo.raise_if_unfinished(core_model_files())
    if excluded:
        args += ["--exclude", *excluded]
    run_dbt(args, data_root)


def _sql_files(entries: list[str]) -> list[Path]:
    files = []
    for entry in entries:
        path = DBT_PROJECT_DIR / entry
        files += sorted(path.rglob("*.sql")) if path.is_dir() else [path]
    return files


def optional_model_files(tag: str) -> list[Path]:
    """The .sql files behind an optional tag ("stream" or "pii")."""
    return _sql_files(OPTIONAL_MODELS[tag])


def core_model_files() -> list[Path]:
    """Every model and test .sql file that is always built."""
    optional = {f for tag in OPTIONAL_MODELS for f in optional_model_files(tag)}
    return [f for f in _sql_files(["models", "tests"]) if f not in optional]


def require_models(tag: str) -> None:
    """Raise NotImplementedError if the models behind `tag` still have TODO markers."""
    todo.raise_if_unfinished(optional_model_files(tag))


def has_stream_output(data_root: Path) -> bool:
    return any((Path(data_root) / "stream" / "station_availability").glob("*.parquet"))


def has_riders(data_root: Path) -> bool:
    return (Path(data_root) / "restricted" / "riders.csv").exists()


def check_pii_salt() -> None:
    """The salt must exist and be long and random enough to make hashes unguessable."""
    salt = os.environ.get("CASCADE_PII_SALT") or config.load_env_file().get("CASCADE_PII_SALT", "")
    if not SALT_PATTERN.match(salt):
        raise TransformError(
            "CASCADE_PII_SALT is missing or too weak (need 16+ letters, digits, - or _). "
            "Copy .env.example to .env and set it to the output of: "
            "uv run python -c \"import secrets; print(secrets.token_hex(32))\"")


def export_gold(data_root: Path = config.DEFAULT_DATA_ROOT) -> dict:
    """Copy every gold table to <data_root>/gold/<table>.parquet. Returns row counts."""
    gold_dir = Path(data_root) / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    con = duckdb.connect(str(Path(data_root) / "warehouse.duckdb"))
    try:
        built = {row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [GOLD_SCHEMA]).fetchall()}
        for table in GOLD_TABLES + [t for t in OPTIONAL_TABLES if t in built]:
            target = gold_dir / f"{table}.parquet"
            tmp = gold_dir / f"{table}.parquet.tmp"
            con.execute(f"COPY {GOLD_SCHEMA}.{table} TO '{tmp}' (FORMAT parquet)")
            os.replace(tmp, target)  # readers never see a half-written file
            counts[table] = con.execute(f"SELECT count(*) FROM {GOLD_SCHEMA}.{table}").fetchone()[0]
    finally:
        con.close()
    for table, n in counts.items():
        log.info("  exported %-18s %9d rows", table, n)
    return counts


def check_freshness(data_root: Path = config.DEFAULT_DATA_ROOT) -> None:
    """Week 10: `dbt source freshness` on silver. Warns after 1 day, fails after 7."""
    todo.raise_if_unfinished([DBT_PROJECT_DIR / "models" / "staging" / "_sources.yml"])
    run_dbt(["source", "freshness", "--select", "source:silver"], data_root)


def transform_batch(batch_id: str | None = None, data_root: Path = config.DEFAULT_DATA_ROOT,
                    full_refresh: bool = False) -> dict:
    """Stage entry point: dbt build, then export gold. Returns gold row counts."""
    build(batch_id, data_root, full_refresh)
    return export_gold(data_root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build gold with dbt and export it to Parquet.")
    parser.add_argument("--batch", help="(re)load exactly this batch into fct_trips, e.g. 2026-10")
    parser.add_argument("--data-root", type=Path, default=config.DEFAULT_DATA_ROOT)
    parser.add_argument("--full-refresh", action="store_true", help="rebuild fct_trips from all of silver")
    parser.add_argument("--freshness", action="store_true", help="only check how fresh silver is (Week 10)")
    args = parser.parse_args(argv)

    config.setup_logging()
    try:
        if args.freshness:
            check_freshness(args.data_root)
        else:
            transform_batch(args.batch, args.data_root, args.full_refresh)
    except (TransformError, ValueError) as err:
        log.error("%s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Week 6: orchestration. One Prefect flow runs the whole spine for a batch.

YOU WRITE: the tasks, both flows and the daily schedule (TODO(week06)).
PROVIDED:  export_gold_task (a finished task, the worked example),
           read_dq_report(), and the command line.

    ingest -> clean -> dbt build -> export gold

Commands (from the repo root):

    uv run python -m cascade.pipeline run --batch 2026-10          # one batch, now
    uv run python -m cascade.pipeline backfill --batch 2026-09 2026-10
    uv run python -m cascade.pipeline serve --batch 2026-10        # daily schedule, keeps running

By default the flow reads the frozen sample (--source data/sample), so graded
runs never need the network. Use --source web for the real downloads.

Why this is safe to rerun: every stage is idempotent. Ingest skips files that
are already in bronze, clean replaces the batch's silver partition, and dbt
replaces the batch's rows in fct_trips. After a failure you fix the cause
and simply run the same batch again.

Practice failure: set CASCADE_FAIL_CLEAN=1 and the clean task raises an error
(CASCADE_FAIL_DBT=1 does the same in the dbt task). See the README for the
walkthrough in the Prefect UI.

Starting from silver: --start silver skips ingest and clean and builds gold
from the silver already in <data_root>/silver, for example silver from
checkpoint-03 if your own Weeks 2-3 aren't working yet.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.schedules import Cron

from cascade import clean, config, ingest, transform

# prefect.toml lists "cascade" under extra_loggers, so Prefect shows our
# modules' messages in the terminal and the UI. Make sure INFO messages pass.
logging.getLogger("cascade").setLevel(logging.INFO)

DEFAULT_SOURCE = str(config.SAMPLE_DIR)
DAILY_CRON = "0 6 * * *"  # every day at 06:00, Chicago time (see serve below)


# =============================================================================
# Tasks: thin wrappers around the stage functions from Weeks 2-5.
# Prefect tracks each task's state, logs, and retries.
# =============================================================================


@task
def ingest_task(batch_id: str, data_root: str, source: str) -> dict:
    """Land trips and weather in bronze (ingest.ingest_batch).

    Downloads can fail for a moment, so this task must retry: 3 retries,
    10 seconds apart. Set that in the @task(...) decorator.
    """
    raise NotImplementedError("TODO(week06): ingest_task")


@task
def clean_task(batch_id: str, data_root: str) -> dict:
    """Bronze -> silver (clean.clean_batch). Returns the data-quality report.

    Practice failure: if the environment variable CASCADE_FAIL_CLEAN is "1",
    raise RuntimeError("Intentional failure: ...") before cleaning.
    """
    raise NotImplementedError("TODO(week06): clean_task")


@task
def dbt_build_task(batch_id: str, data_root: str) -> None:
    """dbt build for this batch (transform.build). Fails if any dbt test fails.

    Practice failure: if CASCADE_FAIL_DBT is "1", raise RuntimeError first.
    """
    raise NotImplementedError("TODO(week06): dbt_build_task")


@task
def export_gold_task(data_root: str) -> dict:
    """WORKED EXAMPLE. Copy the gold tables to Parquet for the dashboard. Returns row counts."""
    return transform.export_gold(data_root)


def read_dq_report(batch_id: str, data_root: str) -> dict:
    """The data-quality report clean wrote for this batch (silver/_dq/<batch>.json)."""
    path = Path(data_root) / "silver" / "_dq" / f"{batch_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"No silver for {batch_id}: {path} is missing. "
                                "Run with --start bronze, or add checkpoint-03's silver first.")
    return json.loads(path.read_text())


# =============================================================================
# Flows
# =============================================================================


@flow(name="cascade-pipeline")
def cascade_pipeline(batch_id: str, data_root: str = str(config.DEFAULT_DATA_ROOT),
                     source: str = DEFAULT_SOURCE, start: str = "bronze") -> dict:
    """Run the whole spine for one batch. Each task starts only if the one before succeeded.

    1. Check the batch id (config.parse_batch_id) and `start`.
    2. start="bronze": ingest_task, then clean_task (its result is the DQ report).
       start="silver": skip both and use read_dq_report() (silver is already there,
       e.g. from checkpoint-03).
    3. dbt_build_task, then export_gold_task.
    4. Log a summary and return
         {"batch_id": batch_id, "dq": report["trips"], "gold_row_counts": <export counts>}
    """
    raise NotImplementedError("TODO(week06): cascade_pipeline")


@flow(name="cascade-backfill")
def backfill(batch_ids: list[str], data_root: str = str(config.DEFAULT_DATA_ROOT),
             source: str = DEFAULT_SOURCE, start: str = "bronze") -> list[dict]:
    """Run cascade_pipeline for several batches, OLDEST FIRST, and return their results.

    Order matters: the incremental fact table loads "batches newer than the
    newest one loaded", so batches must arrive in time order.
    """
    raise NotImplementedError("TODO(week06): backfill")


def serve_daily(batch_id: str, cron: str, data_root: str, source: str, start: str) -> None:
    """Keep running and start cascade_pipeline on a schedule (Ctrl+C stops it).

    Use cascade_pipeline.serve(...) with a Cron schedule in config.TIMEZONE
    (without a time zone, cron times are UTC) and the flow's parameters.
    """
    raise NotImplementedError("TODO(week06): serve_daily")


# =============================================================================
# Command line
# =============================================================================


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Cascade pipeline with Prefect.")
    commands = parser.add_subparsers(dest="command", required=True)

    run_cmd = commands.add_parser("run", help="run one batch now")
    run_cmd.add_argument("--batch", required=True)

    backfill_cmd = commands.add_parser("backfill", help="run several batches in order")
    backfill_cmd.add_argument("--batch", nargs="+", required=True)

    serve_cmd = commands.add_parser("serve", help="keep running and start the flow on a schedule")
    serve_cmd.add_argument("--batch", required=True, help="batch the scheduled run processes")
    serve_cmd.add_argument("--cron", default=DAILY_CRON, help=f'schedule (default "{DAILY_CRON}")')

    for sub in (run_cmd, backfill_cmd, serve_cmd):
        sub.add_argument("--data-root", default=str(config.DEFAULT_DATA_ROOT))
        sub.add_argument("--source", default=DEFAULT_SOURCE, help='"web" or a folder (default data/sample)')
        sub.add_argument("--start", choices=["bronze", "silver"], default="bronze",
                         help="silver: skip ingest and clean, use the silver already there")

    args = parser.parse_args(argv)

    if args.command == "run":
        state = cascade_pipeline(args.batch, args.data_root, args.source, args.start, return_state=True)
        return 0 if state.is_completed() else 1

    if args.command == "backfill":
        state = backfill(args.batch, args.data_root, args.source, args.start, return_state=True)
        return 0 if state.is_completed() else 1

    serve_daily(args.batch, args.cron, args.data_root, args.source, args.start)
    return 0


if __name__ == "__main__":
    sys.exit(main())

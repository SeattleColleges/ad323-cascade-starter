"""Week 11: one command that runs the whole sample pipeline and opens the dashboard.

    uv sync                              # once: setup
    uv run python -m cascade.demo        # pipeline on the frozen sample, then the dashboard

Steps: the Prefect backfill (ingest -> clean -> dbt build -> export gold) for
both sample batches, then Streamlit on dashboard/app.py. Everything is
idempotent, so running it again is safe and quick.

    --no-dashboard   only run the pipeline
    --data-root      where to build (default data/)
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from cascade import config, pipeline

SAMPLE_BATCHES = ["2026-09", "2026-10"]
APP = config.REPO_ROOT / "dashboard" / "app.py"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the sample pipeline, then open the dashboard.")
    parser.add_argument("--data-root", default=str(config.DEFAULT_DATA_ROOT))
    parser.add_argument("--no-dashboard", action="store_true")
    args = parser.parse_args(argv)

    state = pipeline.backfill(SAMPLE_BATCHES, args.data_root, str(config.SAMPLE_DIR), return_state=True)
    if not state.is_completed():
        print("The pipeline failed; see the log above. The dashboard was not started.")
        return 1
    if args.no_dashboard:
        return 0

    print("Opening the dashboard at http://127.0.0.1:8501 (Ctrl+C to stop)")
    env = {**os.environ, "CASCADE_DATA_ROOT": str(Path(args.data_root).resolve())}
    return subprocess.run([sys.executable, "-m", "streamlit", "run", str(APP)], env=env).returncode


if __name__ == "__main__":
    sys.exit(main())

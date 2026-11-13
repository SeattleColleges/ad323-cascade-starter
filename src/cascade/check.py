"""CI check (provided): run the sample pipeline as far as it is written.

    uv run python -m cascade.check

Runs ingest -> clean -> riders -> dbt build + export -> freshness on the frozen
sample, in a throwaway data root (data/runs/check, deleted first).

  * A stage that is still TODO stops the check politely, with exit code 0, so
    CI stays green while later weeks are unfinished.
  * Any real error (a crash, a failing dbt test) exits with 1 and turns CI red.
"""

import argparse
import logging
import shutil
import sys
from pathlib import Path

from cascade import clean, config, ingest, riders, transform

log = logging.getLogger("cascade.check")


def stages(data_root: Path, batches: list[str]):
    """(name, function) for each step, in order."""
    yield "ingest", lambda: [ingest.ingest_batch(b, data_root, source=str(config.SAMPLE_DIR)) for b in batches]
    yield "clean", lambda: [clean.clean_batch(b, data_root) for b in batches]
    yield "riders", lambda: riders.write_riders(data_root)
    yield "dbt build + export", lambda: transform.transform_batch(None, data_root)
    yield "freshness", lambda: transform.check_freshness(data_root)


def run(data_root: Path, batches: list[str]) -> int:
    if data_root.exists():
        shutil.rmtree(data_root)
    for name, step in stages(data_root, batches):
        try:
            step()
        except NotImplementedError as err:
            if not str(err).startswith("TODO(week"):
                raise
            log.warning("Stopped at %s: %s. Everything before it worked.", name, err)
            return 0
        except Exception as err:
            log.error("%s failed: %s", name, err)
            return 1
        log.info("%s: ok", name)
    log.info("The whole sample pipeline ran.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the sample pipeline as far as it is written.")
    parser.add_argument("--data-root", type=Path, default=Path("data/runs/check"))
    parser.add_argument("--batch", nargs="+", default=["2026-09", "2026-10"])
    args = parser.parse_args(argv)
    config.setup_logging()
    return run(args.data_root, args.batch)


if __name__ == "__main__":
    sys.exit(main())

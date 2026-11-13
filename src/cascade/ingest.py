"""Week 2: ingestion. Land raw trip and weather files in bronze, exactly once.

YOU WRITE: every function that raises NotImplementedError("TODO(week02) ...").
PROVIDED:  the helpers at the top, including two worked examples:
           download() (retries) and save_manifest() (an atomic write).

Run it from the repo root:

    uv run python -m cascade.ingest --batch 2026-09                 # download from the web
    uv run python -m cascade.ingest --batch 2026-09 --source data/sample   # copy the frozen sample

Bronze rules:
  * Files are stored exactly as received and never modified after landing.
  * data_root/bronze/_manifest.json records, for every file: where it came
    from, its SHA-256 hash, its row count, its size, and when it was loaded.
  * Ingestion is idempotent: running the same batch again does nothing if the
    file is already there with the hash the manifest expects. No duplicates,
    no re-download.
  * A file that is in bronze but no longer matches its hash means someone
    changed bronze. We stop with an error instead of guessing.
"""

import argparse
import calendar
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from cascade import config

log = logging.getLogger("cascade.ingest")

# Retry settings for downloads. Network calls fail now and then; a short wait
# and another try fixes most of those failures.
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = 2.0  # waits 2s, then 4s between attempts
TIMEOUT_SECONDS = 120

# At least this share of a trips file's rows must start in the batch's month.
MIN_SHARE_IN_MONTH = 0.9


class IngestError(Exception):
    """Something went wrong that the person running the pipeline must fix."""


# =============================================================================
# Small helpers (provided)
# =============================================================================


def sha256_of(path: Path) -> str:
    """Return the SHA-256 hash of a file, reading it in chunks to save memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    """Current time in UTC as an ISO 8601 string, e.g. 2026-11-12T19:04:11Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def manifest_path(data_root: Path) -> Path:
    return Path(data_root) / "bronze" / config.MANIFEST_NAME


def load_manifest(data_root: Path) -> dict:
    """Read the manifest, or return an empty one if nothing has been loaded yet.

    Shape: {batch_id: {dataset: entry}}, for example
        {"2026-09": {"trips": {...}, "weather": {...}}}
    """
    path = manifest_path(data_root)
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def save_manifest(data_root: Path, manifest: dict) -> None:
    """WORKED EXAMPLE. Write the manifest atomically: write a temp file, then rename it.

    A crash halfway through can never leave a half-written manifest behind.
    """
    path = manifest_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def count_csv_rows(path: Path) -> int:
    """Count data rows in a CSV (header excluded).

    We let DuckDB parse the file so quoted fields containing commas or line
    breaks are counted correctly. all_varchar=true skips type guessing; we only
    want a count here.
    """
    query = "SELECT count(*) FROM read_csv(?, header = true, all_varchar = true)"
    return duckdb.execute(query, [str(path)]).fetchone()[0]


def count_weather_rows(path: Path) -> int:
    """Number of hourly readings in an Open-Meteo JSON response."""
    with open(path) as f:
        return len(json.load(f)["hourly"]["time"])


# =============================================================================
# Downloading with retries (provided, WORKED EXAMPLE)
# =============================================================================


def download(url: str, dest: Path) -> None:
    """Download `url` to the file `dest`, retrying temporary failures.

    * 4xx errors (except 429 Too Many Requests) are our fault, e.g. a month
      that does not exist yet. Retrying will not help, so fail right away.
    * 5xx errors, timeouts, and dropped connections are usually temporary,
      so we wait and try again, doubling the wait each time.
    """
    request = urllib.request.Request(url, headers={"User-Agent": "cascade-mobility-pipeline"})
    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            log.info("Downloading %s (attempt %d/%d)", url, attempt, MAX_ATTEMPTS)
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                with open(dest, "wb") as out:
                    shutil.copyfileobj(response, out)
            return
        except urllib.error.HTTPError as err:
            if 400 <= err.code < 500 and err.code != 429:
                raise IngestError(
                    f"{url} returned HTTP {err.code}. Check the batch id; "
                    "the source may not have published that month."
                ) from err
            last_error = err
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            last_error = err

        if attempt < MAX_ATTEMPTS:
            wait = BACKOFF_SECONDS * 2 ** (attempt - 1)
            log.warning("Download failed (%s); retrying in %.0fs", last_error, wait)
            time.sleep(wait)

    raise IngestError(f"Could not download {url} after {MAX_ATTEMPTS} attempts: {last_error}")


# =============================================================================
# Fetchers: each one writes the raw file for one dataset to `dest`
# and returns a description of where it came from (the manifest's "source").
# =============================================================================


def fetch_trips_from_web(batch_id: str, dest: Path, work_dir: Path) -> str:
    """Download the Divvy zip for the month and extract its CSV to `dest`.

    * The URL is config.DIVVY_URL; use download() to fetch the zip into work_dir.
    * Copy the CSV's bytes out unchanged: bronze keeps the original data.
    * Ignore "__MACOSX/..." entries; only real .csv files count.
    * Real case: Divvy's 2026-01 zip contains "202501-divvy-tripdata.csv", but
      the trips inside are from January 2026. If the zip holds exactly ONE CSV
      with an unexpected name, accept it (log a warning) and return
      f"{url}#{that_name}" so the manifest records the original name.
      Otherwise return the URL. More than one CSV, or none: raise IngestError.
    """
    raise NotImplementedError("TODO(week02): fetch_trips_from_web")


def check_trips_month(path: Path, batch_id: str) -> None:
    """Make sure a trips file really belongs to its batch, whatever it is called.

    Raise IngestError, with a message containing "start in <batch_id>", if fewer
    than MIN_SHARE_IN_MONTH of the trips start in that month. (A few late-night
    trips from the month before are normal.) Let DuckDB read the CSV.
    """
    raise NotImplementedError("TODO(week02): check_trips_month")


def fetch_weather_from_web(batch_id: str, dest: Path, work_dir: Path) -> str:
    """Download one month of hourly Chicago weather from Open-Meteo. Return the URL.

    * Ask config.WEATHER_URL for config.WEATHER_HOURLY_VARIABLES, from the 1st
      to the last day of the month, at config.CHICAGO_LATITUDE/LONGITUDE, in
      config.TIMEZONE (so weather hours line up with Divvy's local times).
    * Before returning, check the response really has "hourly" data;
      raise IngestError if not.
    """
    raise NotImplementedError("TODO(week02): fetch_weather_from_web")


def fetch_from_folder(source_dir: Path, batch_id: str, filename: str, dest: Path) -> str:
    """Copy <source_dir>/<batch_id>/<filename> to `dest` (e.g. from data/sample).

    Raise IngestError with "not found" in the message if the file is missing.
    Return the source path as a string.
    """
    raise NotImplementedError("TODO(week02): fetch_from_folder")


# =============================================================================
# The load-once logic
# =============================================================================


def ingest_dataset(batch_id: str, data_root: Path, dataset: str, source: str, force: bool = False) -> dict:
    """Land one dataset ("trips" or "weather") for one batch in bronze. Return its manifest entry.

    Target file: <data_root>/bronze/<batch_id>/<filename>, where filename is
    config.trips_filename(batch_id) or config.WEATHER_FILENAME.

    1. Already loaded (in the manifest AND the file exists) and not `force`?
         - same SHA-256 as the manifest: log "already loaded ... skipping" and
           return the existing entry. Do not rewrite the file or the manifest.
         - different hash: raise IngestError("... does not match the hash ...").
           Bronze is never modified; a person must investigate.
    2. Otherwise fetch into a temporary file in the target folder, check it
       (check_trips_month for trips), count its rows, then rename it into
       place. A failed run must leave no partial file and no manifest entry.
    3. Record the entry and save the manifest. An entry has exactly these keys:
         file      "<batch_id>/<filename>"
         source    what the fetcher returned
         sha256    sha256_of(target)
         rows      count_csv_rows() or count_weather_rows()
         bytes     the file size
         loaded_at utc_now()

    `source` is "web", or a folder laid out like bronze (e.g. "data/sample").
    """
    raise NotImplementedError("TODO(week02): ingest_dataset")


def ingest_batch(batch_id: str, data_root: Path = config.DEFAULT_DATA_ROOT, source: str = "web",
                 force: bool = False) -> dict:
    """Stage entry point: land trips and weather for one batch.

    Check the batch id first with config.parse_batch_id (it raises ValueError).
    Return {"trips": <entry>, "weather": <entry>}.
    """
    raise NotImplementedError("TODO(week02): ingest_batch")


# =============================================================================
# Command line
# =============================================================================


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Land Divvy trips and Open-Meteo weather in bronze.")
    parser.add_argument("--batch", nargs="+", required=True, help="one or more batch ids, e.g. 2026-09")
    parser.add_argument("--data-root", type=Path, default=config.DEFAULT_DATA_ROOT)
    parser.add_argument("--source", default="web", help='"web" (default) or a folder such as data/sample')
    parser.add_argument("--force", action="store_true", help="reload even if already in bronze")
    args = parser.parse_args(argv)

    config.setup_logging()
    try:
        for batch_id in args.batch:
            ingest_batch(batch_id, args.data_root, args.source, args.force)
    except (IngestError, ValueError) as err:
        log.error("%s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

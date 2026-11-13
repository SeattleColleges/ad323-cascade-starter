"""Shared settings: where data lives and where it comes from.

Every pipeline stage takes a `batch_id` like "2026-09" and a `data_root`
folder. Inside `data_root` the lake layers are plain subfolders:

    data_root/bronze/   raw files exactly as received, plus _manifest.json
    data_root/silver/   cleaned Parquet, partitioned by year_month
    data_root/gold/     modeled tables (later weeks)

Keeping these names in one place means the same code runs on the full data
(data_root = "data") and in tests (data_root = a temporary folder).
"""

import re
from pathlib import Path

# ----- Folders ---------------------------------------------------------------

DEFAULT_DATA_ROOT = Path("data")

# The repository folder (two levels above this file: src/cascade/config.py).
REPO_ROOT = Path(__file__).resolve().parents[2]

# The frozen sample committed to the repo. It uses the same layout as bronze:
#   data/sample/<batch_id>/<file>
SAMPLE_DIR = Path("data/sample")

MANIFEST_NAME = "_manifest.json"

# ----- Sources ---------------------------------------------------------------

# Divvy publishes one zip per month, e.g. 202609-divvy-tripdata.zip,
# containing one CSV with the same base name.
DIVVY_URL = "https://divvy-tripdata.s3.amazonaws.com/{yyyymm}-divvy-tripdata.zip"

# Open-Meteo historical ("archive") API. Free, no key. Licensed CC BY 4.0:
# credit "Weather data by Open-Meteo.com".
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
CHICAGO_LATITUDE = 41.88
CHICAGO_LONGITUDE = -87.63
WEATHER_HOURLY_VARIABLES = ["temperature_2m", "precipitation", "wind_speed_10m", "weather_code"]
# Divvy timestamps are local Chicago time, so we ask for weather in the same
# time zone. That makes trips and weather join on the hour directly.
TIMEZONE = "America/Chicago"

# ----- File names ------------------------------------------------------------

WEATHER_FILENAME = "weather_chicago_hourly.json"

_BATCH_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parse_batch_id(batch_id: str) -> tuple[int, int]:
    """Check that a batch id looks like "YYYY-MM" and return (year, month)."""
    match = _BATCH_PATTERN.match(batch_id)
    if match is None:
        raise ValueError(f"Batch id must look like 'YYYY-MM' (e.g. 2026-09), got {batch_id!r}")
    return int(match.group(1)), int(match.group(2))


def trips_filename(batch_id: str) -> str:
    """Name of the trips CSV for a batch, e.g. '2026-09' -> '202609-divvy-tripdata.csv'."""
    year, month = parse_batch_id(batch_id)
    return f"{year}{month:02d}-divvy-tripdata.csv"


def setup_logging() -> None:
    """Send log messages from every cascade.* module to the terminal.

    We attach our own handler to the "cascade" logger instead of calling
    logging.basicConfig(), because tools we run in-process (Prefect, in
    Week 6) change the root logger's settings, which would hide our messages.
    """
    import logging

    logger = logging.getLogger("cascade")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def load_env_file(path: Path | None = None) -> dict[str, str]:
    """Read KEY=VALUE lines from the project's .env file (Week 9).

    Secrets live in .env, which is gitignored; .env.example (committed) lists
    the names with no values. Blank lines and # comments are skipped. Returns a
    dict and does NOT change os.environ, so a real environment variable always
    wins over the file.
    """
    path = path or REPO_ROOT / ".env"
    values = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values

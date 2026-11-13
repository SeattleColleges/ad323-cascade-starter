"""Week 7 (provided): download a full year of cleaned trips for the Spark lab.

    uv run python spark/get_full_year.py                      # into data/runs/full-year/silver

The lab needs about 6.3 million trips (2025-11 through 2026-10), already
cleaned to silver Parquet. They come as one release file from the course's
checkpoints repo, so Week 7 doesn't depend on your own Week 3 code:

    https://github.com/SeattleColleges/ad323-cascade-checkpoints/releases/download/week07-full-year-silver/silver-full-year.zip

About 276 MB to download, about 270 MB on disk. The file is checked
against its SHA-256 before anything is unpacked. Safe to rerun: if the year
is already there, nothing happens.
"""

import argparse
import hashlib
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

URL = "https://github.com/SeattleColleges/ad323-cascade-checkpoints/releases/download/week07-full-year-silver/silver-full-year.zip"
SHA256 = "c0489a2a7f25f35d74f1a69aea1f7c182b240bfa2542e7cc446e9bc785b83ee5"
MONTHS = 12


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def already_there(silver: Path) -> bool:
    return len(list((silver / "trips").glob("year_month=*/*.parquet"))) == MONTHS


def download(url: str, dest: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "cascade-mobility-pipeline"})
    with urllib.request.urlopen(request, timeout=120) as response, open(dest, "wb") as out:
        total = int(response.headers.get("Content-Length", 0))
        done = 0
        while chunk := response.read(4 * 1024 * 1024):
            out.write(chunk)
            done += len(chunk)
            if total:
                print(f"\r  {done / 1e6:6.0f} / {total / 1e6:.0f} MB", end="", flush=True)
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download the full-year silver for the Week 7 lab.")
    parser.add_argument("--data-root", type=Path, default=Path("data/runs/full-year"))
    parser.add_argument("--url", default=URL, help="where to download from (default: the course release)")
    args = parser.parse_args(argv)

    silver = args.data_root / "silver"
    if already_there(silver):
        print(f"{silver} already holds all {MONTHS} months; nothing to do.")
        return 0

    args.data_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.data_root) as work:
        zip_path = Path(work) / "silver-full-year.zip"
        print(f"Downloading {args.url}")
        try:
            download(args.url, zip_path)
        except OSError as err:
            print(f"Download failed: {err}\nIf the release isn't published yet, ask your instructor.")
            return 1
        if sha256_of(zip_path) != SHA256:
            print("The download is damaged or not the course file (SHA-256 mismatch). Try again.")
            return 1
        unpacked = Path(work) / "unpacked"
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(unpacked)
        if silver.exists():
            shutil.rmtree(silver)
        shutil.move(str(unpacked / "silver"), silver)

    files = sorted((silver / "trips").glob("year_month=*/*.parquet"))
    print(f"Done: {len(files)} months of silver trips in {silver}")
    return 0 if already_there(silver) else 1


if __name__ == "__main__":
    sys.exit(main())

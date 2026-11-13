"""Week 2 tests: ingestion lands each file exactly once and records it."""

import io
import json
import urllib.error

import pytest

from cascade import config, ingest
from conftest import SAMPLE_DIR

pytestmark = pytest.mark.week02


def ingest_sample(batch_id, data_root, force=False):
    return ingest.ingest_batch(batch_id, data_root, source=str(SAMPLE_DIR), force=force)


# ----- Landing and the manifest ----------------------------------------------


def test_ingest_lands_files_and_writes_manifest(data_root):
    entries = ingest_sample("2026-09", data_root)

    trips_file = data_root / "bronze" / "2026-09" / "202609-divvy-tripdata.csv"
    weather_file = data_root / "bronze" / "2026-09" / config.WEATHER_FILENAME
    assert trips_file.exists() and weather_file.exists()

    manifest = json.loads((data_root / "bronze" / "_manifest.json").read_text())
    assert manifest["2026-09"]["trips"] == entries["trips"]
    assert entries["trips"]["rows"] == 50_000
    assert entries["weather"]["rows"] == 720  # 30 days x 24 hours
    assert entries["trips"]["sha256"] == ingest.sha256_of(trips_file)
    assert set(entries["trips"]) == {"file", "source", "sha256", "rows", "bytes", "loaded_at"}


def test_bronze_file_is_identical_to_source(data_root):
    ingest_sample("2026-10", data_root)
    landed = data_root / "bronze" / "2026-10" / "202610-divvy-tripdata.csv"
    original = SAMPLE_DIR / "2026-10" / "202610-divvy-tripdata.csv"
    assert landed.read_bytes() == original.read_bytes()


# ----- Idempotency -----------------------------------------------------------


def test_rerun_is_idempotent(data_root):
    first = ingest_sample("2026-09", data_root)
    trips_file = data_root / "bronze" / "2026-09" / "202609-divvy-tripdata.csv"
    mtime_before = trips_file.stat().st_mtime_ns
    manifest_before = (data_root / "bronze" / "_manifest.json").read_text()

    second = ingest_sample("2026-09", data_root)

    assert second == first  # same entries, including the original loaded_at
    assert trips_file.stat().st_mtime_ns == mtime_before  # file was not rewritten
    assert (data_root / "bronze" / "_manifest.json").read_text() == manifest_before


def test_two_batches_get_separate_manifest_entries(data_root):
    ingest_sample("2026-09", data_root)
    ingest_sample("2026-10", data_root)
    ingest_sample("2026-09", data_root)  # rerun must not add or duplicate anything
    manifest = ingest.load_manifest(data_root)
    assert sorted(manifest) == ["2026-09", "2026-10"]
    assert all(sorted(manifest[b]) == ["trips", "weather"] for b in manifest)


def test_no_temporary_files_left_behind(data_root):
    ingest_sample("2026-09", data_root)
    names = sorted(p.name for p in (data_root / "bronze" / "2026-09").iterdir())
    assert names == ["202609-divvy-tripdata.csv", config.WEATHER_FILENAME]


# ----- Protecting bronze ------------------------------------------------------


def test_modified_bronze_file_is_rejected(data_root):
    ingest_sample("2026-09", data_root)
    trips_file = data_root / "bronze" / "2026-09" / "202609-divvy-tripdata.csv"
    with open(trips_file, "a") as f:
        f.write('"FAKE","classic_bike",,,,,,,,,,,"member"\n')

    with pytest.raises(ingest.IngestError, match="does not match the hash"):
        ingest_sample("2026-09", data_root)


def test_force_reloads_a_modified_file(data_root):
    original = ingest_sample("2026-09", data_root)["trips"]
    trips_file = data_root / "bronze" / "2026-09" / "202609-divvy-tripdata.csv"
    trips_file.write_text("garbage")

    reloaded = ingest_sample("2026-09", data_root, force=True)["trips"]
    assert reloaded["sha256"] == original["sha256"]
    assert reloaded["rows"] == 50_000


def test_deleted_bronze_file_is_reloaded(data_root):
    ingest_sample("2026-09", data_root)
    trips_file = data_root / "bronze" / "2026-09" / "202609-divvy-tripdata.csv"
    trips_file.unlink()
    ingest_sample("2026-09", data_root)
    assert trips_file.exists()


# ----- Clear errors -----------------------------------------------------------


@pytest.mark.parametrize("bad_id", ["2026-13", "202609", "26-09", "2026-9"])
def test_bad_batch_id_is_rejected(bad_id, data_root):
    with pytest.raises(ValueError, match="YYYY-MM"):
        ingest_sample(bad_id, data_root)


def test_missing_source_file_fails_cleanly(data_root):
    with pytest.raises(ingest.IngestError, match="not found"):
        ingest_sample("2026-11", data_root)  # not in the sample
    assert ingest.load_manifest(data_root) == {}  # nothing recorded
    assert list((data_root / "bronze" / "2026-11").iterdir()) == []  # no partial file


def test_cli_returns_error_code_for_bad_batch(data_root):
    assert ingest.main(["--batch", "nope", "--data-root", str(data_root)]) == 1


# ----- Retries (network faked, no real downloads) -----------------------------


def test_download_retries_temporary_failures(tmp_path, monkeypatch):
    calls = []

    def flaky_urlopen(request, timeout):
        calls.append(request.full_url)
        if len(calls) < 3:
            raise urllib.error.URLError("connection reset")
        return io.BytesIO(b"hello")

    monkeypatch.setattr(ingest.urllib.request, "urlopen", flaky_urlopen)
    monkeypatch.setattr(ingest.time, "sleep", lambda seconds: None)  # don't actually wait

    dest = tmp_path / "out.bin"
    ingest.download("https://example.invalid/file", dest)
    assert len(calls) == 3
    assert dest.read_bytes() == b"hello"


def test_download_gives_up_after_max_attempts(tmp_path, monkeypatch):
    def always_down(request, timeout):
        raise urllib.error.HTTPError(request.full_url, 503, "Service Unavailable", None, None)

    monkeypatch.setattr(ingest.urllib.request, "urlopen", always_down)
    monkeypatch.setattr(ingest.time, "sleep", lambda seconds: None)

    with pytest.raises(ingest.IngestError, match="after 3 attempts"):
        ingest.download("https://example.invalid/file", tmp_path / "out.bin")


def test_download_does_not_retry_not_found(tmp_path, monkeypatch):
    calls = []

    def not_found(request, timeout):
        calls.append(1)
        raise urllib.error.HTTPError(request.full_url, 404, "Not Found", None, None)

    monkeypatch.setattr(ingest.urllib.request, "urlopen", not_found)
    with pytest.raises(ingest.IngestError, match="HTTP 404"):
        ingest.download("https://example.invalid/file", tmp_path / "out.bin")
    assert len(calls) == 1


# ----- Don't trust file names -----------------------------------------------------


def test_wrong_month_file_is_rejected(tmp_path, data_root):
    # September's trips placed in a folder for October.
    source = tmp_path / "source"
    (source / "2026-10").mkdir(parents=True)
    (source / "2026-10" / "202610-divvy-tripdata.csv").write_bytes(
        (SAMPLE_DIR / "2026-09" / "202609-divvy-tripdata.csv").read_bytes())
    with pytest.raises(ingest.IngestError, match="start in 2026-10"):
        ingest.ingest_dataset("2026-10", data_root, "trips", str(source))
    assert ingest.load_manifest(data_root) == {}


def test_misnamed_csv_inside_zip_is_accepted(tmp_path, data_root, monkeypatch):
    # Real case: Divvy's 2026-01 zip holds "202501-divvy-tripdata.csv" with January 2026 trips.
    import zipfile

    zip_path = tmp_path / "divvy.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(SAMPLE_DIR / "2026-09" / "202609-divvy-tripdata.csv", "202509-divvy-tripdata.csv")
        zf.writestr("__MACOSX/._202509-divvy-tripdata.csv", "junk")
    monkeypatch.setattr(ingest, "download", lambda url, dest: dest.write_bytes(zip_path.read_bytes()))

    entry = ingest.ingest_dataset("2026-09", data_root, "trips", "web")
    assert entry["rows"] == 50_000
    assert entry["source"].endswith("#202509-divvy-tripdata.csv")  # original name kept
    assert (data_root / "bronze" / "2026-09" / "202609-divvy-tripdata.csv").exists()

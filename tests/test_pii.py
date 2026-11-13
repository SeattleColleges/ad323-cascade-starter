"""Week 9 tests: synthetic riders, salted hashing, and keeping raw PII out of gold."""

import hashlib

import duckdb
import pytest

from cascade import riders, transform
from conftest import land_silver

pytestmark = pytest.mark.week09

TEST_SALT = "test-salt-not-a-secret-0123456789"


def test_riders_are_reproducible_and_fake():
    first, second = riders.generate_riders(), riders.generate_riders()
    assert first == second                                     # fixed seed
    assert len(first) == 5_000
    assert len({r["email"] for r in first}) == 5_000          # one account per email
    assert all(r["email"].split("@")[1] in riders.EMAIL_DOMAINS for r in first)  # never a real domain


@pytest.fixture(scope="module")
def pii_root(tmp_path_factory):
    """Sample September + riders, built with a known test salt."""
    transform.require_models("pii")          # dim_rider and stg_riders still TODO -> skip
    data_root = tmp_path_factory.mktemp("pii") / "data"
    land_silver("2026-09", data_root)
    riders.write_riders(data_root)
    mp = pytest.MonkeyPatch()
    mp.setenv("CASCADE_PII_SALT", TEST_SALT)
    transform.transform_batch(None, data_root)
    mp.undo()
    return data_root


def test_email_hash_is_salted_sha256(pii_root):
    raw = {r["rider_id"]: r for r in riders.generate_riders()}
    row = duckdb.sql(f"SELECT rider_id, email_hash FROM '{pii_root}/gold/dim_rider.parquet' "
                     "WHERE rider_id = 'R00001'").fetchone()
    expected = hashlib.sha256((TEST_SALT + raw["R00001"]["email"]).encode()).hexdigest()
    unsalted = hashlib.sha256(raw["R00001"]["email"].encode()).hexdigest()
    assert row[1] == expected
    assert row[1] != unsalted        # a plain hash could be reversed with a list of known emails


def test_exported_gold_contains_no_raw_pii(pii_root):
    raw = riders.generate_riders()
    exported = duckdb.sql(f"SELECT * FROM '{pii_root}/gold/dim_rider.parquet'")
    assert "email" not in exported.columns and "first_name" not in exported.columns
    text = " ".join(str(v) for row in exported.fetchall() for v in row)
    assert "@" not in text
    assert raw[0]["last_name"] not in text.split()
    # The raw table exists only in the restricted schema, never as a gold file.
    assert not list((pii_root / "gold").glob("*rider*"))[1:]   # only dim_rider.parquet
    assert not (pii_root / "gold" / "stg_riders.parquet").exists()


def test_names_and_ages_are_generalized(pii_root):
    rows = duckdb.sql(f"SELECT name_masked, age_band, home_zip3 FROM '{pii_root}/gold/dim_rider.parquet'").fetchall()
    assert all(name.count("***") == 2 for name, _, _ in rows)
    assert all(len(zip3) == 3 for _, _, zip3 in rows)


def test_missing_or_weak_salt_stops_the_build(pii_root, monkeypatch):
    monkeypatch.setattr(transform.config, "load_env_file", lambda path=None: {})   # ignore a local .env
    for bad in ["", "short", "has spaces in it, 1234567"]:
        monkeypatch.setenv("CASCADE_PII_SALT", bad)
        with pytest.raises(transform.TransformError, match="CASCADE_PII_SALT"):
            transform.build(None, pii_root)

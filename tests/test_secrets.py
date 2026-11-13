"""Week 9 test (provided): fail if a secret is committed.

This is a last line of defence that runs with the other tests (and in CI).
The first line is GitHub's push protection, which blocks a push that contains
a known secret before it ever reaches the server (see README, Week 9).

What counts as a problem:
  * any tracked file named .env or .env.<something> (except .env.example)
  * known secret formats in tracked text files: GitHub tokens, AWS access keys,
    Slack tokens, private keys, and a filled-in CASCADE_PII_SALT=...
"""

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.week09

SECRET_PATTERNS = {
    "GitHub token": re.compile(r"\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36}\b|\bgithub_pat_[A-Za-z0-9_]{50,}"),
    "AWS access key": re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),
    "Slack token": re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"),
    "Private key": re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----"),
    "Filled-in PII salt": re.compile(r"^\s*CASCADE_PII_SALT\s*=\s*['\"]?[A-Za-z0-9_-]{16,}", re.MULTILINE),
}
SKIP_SUFFIXES = {".parquet", ".zip", ".gz", ".png", ".jpg", ".duckdb", ".ipynb_checkpoints"}


def tracked_files() -> list[Path]:
    try:
        out = subprocess.run(["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    return [REPO_ROOT / line for line in out.splitlines()]


def is_env_file(path: Path) -> bool:
    return (path.name == ".env" or path.name.startswith(".env.")) and path.name != ".env.example"


def find_secrets(path: Path) -> list[str]:
    """Names of the secret patterns found in one file."""
    if path.suffix in SKIP_SUFFIXES or not path.is_file():
        return []
    text = path.read_text(errors="ignore")
    return [name for name, pattern in SECRET_PATTERNS.items() if pattern.search(text)]


def test_no_env_files_are_committed():
    committed = [p.relative_to(REPO_ROOT) for p in tracked_files() if is_env_file(p)]
    assert committed == [], f"Never commit .env files: {committed}. Run: git rm --cached <file>"


def test_no_secrets_in_tracked_files():
    findings = {str(p.relative_to(REPO_ROOT)): found for p in tracked_files() if (found := find_secrets(p))}
    assert findings == {}, f"Possible secrets committed: {findings}"


# ----- The scanner itself must catch what it claims to catch ------------------------------


@pytest.mark.parametrize("content, expected", [
    ("token = ghp_" + "a1B2c3D4" * 4 + "e5F6", "GitHub token"),
    ("aws_key = AKIA" + "ABCDEFGHIJKLMNOP", "AWS access key"),
    ("-----BEGIN RSA " + "PRIVATE KEY-----\nMIIE...", "Private key"),   # split so this file stays clean
    ("CASCADE_PII_SALT=" + "9f2c" * 8, "Filled-in PII salt"),
])
def test_scanner_catches_planted_secrets(tmp_path, content, expected):
    planted = tmp_path / "config.txt"
    planted.write_text(content)
    assert expected in find_secrets(planted)


def test_scanner_allows_the_empty_example():
    assert find_secrets(REPO_ROOT / ".env.example") == []
    assert is_env_file(Path(".env")) and is_env_file(Path(".env.local"))
    assert not is_env_file(Path(".env.example"))

"""Check that this computer is ready for the course (provided).

    uv run python -m cascade.doctor              # Week 1: everything except Docker must be OK
    uv run python -m cascade.doctor --docker     # Week 6: Docker must be ready too (labs in Weeks 7-8)

Paste the output into docs/WEEK_01.md (and again into WEEK_06.md with --docker).
FAIL means fix it before the next lab, or switch to Codespaces (see README).
WARN means it will probably work, but read the note.
"""

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path

from cascade import config

GB = 1024 ** 3
MIN_FREE_DISK_GB = 15            # Spark image 7 GB + Redpanda 0.5 GB + data + room
MIN_RAM_GB = 7.5                 # "8 GB" laptops report a little less
MIN_DOCKER_MEMORY_GB = 3.5       # the Spark container is capped at 3 GB


def run(command: list[str], timeout: int = 20) -> tuple[int, str]:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        return result.returncode, (result.stdout + result.stderr).strip()
    except (FileNotFoundError, subprocess.TimeoutExpired) as err:
        return 127, str(err)


def is_wsl() -> bool:
    try:
        return "microsoft" in Path("/proc/version").read_text().lower()
    except OSError:
        return False


def total_ram_bytes() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")     # Linux, WSL, macOS
    except (ValueError, OSError, AttributeError):
        pass
    if sys.platform == "win32":
        import ctypes

        class MemoryStatus(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong),
                        ("total", ctypes.c_ulonglong), ("avail", ctypes.c_ulonglong),
                        ("total_page", ctypes.c_ulonglong), ("avail_page", ctypes.c_ulonglong),
                        ("total_virtual", ctypes.c_ulonglong), ("avail_virtual", ctypes.c_ulonglong),
                        ("avail_extended", ctypes.c_ulonglong)]
        status = MemoryStatus(length=ctypes.sizeof(MemoryStatus))
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return status.total
    return None


def lab_images() -> list[str]:
    """The pinned images from docker/*.yml (tag@digest)."""
    images = []
    for compose in sorted((config.REPO_ROOT / "docker").glob("*.yml")):
        images += re.findall(r"^\s*image:\s*(\S+)", compose.read_text(), re.MULTILINE)
    return images


def checks(need_docker: bool):
    """Yield (status, check, detail) with status OK, WARN or FAIL."""
    yield "OK", "System", f"{platform.system()} {platform.release()} ({platform.machine()})" + \
        (", inside WSL" if is_wsl() else "")

    v = sys.version_info
    yield ("OK" if v[:2] == (3, 12) else "FAIL"), "Python", \
        f"{v.major}.{v.minor}.{v.micro}" + ("" if v[:2] == (3, 12) else ": run commands with `uv run`")

    repo = config.REPO_ROOT
    if is_wsl() and str(repo).startswith("/mnt/"):
        yield "FAIL", "Repo location", f"{repo} is on the Windows drive. Clone it inside Linux: cd ~ and clone again"
    elif sys.platform == "win32":
        yield "WARN", "Repo location", "native Windows: the course expects WSL2 (Ubuntu). Tests work; Docker labs need WSL2"
    else:
        yield "OK", "Repo location", str(repo)

    free = shutil.disk_usage(repo).free / GB
    yield ("OK" if free >= MIN_FREE_DISK_GB else "WARN"), "Free disk", \
        f"{free:.0f} GB" + ("" if free >= MIN_FREE_DISK_GB else f" (need about {MIN_FREE_DISK_GB} GB by Week 7)")

    ram = total_ram_bytes()
    if ram is None:
        yield "WARN", "Memory", "could not read the computer's memory size"
    else:
        yield ("OK" if ram / GB >= MIN_RAM_GB else "WARN"), "Memory", f"{ram / GB:.1f} GB" + \
            ("" if ram / GB >= MIN_RAM_GB else ": below 8 GB; plan to use Codespaces for Weeks 7-8")

    code, out = run(["git", "--version"])
    yield ("OK" if code == 0 else "FAIL"), "Git", out if code == 0 else "not installed"

    sample = config.REPO_ROOT / config.SAMPLE_DIR
    missing = [b for b in ["2026-09", "2026-10"] if not (sample / b / config.trips_filename(b)).exists()]
    yield ("FAIL" if missing else "OK"), "Sample data", \
        f"missing {missing}" if missing else "data/sample has both months"

    # ----- Docker (Weeks 7-8) -----
    bad = "FAIL" if need_docker else "WARN"
    code, out = run(["docker", "version", "--format", "{{.Server.Version}}"])
    if code == 127:
        yield bad, "Docker", "not installed (needed from Week 7; see README, Prepare for Week 7)"
        return
    if code != 0:
        yield bad, "Docker", "installed but not running: start Docker Desktop" + \
            (" and enable WSL integration for Ubuntu" if is_wsl() else "")
        return
    yield "OK", "Docker", f"engine {out}"

    code, out = run(["docker", "info", "--format", "{{json .MemTotal}}"])
    if code == 0 and out.isdigit():
        mem = int(out) / GB
        yield ("OK" if mem >= MIN_DOCKER_MEMORY_GB else bad), "Docker memory", f"{mem:.1f} GB" + \
            ("" if mem >= MIN_DOCKER_MEMORY_GB else
             " (Spark needs 3 GB: raise it in Docker Desktop, or memory= in .wslconfig on Windows)")

    for image in lab_images():
        code, _ = run(["docker", "image", "inspect", image], timeout=30)
        short = image.split("@")[0]
        yield ("OK" if code == 0 else bad), "Image", short + ("" if code == 0 else f": not pulled yet (docker pull {short})")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check that this computer is ready for the course.")
    parser.add_argument("--docker", action="store_true", help="Docker must be ready too (Week 6 check)")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    results = list(checks(args.docker))
    if args.json:
        print(json.dumps([{"status": s, "check": c, "detail": d} for s, c, d in results], indent=2))
    else:
        for status, check, detail in results:
            print(f"{status:4}  {check:14} {detail}")
        fails = sum(s == "FAIL" for s, _, _ in results)
        warns = sum(s == "WARN" for s, _, _ in results)
        print(f"\n{fails} FAIL, {warns} WARN." + (" Ready." if not fails else " Fix the FAIL lines, or use Codespaces."))
    return 1 if any(s == "FAIL" for s, _, _ in results) else 0


if __name__ == "__main__":
    sys.exit(main())

"""Unfinished work in the starter repo, and how the tests and CI treat it.

Every piece you still have to write is marked with TODO(weekNN):

  * Python functions raise   NotImplementedError("TODO(week03): ...")
  * SQL files start with     -- TODO(week04): ...

Delete the raise (or the marker line) once that piece is done.

While a marker is still there, the tests for that week are SKIPPED, not
failed, and CI stays green. So an unfinished Week 9 never breaks Week 3.
To prove a week is finished, run its tests with --require-implemented:
then a TODO counts as a failure.

    uv run pytest -m week03 --require-implemented
"""

import re
from pathlib import Path

MARKER = re.compile(r"TODO\((week\d\d)\)")


def unfinished(paths: list[Path]) -> list[tuple[Path, str]]:
    """The files that still contain a TODO(weekNN) marker, with that week."""
    found = []
    for path in sorted(paths):
        match = MARKER.search(Path(path).read_text(encoding="utf-8"))
        if match:
            found.append((Path(path), match.group(1)))
    return found


def raise_if_todo(text: str, what: str) -> None:
    """Stop with NotImplementedError("TODO(weekNN): ...") if `text` still has a marker."""
    match = MARKER.search(text)
    if match:
        raise NotImplementedError(f"TODO({match.group(1)}): finish {what}")


def raise_if_unfinished(paths: list[Path]) -> None:
    """Stop with NotImplementedError("TODO(weekNN): ...") if any file is unfinished."""
    found = unfinished(paths)
    if found:
        week = min(w for _, w in found)
        names = ", ".join(p.name for p, w in found if w == week)
        raise NotImplementedError(f"TODO({week}): finish {names}")

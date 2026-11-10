#!/usr/bin/env python3
# lighthouse: python@v0.8.1
"""Python selector for the test gate: run a command on the tests that belong to the commit.

Usage: `gate_python.py [--tests DIR]... -- CMD...`, for example
`gate_python.py -- uv run pytest -q`. `--tests` defaults to `tests`.

Without `LIGHTHOUSE_GATE_FILES` (a commit to the default branch) it runs `CMD` bare: the
whole domain. With it, it maps the changed paths (one per line, relative to the root) to
test files and runs `CMD <test files>`. A changed `test_*.py` under a test directory
selects itself; any other path `.../X.py`, `.../X.sh` or `.../X` selects every existing
`test_<X, - as _>.py` under the test directories. The gate tests too little rather than
too much, CI tests everything.

Exit codes: those of `CMD`, or 3 when nothing was tested — a usage error, a list that
cannot be read, or a `CMD` that cannot start — with a line
`gate-python: COULD NOT CHECK — <reason>` on stderr. List 3 in
`[test_gate] unchecked_exit_codes`; 2 cannot go there, pytest uses it.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

USAGE = "usage: gate_python.py [--tests DIR]... -- CMD..."
UNCHECKED = 3


class Unchecked(Exception):
    """Nothing was tested; the message is the reason."""


def select_tests(files: list[str], root: Path, test_dirs: list[str]) -> list[str]:
    """Test files (relative to `root`, sorted, unique) that belong to the changed `files`."""
    root = root.resolve()
    existing: list[Path] = []
    for directory in test_dirs:
        base = (root / directory).resolve()
        if base.is_dir():
            existing.extend(p for p in base.rglob("test_*.py") if p.is_file())
    selected: set[str] = set()
    for name in files:
        if not name:
            continue
        path = (root / name).resolve()
        if path.name.startswith("test_") and path.suffix == ".py":
            if path in existing:
                selected.add(path.relative_to(root).as_posix())
            continue
        stem = path.name
        if path.suffix in (".py", ".sh"):
            stem = path.stem
        wanted = f"test_{stem.replace('-', '_')}.py"
        selected.update(p.relative_to(root).as_posix() for p in existing if p.name == wanted)
    return sorted(selected)


def _run(cmd: list[str]) -> int:
    try:
        return subprocess.call(cmd)
    except OSError as exc:
        raise Unchecked(f"{cmd[0]} cannot start: {exc.strerror or exc}") from exc


def _select_and_run(argv: list[str]) -> int:
    args = argv[1:]
    if "--" not in args:
        raise Unchecked(USAGE)
    split = args.index("--")
    options, cmd = args[:split], args[split + 1 :]
    test_dirs: list[str] = []
    while options:
        if options[0] != "--tests" or len(options) < 2:
            raise Unchecked(USAGE)
        test_dirs.append(options[1])
        options = options[2:]
    if not cmd:
        raise Unchecked(USAGE)
    files_path = os.environ.get("LIGHTHOUSE_GATE_FILES")
    if not files_path:
        return _run(cmd)
    try:
        changed = Path(files_path).read_text(encoding="utf-8").split("\n")
    except OSError as exc:
        raise Unchecked(f"cannot read the changed files: {exc}") from exc
    tests = select_tests(changed, Path.cwd(), test_dirs or ["tests"])
    if not tests:
        print("gate-python: skipped — no test belongs to the commit", flush=True)
        return 0
    print(f"gate-python: {len(tests)} test files", flush=True)
    return _run([*cmd, *tests])


def main(argv: list[str]) -> int:
    try:
        return _select_and_run(argv)
    except Unchecked as exc:
        sys.stderr.write(f"gate-python: COULD NOT CHECK — {exc}\n")
        return UNCHECKED


if __name__ == "__main__":
    sys.exit(main(sys.argv))

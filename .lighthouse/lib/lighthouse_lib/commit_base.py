# lighthouse: core@v0.8.1
"""`commit-test-gate` (#80): record the HEAD of each repo root before a commit, and
find afterwards which roots moved and which files changed.

`record` runs in the pre phase of the hook, `moved` and `changed_files` in the post
phase. The CLI (payload on stdin) is what the shell hook calls:

    commit_base.py record ROOT...
    commit_base.py moved
    commit_base.py files ROOT BASE HEAD OUT

Any git or I/O failure ends with exit code 2 and the reason on stderr.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import NamedTuple

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import bash_writes

# The tree git hashes for an empty repository; the base of a first commit.
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

_GIT_DEADLINE_S = 30.0


class Moved(NamedTuple):
    root: str
    base: str
    head: str


def state_dir(session_id: str, create: bool = True) -> Path:
    """`${XDG_RUNTIME_DIR:-/tmp}/lighthouse-commit-test-gate/<session_id>/`, mode `0700`.

    With `create=False` only the path: `moved` runs after every Bash call and must not
    fail on a state dir it cannot make.
    """
    if "/" in session_id or session_id == "..":
        raise ValueError(f"invalid session_id: {session_id!r}")
    base = Path(os.environ.get("XDG_RUNTIME_DIR") or "/tmp") / "lighthouse-commit-test-gate"
    path = base / session_id
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path, 0o700)
    return path


def _marker(payload: dict, create: bool = True) -> Path:
    state = state_dir(str(payload.get("session_id", "")), create=create)
    return state / f"{bash_writes.marker_key(payload)}.json"


def record(payload: dict, roots: list[str]) -> None:
    """Write the HEAD of every root to the marker of this Bash call."""
    marker = _marker(payload)
    deadline = time.time() + _GIT_DEADLINE_S
    heads = {root: bash_writes.head_of(root, deadline=deadline) for root in roots}
    bash_writes.prune_markers(marker.parent, time.time())
    bash_writes._atomic_write_json(marker.parent, marker, {"heads": heads})


def moved(payload: dict) -> list[Moved]:
    """Read and delete the marker; the roots whose HEAD is now another commit."""
    marker = _marker(payload, create=False)
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    marker.unlink(missing_ok=True)
    deadline = time.time() + _GIT_DEADLINE_S
    result = []
    for root, before in data["heads"].items():
        now = bash_writes.head_of(root, deadline=deadline)
        if now != before:
            result.append(Moved(root, EMPTY_TREE if before == "unborn" else before, now))
    return result


def changed_files(root: str, base: str, head: str) -> list[str]:
    """Paths changed between `base` and `head`, both sides of a rename, sorted."""
    out = bash_writes.git(
        root,
        "diff",
        "--name-only",
        "--no-renames",
        "-z",
        base,
        head,
        deadline=time.time() + _GIT_DEADLINE_S,
    )
    return sorted(p for p in out.decode("utf-8", errors="surrogateescape").split("\0") if p)


def main(argv: list[str]) -> int:
    try:
        if argv[:1] == ["record"]:
            record(json.loads(sys.stdin.read()), argv[1:])
        elif argv[:1] == ["moved"]:
            for row in moved(json.loads(sys.stdin.read())):
                sys.stdout.write(f"{row.root}\t{row.base}\t{row.head}\n")
        elif argv[:1] == ["files"] and len(argv) == 5:
            files = changed_files(argv[1], argv[2], argv[3])
            Path(argv[4]).write_text("".join(f"{p}\n" for p in files), encoding="utf-8")
        else:
            sys.stderr.write("usage: commit_base.py record ROOT... | moved | files ROOT BASE HEAD OUT\n")
            return 2
    except (bash_writes.GitError, OSError, ValueError, KeyError) as exc:
        sys.stderr.write(f"commit_base: {exc}\n")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

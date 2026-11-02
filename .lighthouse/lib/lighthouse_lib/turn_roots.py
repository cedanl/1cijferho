# lighthouse: core@v0.8.1
"""In which checkouts of this project did the turn write? (#54)

The Stop hooks (`ty-gate.sh`, `go-lint-gate.sh`) check a checkout. The `cwd` of the
Stop payload is the shell's directory at that moment, not where the turn changed
files. Measured in #54 (Claude Code 2.1.310, a headless session started in the
main checkout, a Stop hook that logs its payload):

- Write into the worktree by absolute path, then `ls <worktree>`: `cwd` is the
  main checkout;
- `cd <worktree> && pwd`: `cwd` is the worktree.

The `cwd` follows the Bash tool's `cd`, and Claude Code resets it to the start
directory after a `cd` outside the project. This lib reads the turn from
`transcript_path` instead.

The turn starts at the last prompt: a `user` entry that is not `isMeta` or an
`isCompactSummary` and carries text, not a `tool_result`. From there on, plus the entries since that
moment in the subagent transcripts under `<transcript without .jsonl>/subagents/`,
it collects:

- the `file_path` or `notebook_path` of Edit, Write, MultiEdit and NotebookEdit;
- per Bash call, `bash_writes.derive_roots` with the `cwd` of that entry.

Each path becomes the root of its checkout; `bash_writes.project_filter` keeps the
checkouts of this project (the one of `CLAUDE_PROJECT_DIR`, else of the payload
`cwd`). A turn
that names none gives the root of the payload `cwd`, as before #54.

    python3 turn_roots.py [--match PATTERN]... < payload

With `--match` (#80) each root gets a verdict: did the turn touch a path in
that root that matches one of the patterns (`fnmatch.fnmatchcase` on the path
relative to the root; `*` also matches `/`)? The turn starts at the `timestamp`
of the prompt, in whole epoch seconds rounded down (`since`). The paths of the
turn in a root come from three sources:

- the Edit, Write, MultiEdit and NotebookEdit paths above that lie in that
  checkout (not in a worktree under it);
- `git status --porcelain -z -uall`: a changed or new path with
  `int(st_mtime) >= since`, and every deleted path;
- the reflog of `HEAD`: `git diff --name-only <base> HEAD`, with as base the
  newest reflog entry from before `since`, else the oldest entry. A commit,
  amend, rebase, pull or checkout in the turn counts, also when nothing is
  dirty afterwards. An unborn `HEAD` skips this source.

In doubt, a root is `touched`, as before #80: a prompt without a readable
`timestamp`, or a git call that fails for that root. Too little is acceptable:
a dirty path from an earlier turn, a Bash write git does not see (an ignored
file), and the first commit of a repo that came into being in the turn do not
count. So does an interrupted turn: after an Esc the Stop hook does not run, so
its edits are never checked when the next turn touches only other paths.

Exit 0: without `--match` the roots, sorted, one per line; with it one line
`touched\t<root>` or `untouched\t<root>` per root. Exit 1: the checkout cannot
be determined (no or unreadable transcript, no prompt in it, a root git cannot
read), the reason on stdout. Only stdlib: the hooks run under the host `python3`.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import sys
import time
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import bash_writes, git_invocations

# The tools that write a file, and the input key that names it.
PATH_KEYS = {
    "Edit": "file_path",
    "Write": "file_path",
    "MultiEdit": "file_path",
    "NotebookEdit": "notebook_path",
}

DEADLINE_S = 20.0


class Undetermined(Exception):
    """The checkout of the turn cannot be determined; the message says why."""


def _entries(path: Path) -> Iterator[dict]:
    """The JSON objects of a transcript; a line that is no object is skipped."""
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if isinstance(entry, dict):
                yield entry


def _is_prompt(entry: dict) -> bool:
    if entry.get("type") != "user" or entry.get("isMeta"):
        return False
    # An auto-compaction summary is a string-content user entry without isMeta; taking
    # it for a prompt would restart the turn in the middle of it.
    if entry.get("isCompactSummary"):
        return False
    content = (entry.get("message") or {}).get("content")
    if isinstance(content, str):
        return True
    if not isinstance(content, list):
        return False
    kinds = {block.get("type") for block in content if isinstance(block, dict)}
    return "text" in kinds and "tool_result" not in kinds


def _moment(entry: dict) -> datetime | None:
    stamp = entry.get("timestamp")
    if not isinstance(stamp, str):
        return None
    try:
        # Before Python 3.11 `fromisoformat` does not read a trailing `Z`.
        return datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return None


def _tool_uses(entries: list[dict]) -> Iterator[tuple[str, dict, str | None]]:
    """`(name, input, cwd)` of every tool call in `entries`."""
    for entry in entries:
        if entry.get("type") != "assistant":
            continue
        content = (entry.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                tool_input = block.get("input")
                if isinstance(tool_input, dict):
                    yield str(block.get("name")), tool_input, entry.get("cwd")


def turn_entries(transcript: Path) -> tuple[list[dict], datetime | None]:
    """The entries of the current turn and the moment of its prompt (`None` without
    a readable `timestamp`): the main transcript from the last prompt, plus the
    subagent entries from that moment on."""
    try:
        turn: list[dict] | None = None
        prompt: dict | None = None
        for entry in _entries(transcript):
            if _is_prompt(entry):
                turn, prompt = [], entry
            elif turn is not None:
                turn.append(entry)
    except OSError as exc:
        raise Undetermined(f"the transcript {transcript} cannot be read ({exc.strerror}).") from exc
    if turn is None or prompt is None:
        raise Undetermined(f"no prompt in the transcript {transcript}.")

    start = _moment(prompt)
    subagents = transcript.with_suffix("") / "subagents"
    if start is not None and subagents.is_dir():
        for path in sorted(subagents.glob("*.jsonl")):
            try:
                for entry in _entries(path):
                    moment = _moment(entry)
                    if moment is not None and moment >= start:
                        turn.append(entry)
            except OSError as exc:
                raise Undetermined(f"the subagent transcript {path} cannot be read ({exc.strerror}).") from exc
    return turn, start


def _root_of(path: str) -> str | None:
    ancestor = git_invocations.existing_ancestor(path)
    if ancestor is None:
        return None
    root = git_invocations.repo_of(ancestor)
    return os.path.realpath(root) if root is not None else None


def touched_roots(entries: list[dict]) -> set[str]:
    """The checkout roots the tool calls in `entries` name, not yet per project."""
    roots: set[str] = set()
    for name, tool_input, cwd in _tool_uses(entries):
        key = PATH_KEYS.get(name)
        if key is not None:
            path = tool_input.get(key)
            if isinstance(path, str) and os.path.isabs(path):
                root = _root_of(path)
                if root is not None:
                    roots.add(root)
        elif name == "Bash":
            command = tool_input.get("command")
            if isinstance(command, str) and isinstance(cwd, str):
                roots |= bash_writes.derive_roots(command, cwd)
    return roots


def tool_paths(entries: list[dict]) -> set[str]:
    """The `os.path.realpath` of every absolute path an Edit, Write, MultiEdit or
    NotebookEdit call in `entries` names."""
    paths: set[str] = set()
    for name, tool_input, _cwd in _tool_uses(entries):
        key = PATH_KEYS.get(name)
        if key is not None:
            path = tool_input.get(key)
            if isinstance(path, str) and os.path.isabs(path):
                paths.add(os.path.realpath(path))
    return paths


def changed_since(root: str, since: int, *, deadline: float) -> set[str]:
    """The paths, relative to `root`, that git shows as changed since `since`
    (whole epoch seconds): dirty with a recent mtime, deleted, or in the diff
    over the reflog of the turn. Raises `bash_writes.GitError`."""
    data = bash_writes.git(root, "status", "--porcelain", "-z", "-uall", deadline=deadline)
    changed, removed = bash_writes.parse_porcelain(data)
    paths = set(removed)
    for rel in changed:
        try:
            mtime = os.lstat(os.path.join(root, rel)).st_mtime
        except OSError:
            # Changed but gone from disk: counts, like a deleted path.
            paths.add(rel)
            continue
        if int(mtime) >= since:
            paths.add(rel)

    if bash_writes.head_of(root, deadline=deadline) == "unborn":
        return paths
    reflog = bash_writes.read_reflog(root, deadline=deadline)
    if not reflog:
        return paths
    # Newest first: the first entry from before the turn, else the oldest one.
    base = next((entry.sha for entry in reflog if entry.epoch < since), reflog[-1].sha)
    diff = bash_writes.git(root, "diff", "--name-only", "--no-renames", "-z", base, "HEAD", deadline=deadline)
    paths.update(os.fsdecode(name) for name in diff.split(b"\0") if name)
    return paths


def _turn(payload: dict, *, deadline: float) -> tuple[list[str], list[dict], datetime | None]:
    """The roots to check for this Stop payload (sorted), the entries of the turn,
    and the moment of its prompt."""
    cwd = payload.get("cwd") or os.environ.get("CLAUDE_PROJECT_DIR") or ""
    transcript = payload.get("transcript_path")
    if not isinstance(transcript, str) or not transcript:
        raise Undetermined("the payload has no transcript_path.")

    cwd_root = _root_of(cwd) if cwd else None
    project_dir = os.environ.get("CLAUDE_PROJECT_DIR")
    project_root = (_root_of(project_dir) if project_dir else None) or cwd_root
    if project_root is None:
        raise Undetermined(f"not inside a git repository ({cwd}).")

    entries, start = turn_entries(Path(transcript))
    roots = touched_roots(entries)
    try:
        kept, errors = bash_writes.project_filter(roots, project_root, deadline=deadline)
    except bash_writes.GitError as exc:
        raise Undetermined(f"git cannot read {project_root} ({exc}).") from exc
    if errors:
        raise Undetermined(" ".join(errors))
    if kept:
        return sorted(kept), entries, start

    if cwd_root is None:
        raise Undetermined(f"not inside a git repository ({cwd}).")
    return [cwd_root], entries, start


def resolve(payload: dict) -> list[str]:
    """The roots to check for this Stop payload, sorted."""
    roots, _entries, _start = _turn(payload, deadline=time.time() + DEADLINE_S)
    return roots


def classify(payload: dict, patterns: list[str]) -> list[tuple[str, bool]]:
    """The roots of `resolve(payload)`, each with `True` when the turn touched a
    path in that root that matches one of `patterns`."""
    deadline = time.time() + DEADLINE_S
    roots, entries, start = _turn(payload, deadline=deadline)
    if start is None:
        return [(root, True) for root in roots]
    since = int(start.timestamp())
    # A path belongs only to its own checkout: a worktree lies under the main
    # checkout, so a prefix test on the path would give it to both.
    edited = [(path, _root_of(path)) for path in tool_paths(entries)]

    verdicts: list[tuple[str, bool]] = []
    for root in roots:
        rels = {os.path.relpath(path, root) for path, owner in edited if owner == root}
        try:
            rels |= changed_since(root, since, deadline=deadline)
        except bash_writes.GitError:
            verdicts.append((root, True))
            continue
        touched = any(fnmatch.fnmatchcase(rel, pattern) for rel in rels for pattern in patterns)
        verdicts.append((root, touched))
    return verdicts


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="The checkouts this turn wrote in.")
    parser.add_argument("--match", action="append", metavar="PATTERN", help="judge each root on this pattern")
    args = parser.parse_args(argv)
    try:
        payload = json.loads(sys.stdin.read())
    except ValueError:
        print("the payload is not valid JSON.")
        return 1
    try:
        if not isinstance(payload, dict):
            raise Undetermined("the payload is no JSON object.")
        if args.match:
            verdicts = classify(payload, args.match)
            lines = [f"{'touched' if touched else 'untouched'}\t{root}" for root, touched in verdicts]
        else:
            lines = resolve(payload)
    except Undetermined as exc:
        print(exc)
        return 1
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

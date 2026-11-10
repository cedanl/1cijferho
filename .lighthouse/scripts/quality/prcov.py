# lighthouse: python@v0.8.1
"""Assign every line under `python.source_dirs` on `HEAD` to the squash PR that
wrote it.

Feeds the nightly arm of `ratchet.py`: one `coverage.json` of the full
suite, per-PR diff coverage via `git blame` instead of a diff against one
merge-base. The calculation itself comes from `diffcov.diff_coverage` — this module
only supplies the assignment (which line belongs to which PR).
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from quality import diffcov
from quality.gitenv import suite_env

# The last `(#N)` at the end of the title — what GitHub appends on squash merge.
# An earlier ref in the middle of the title (issue move, cherry-pick) does not
# count; only $ at the end distinguishes "(#12) is at the front" (no
# match) from "... (#1009)" (match).
_PR_RE = re.compile(r"\(#(\d+)\)\s*$")

# Header line of `git blame --porcelain`: <40 hex sha> <orig-line> <final-line>
# [<number of lines in the group>]. Every source line carries such a header, even if it
# is not the first of its group — then only without the extra info lines
# after it. `final` is the line number on HEAD.
_BLAME_HEADER_RE = re.compile(r"^([0-9a-f]{40}) \d+ (\d+)")


class GitCommandError(RuntimeError):
    """A git call in this module failed. Fail closed."""


@dataclass(frozen=True)
class SquashCommit:
    sha: str
    pr: int


def pr_number(subject: str) -> int | None:
    """The PR number from the **last** `(#N)` at the end of the title."""
    match = _PR_RE.search(subject)
    return int(match.group(1)) if match else None


def _git_process(root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        env=suite_env(),
        check=False,
    )


def _run_git(root: Path, args: list[str]) -> str:
    proc = _git_process(root, args)
    if proc.returncode != 0:
        raise GitCommandError(f"`git {' '.join(args)}` failed:\n{proc.stderr}")
    return proc.stdout


def _is_ancestor(root: Path, since: str) -> bool:
    proc = _git_process(root, ["merge-base", "--is-ancestor", since, "HEAD"])
    return proc.returncode == 0


def select_commits(root: Path, since: str | None) -> list[SquashCommit]:
    """Squash commits on `HEAD` since `since`, each with a PR number.

    `since..HEAD` via `--first-parent` (the mainline, no feature commits that
    a squash merge already shakes off). If `since` is missing, or is not an
    ancestor of `HEAD` (`git merge-base --is-ancestor`) — a fresh repo,
    a force-push, a watermark that fell outside the history — the
    selection falls back to the last 24 hours instead of a range that fails
    or silently takes in the whole history. Commits without `(#N)` in the title
    (no squash merge, a loose fix) drop out.
    """
    if since is not None and _is_ancestor(root, since):
        args = ["log", "--first-parent", "--format=%H%x1f%s", f"{since}..HEAD"]
    else:
        if since is not None:
            # Not silent: a shallow clone or rewritten history would otherwise fall
            # back here every night, and then also with a wrong blame.
            print(
                f"nightly: {since} is not an ancestor of HEAD (shallow clone or "
                "rewritten history?); judging the merges of the last 24 hours",
                file=sys.stderr,
            )
        args = [
            "log",
            "--first-parent",
            "--format=%H%x1f%s",
            "--since=24.hours.ago",
            "HEAD",
        ]
    output = _run_git(root, args)
    commits: list[SquashCommit] = []
    for line in output.splitlines():
        if not line:
            continue
        sha, _, subject = line.partition("\x1f")
        pr = pr_number(subject)
        if pr is not None:
            commits.append(SquashCommit(sha=sha, pr=pr))
    return commits


def blame(root: Path, path: str) -> dict[str, list[int]]:
    """sha → line numbers on `HEAD`, from `git blame --porcelain HEAD -- path`."""
    output = _run_git(root, ["blame", "--porcelain", "HEAD", "--", path])
    result: dict[str, list[int]] = {}
    for line in output.splitlines():
        match = _BLAME_HEADER_RE.match(line)
        if match is None:
            continue
        sha, final = match.group(1), int(match.group(2))
        result.setdefault(sha, []).append(final)
    return result


def _changed_python_files(root: Path, sha: str, source_dirs: Sequence[str]) -> list[str]:
    """`.py` paths under `source_dirs` that this commit touched (created,
    changed or removed) — whether it still exists on `HEAD` is for the caller to check."""
    if not source_dirs:
        return []
    output = _run_git(root, ["show", "--name-only", "--format=", sha, "--", *source_dirs])
    return [line for line in output.splitlines() if line.strip().endswith(".py")]


def attribute(
    root: Path, commits: Sequence[SquashCommit], source_dirs: Sequence[str] = ("src",)
) -> dict[int, dict[str, list[int]]]:
    """PR → {path: lines}, via `git blame` on every path a commit touched.

    `blame` runs on `HEAD` and so only once per path, not per commit:
    the result assigns every line to whoever wrote it last, so
    a later commit that rewrites a line automatically takes it over
    from an earlier one — no separate "overwrite" case is needed. A path
    that a later commit removed again is no longer on `HEAD` and
    silently yields no entry, no exception. Every PR from `commits` is in
    the result, even without a touched path.
    """
    result: dict[int, dict[str, list[int]]] = {commit.pr: {} for commit in commits}
    blame_cache: dict[str, dict[str, list[int]]] = {}
    for commit in commits:
        for path in _changed_python_files(root, commit.sha, source_dirs):
            if not (root / path).exists():
                continue
            if path not in blame_cache:
                blame_cache[path] = blame(root, path)
            lines = blame_cache[path].get(commit.sha)
            if lines:
                result[commit.pr][path] = lines
    return result


def per_pr(
    attributed: Mapping[int, Mapping[str, Sequence[int]]],
    report: Mapping[str, Any],
    read_source: Callable[[str], str],
    exclude: Sequence[str] = (),
) -> dict[int, diffcov.DiffCoverage]:
    """Coverage per PR from the same report — no separate suite run per PR."""
    return {
        pr: diffcov.diff_coverage(paths, report, read_source, exclude)
        for pr, paths in attributed.items()
    }

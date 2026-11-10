#!/usr/bin/env python3
# lighthouse: core@v0.8.1
"""Release issues that waited on a just-closed issue, as the bot from `[ci]`.

The release lane (`.github/workflows/lighthouse-unblock-dependents.yml`) runs this
script on every `issues: closed`. For each open issue that the closed issue blocked
(`dependencies/blocking`) it counts the blockers that are still open. If none
are open any more and the issue carries the label `[labels] blocked_issue`, that label is removed and
the issue gets a comment. The closed issue gets one comment back with what was
released and what still waits.

Norms, binding for every change to this file:

- **Only `completed` releases.** An issue that was closed as `not_planned` or `duplicate`
  did not do the work; the block stays and a human decides.
- **Only what carries the label.** A dependent without the blocking label is
  not read, not edited and not mentioned; a second run after a successful
  release therefore posts nothing new. One that still waits is mentioned again.
- **Comments first, then the label.** First the reply comment on the closed issue,
  then per dependent the comment and only then the DELETE. If a comment fails, the
  label stays and a restart catches it; a 404 on the DELETE means "already gone".
- **Not read is not zero.** If reading the blockers of one issue fails, that label
  stays and the run is red; "could not read" never counts as released.
- **Stdlib-only**, plus `lighthouse_lib` from `.lighthouse/lib`: the script runs with
  `uv run --no-project`, without `uv sync`.

Why a workflow and not a step in `lighthouse-close-issue`: a session may not write as
the bot (the token exists only in Actions), and an issue also closes
via a PR merge or by hand. The skill checks afterwards.

Output and comments are English; the reply on the closed issue starts with the fixed
line `RELEASE_LINE`, which `close-issue` reads.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

# Run as a file (`python3 .lighthouse/scripts/unblock_dependents.py`): put
# `.lighthouse/lib` first, so `lighthouse_lib` imports from this installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib import config  # noqa: E402
from lighthouse_lib.config import ConfigError  # noqa: E402

Gh = Callable[[Sequence[str]], str]

REPO_PATH = "repos/{owner}/{repo}"
RELEASE_LINE = "Released lane after this close:"


def blocked_label() -> str:
    """The blocking label from `[labels] blocked_issue`, with the config reader's default."""
    return str(config.lookup(config.load(config.config_path()), "labels.blocked_issue"))


class _GhError(subprocess.CalledProcessError):
    """A `gh` error with the stderr in `str()`."""

    def __str__(self) -> str:
        base = super().__str__()
        stderr = (self.stderr or "").strip()
        return f"{base}\n{stderr}" if stderr else base


def run_gh(args: Sequence[str]) -> str:
    """`gh` with a timeout; standalone."""
    try:
        completed = subprocess.run(
            ["gh", *args],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        raise _GhError(error.returncode, error.cmd, error.output, error.stderr) from error
    except subprocess.TimeoutExpired as error:
        message = f"gh {args[0]}: timed out after {error.timeout:.0f} s"
        raise _GhError(-1, error.cmd, None, message) from error
    return completed.stdout


def _error_line(error: Exception) -> str:
    """The cause on one line: for a `gh` error the stderr, not the `Command [...]` header."""
    if isinstance(error, subprocess.CalledProcessError):
        for line in (error.stderr or "").splitlines():
            if line.strip():
                return line.strip()
    if isinstance(error, KeyError):
        return f"missing field {error}"
    return str(error)


def _api(gh: Gh, path: str) -> Any:
    return json.loads(gh(["api", f"{REPO_PATH}/{path}"]))


def _get_list(gh: Gh, path: str) -> list[dict[str, Any]]:
    """A dependency list, or `TypeError` if `gh` gives anything other than a list."""
    items = _api(gh, f"{path}?per_page=100")
    if not isinstance(items, list):
        raise TypeError(f"{path} returned no JSON list ({type(items).__name__})")
    return items


def _comment(gh: Gh, nr: int, text: str) -> None:
    gh(["api", "-X", "POST", f"{REPO_PATH}/issues/{nr}/comments", "-f", f"body={text}"])


def _labels(issue: dict[str, Any]) -> set[str]:
    return {label["name"] for label in issue["labels"]}


def _remove_label(gh: Gh, nr: int, label: str) -> None:
    """DELETE the label; a 404 (removed by hand in the meantime) is not an error."""
    try:
        gh(["api", "-X", "DELETE", f"{REPO_PATH}/issues/{nr}/labels/{quote(label, safe='')}"])
    except subprocess.CalledProcessError as error:
        if "HTTP 404" not in (error.stderr or ""):
            raise


_READ_ERRORS = (subprocess.CalledProcessError, ValueError, KeyError, TypeError)


def _read_dependents(gh: Gh, nr: int) -> tuple[str | None, list[dict[str, Any]]]:
    """(message, open dependents); a message means: nothing to do, and why."""
    issue = _api(gh, f"issues/{nr}")
    if issue["state"] != "closed":
        return f"#{nr} is open: nothing to release.", []
    if issue["state_reason"] != "completed":
        reason = issue["state_reason"]
        return f"#{nr} closed as `{reason}`: blocks stay.", []
    blocking = _get_list(gh, f"issues/{nr}/dependencies/blocking")
    dependents = [i for i in blocking if i["state"] == "open"]
    if not dependents:
        return f"#{nr} blocked no open issues.", []
    return None, dependents


def _judge(gh: Gh, dep: dict[str, Any], label: str) -> tuple[str, str]:
    """(kind, text) for one dependent, without writing: `vrij`, `wacht` or `al`."""
    if label not in _labels(dep):
        return "al", f"does not carry `{label}`, nothing to do"
    blockers = _get_list(gh, f"issues/{dep['number']}/dependencies/blocked_by")
    open_blockers = [f"#{b['number']}" for b in blockers if b["state"] == "open"]
    if open_blockers:
        return "wacht", f"still waits for {', '.join(open_blockers)}"
    return "vrij", f"released, `{label}` comes off"


def _judge_all(
    gh: Gh, dependents: list[dict[str, Any]], lines: list[str], label: str
) -> tuple[int, list[int], list[str]]:
    """Phase 1: (exit code, numbers to release, lines for the reply comment).

    Appends one summary line per dependent to `lines`.
    """
    code = 0
    released: list[int] = []
    reply_lines: list[str] = []
    for dep in dependents:
        m = dep["number"]
        try:
            kind, text = _judge(gh, dep, label)
        except _READ_ERRORS as error:
            code = 1
            lines.append(f"- #{m}: **Error:** {_error_line(error)}")
            continue
        lines.append(f"- #{m}: {text}")
        if kind == "vrij":
            released.append(m)
            reply_lines.append(f"- #{m} is released: `{label}` comes off")
        elif kind == "wacht":
            reply_lines.append(f"- #{m} {text}")
    return code, released, reply_lines


def _release(gh: Gh, nr: int, m: int, label: str) -> None:
    _comment(
        gh,
        m,
        f"Released: the last open blocker #{nr} is closed; "
        f"the release lane removes `{label}`.",
    )
    _remove_label(gh, m, label)


def run(gh: Gh, nr: int, *, label: str, dry_run: bool = False) -> tuple[int, list[str]]:
    """Release the dependents of `nr`; (exit code, summary lines).

    Three phases: read, the reply comment on `nr`, only then the dependents. If the
    reply comment fails, nothing has been touched yet and a restart redoes everything.
    """
    lines = [f"## Release lane for #{nr}", ""]
    try:
        message, dependents = _read_dependents(gh, nr)
    except _READ_ERRORS as error:
        return 1, [*lines, f"**Error:** #{nr} not read: {_error_line(error)}"]
    if message is not None:
        return 0, [*lines, message]

    code, released, reply_lines = _judge_all(gh, dependents, lines, label)
    if dry_run:
        return code, [*lines, "", "_dry-run: nothing written._"]
    if not reply_lines:
        return code, lines
    try:
        _comment(gh, nr, "\n".join([RELEASE_LINE, "", *reply_lines]))
    except _READ_ERRORS as error:
        lines.append(
            f"**Error:** comment on #{nr}: {_error_line(error)}; nothing touched"
        )
        return 1, lines
    for m in released:
        try:
            _release(gh, nr, m, label)
        except _READ_ERRORS as error:
            code = 1
            lines.append(f"- #{m}: **Error:** {_error_line(error)}")
    return code, lines


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Release the issues that waited on a just-closed issue, when it was their last open blocker."
    )
    parser.add_argument("--issue", type=int, required=True, help="the closed issue")
    parser.add_argument(
        "--dry-run", action="store_true", help="read only, write nothing"
    )
    args = parser.parse_args(argv)
    try:
        label = blocked_label()
    except ConfigError as exc:
        print(f"unblock-dependents: {exc}", file=sys.stderr)
        return 2
    code, lines = run(run_gh, args.issue, label=label, dry_run=args.dry_run)
    text = "\n".join(lines) + "\n"
    sys.stdout.write(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as file:
            file.write(text)
    return code


if __name__ == "__main__":
    sys.exit(main())

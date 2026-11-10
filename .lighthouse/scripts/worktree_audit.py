#!/usr/bin/env python3
# lighthouse: core@v0.8.1
"""worktree-audit: one join over worktrees, PR state, branches and sessions.

WHY THIS EXISTS
---------------
Cleaning up and finding things back are two questions with the same join underneath.
Measured on 2026-08-24: 12 worktrees, 94 remote branches of which 83 with a merged PR,
and 7 locked worktrees. It was done by hand twice in two weeks and goes wrong every
time.

Four things that `git worktree prune` does not catch:

  1. Squash-merge makes `git branch --merged` a lie — the original commits are not
     in main's history. A PR lookup on the head branch is the only reliable test.
  2. Unpushed commits: work that exists nowhere else. `--not --remotes`, not a
     comparison with main. Note: with a squash-merge that flag counts the WHOLE
     history by construction, because the original commits never reach a remote.
     Rank 4 corrects that with the merge moment (`_squash_artefact`).
  3. Fixture branches from a failed test run: one commit that is its own root,
     local, without a PR. See `_is_test_leakage`.
  4. Locked worktrees: report, do not force.

THE ROW IS A BRANCH, NOT A WORKTREE
-----------------------------------
83 of the 94 cleanable things on origin have no worktree; a worktree-centric model
cannot carry those without a second table. Conversely, a worktree always hangs on a
branch. So `worktree` and `port_block` are nullable.

THIS SCRIPT MUTATES NOTHING
---------------------------
No delete, no unlock, no kill, no writing to `.port-registry`. It delivers commands;
the human pastes. `run_git` refuses a non-reading git verb, so that norm sits in the
execution path and not only in the documentation. `--only` mutates nothing either:
lighthouse-close-issue runs the delivered lines, without asking — not this script.

Usage:
    python3 .lighthouse/scripts/worktree_audit.py            # --cleanup (default)
    python3 .lighthouse/scripts/worktree_audit.py --sessions
    python3 .lighthouse/scripts/worktree_audit.py --origin
    python3 .lighthouse/scripts/worktree_audit.py --kpi
    python3 .lighthouse/scripts/worktree_audit.py --only <branch>   # exit 0 = cleanable
    python3 .lighthouse/scripts/worktree_audit.py --issue <nr>   # exit 2 = PR lookup failed

The default branch comes from `[forge] default_branch`, the extra protected refs and paths
from `[worktree_audit]`, the repo for the PR lookup from `[forge] repo`. Output is English;
`--only` ends on the fixed token `cleanup: yes` or `cleanup: no — <reason>`; `--issue`
prints one such block per branch of the issue, each ending on that token.
"""

from __future__ import annotations

import argparse
import datetime
import fnmatch
import json
import re
import subprocess
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

# Run as a file (`python3 .lighthouse/scripts/worktree_audit.py`): put `.lighthouse/lib`
# first, so `lighthouse_lib` imports from this installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib import config  # noqa: E402
from lighthouse_lib.config import ConfigError  # noqa: E402
from lighthouse_lib.pr_body_gate import refs_targets  # noqa: E402

# --- protect list ----------------------------------------------------------
# The default branch is always protected; a setting that you can forget to fill
# in is no protection. On top of that come the refs and paths that only this
# project knows, from `[worktree_audit] protected_refs` and `protected_paths`
# (for example checkpoint refs such as `entire/*` or a port registration such as
# `.port-registry`): such things have no PR, are not merged, and are not missed
# until something breaks. `apply_config` fills them; until then the defaults
# apply.
DEFAULT_BRANCH = "main"
PROTECTED_REFS: tuple[str, ...] = (DEFAULT_BRANCH,)
PROTECTED_PATHS: tuple[str, ...] = ()


def apply_config() -> str | None:
    """Read the protected refs and paths and the default branch from `lighthouse.toml`;
    return the repo (`forge.repo`), or None if it is not set.

    The origin remote is no source for the repo."""
    global DEFAULT_BRANCH, PROTECTED_REFS, PROTECTED_PATHS
    cfg = config.load(config.config_path())
    DEFAULT_BRANCH = str(config.lookup(cfg, "forge.default_branch"))
    PROTECTED_REFS = (
        DEFAULT_BRANCH,
        *config.lookup(cfg, "worktree_audit.protected_refs"),
    )
    PROTECTED_PATHS = tuple(config.lookup(cfg, "worktree_audit.protected_paths"))
    try:
        return str(config.lookup(cfg, "forge.repo"))
    except KeyError:
        return None


@dataclass(frozen=True)
class Row:
    """One line of the audit. The key is a branch name, or — for an orphan
    dir — the directory name under `.claude/worktrees/`."""

    name: str
    local: bool
    remote: bool
    worktree: str | None = None
    locked: bool = False
    lock_pid: int | None = None
    # The text after `locked `, or None for a bare `locked` (#963). The
    # Remote Control session server sets a reason that starts with
    # "claude agent bridge-"; `_lock_state` tests on that.
    lock_reason: str | None = None
    dirty: bool | None = None
    unpushed: int | None = None
    pr_number: int | None = None
    pr_state: str | None = None  # "open" | "merged" | "closed" | None
    # Epoch seconds. Deliberately epoch and not ISO: `merged_at` comes from GitHub in
    # UTC ("Z") and the commit date from git with a local offset. Comparing those two
    # as strings reads 08:24+02:00 as later than 08:08Z, and that is exactly the
    # other way round. Epoch has no offset.
    pr_merged_at: float | None = None
    tip_committed_at: float | None = None
    # None = not measured. True = the tip commit has no parent.
    root_commit: bool | None = None
    # False = the PR lookup itself failed, so pr_state carries no information.
    pr_lookup_ok: bool = True
    # None = the content check did not succeed. Not the same as (): that is
    # "measured, nothing unique". Unknown must never read as clean.
    unique_files: tuple[str, ...] | None = None
    # Commits that are not in main (`main..<ref>`). 0 = the branch never got
    # further than main; then "nothing unique" is emptiness, not landed work.
    own_commits: int | None = None
    last_session: float | None = None
    session_id: str | None = None
    live: bool = False
    port_block: str | None = None
    orphan_dir: str | None = None
    dir_entries: tuple[str, ...] = ()


@dataclass(frozen=True)
class Verdict:
    rank: int
    icon: str
    text: str
    modifiers: tuple[str, ...] = ()

    @property
    def safe(self) -> bool:
        """Only rank 6 and 8 are "safe to remove". Everything above blocks."""
        return self.rank in (6, 8)

    def render(self) -> str:
        suffix = f" [{', '.join(self.modifiers)}]" if self.modifiers else ""
        return f"{self.icon} {self.text}{suffix}"


def pid_alive(pid: int) -> bool:
    return Path(f"/proc/{pid}").exists()


def is_protected(row: Row) -> bool:
    # Strip `refs/heads/` before matching: a later parser that delivers the full
    # ref (instead of `%(refname:short)`) must not silently disarm the guard.
    short_name = row.name.removeprefix("refs/heads/")
    if any(fnmatch.fnmatch(short_name, pattern) for pattern in PROTECTED_REFS):
        return True
    return any(
        fnmatch.fnmatch(entry, pattern)
        for entry in row.dir_entries
        for pattern in PROTECTED_PATHS
    )


_SERVER_LOCK_PREFIX = "claude agent bridge-"


def _lock_state(
    row: Row, alive: Callable[[int], bool]
) -> tuple[Verdict | None, tuple[str, ...]]:
    """Rank 2, plus the modifier that a stale lock — or a server lock —
    leaves behind.

    A lock with a dead pid is no blockade but a note: the row falls through to the
    next rank and carries "stale lock" along. This script never unlocks. A lock
    without a pid does block — unknown is not dead.

    A lock whose reason starts with "claude agent bridge-" was set by the
    Remote Control session server (#963), not by a session itself. A spike
    measured: that pid is
    the SERVER's and stays alive for weeks, long after the session is archived
    — so it proves nothing about whether someone is working now. Such a lock
    therefore never blocks on its own (no "stale lock" next to it, whether or not
    the pid is alive): the row falls through to the normal ladder, with "server-lock"
    as a note. Whether someone IS working now is decided by the separate `row.live`
    step in `judge`.
    """
    if not row.locked:
        return None, ()
    if row.lock_reason is not None and row.lock_reason.startswith(_SERVER_LOCK_PREFIX):
        return None, ("server-lock",)
    if row.lock_pid is None:
        return Verdict(2, "⛔", "locked, pid unknown"), ()
    if alive(row.lock_pid):
        return Verdict(2, "⛔", f"active session (pid {row.lock_pid})"), ()
    return None, ("stale lock",)


def _branch_verdict(row: Row, mods: tuple[str, ...]) -> Verdict:
    """Rank 5-8: the PR check and then the content check.

    Only for a row that has a branch. An orphan dir does not pass here —
    without that boundary it would be called "safe to remove" at rank 8.
    """
    if row.pr_state == "open":
        return Verdict(5, "⏸️", f"PR #{row.pr_number} open", mods)
    if row.pr_state == "merged":
        return Verdict(6, "✅", "safe to remove", mods)
    # Unknown is never clean. A merged PR is the stronger signal and reaches
    # rank 6 above even if the content check failed; only what would
    # otherwise fall to 7/8 is blocked here.
    if not row.pr_lookup_ok:
        return Verdict(7, "⚠️", "PR status unknown — check", mods)
    if row.unique_files is None:
        return Verdict(7, "⚠️", "content check failed — check", mods)
    if row.unique_files:
        n = len(row.unique_files)
        return Verdict(7, "⚠️", f"unique work — review ({n} files)", mods)
    # Same rank, other reason: a branch without own commits has nothing
    # unique because there is nothing. "safe to remove" would read as "the work landed
    # in main" — that is how issue-941 looked, created and never committed.
    if row.own_commits == 0:
        return Verdict(8, "✅", "empty — no own commits", mods)
    return Verdict(8, "✅", "safe to remove", mods)


def _squash_artefact(row: Row) -> bool:
    """Explains an unpushed count that only reflects the squash-merge.

    `git rev-list --count <b> --not --remotes` answers "how many commits of this
    branch are on no remote". With a squash-merge that is the WHOLE history:
    GitHub throws the separate commits away and puts one new commit on main, so
    the originals never reach a remote. On this repo that made 9 of the 14
    ⚠️ rows false, and because rank 4 is above the content check those branches
    were never measured on content.

    The replacement measure is "was there more committing after the merge". If not,
    everything landed in the squash commit. If so — or if we do not know — rank 4
    stays; that is the #544 case, and unknown is never clean here.
    """
    if row.pr_state != "merged":
        return False
    if row.pr_merged_at is None or row.tip_committed_at is None:
        return False
    return row.tip_committed_at <= row.pr_merged_at


def _is_test_leakage(row: Row) -> bool:
    """A branch with one commit that is its own root, local, without a PR.

    That is the signature of `git checkout --orphan` in the wrong repo.
    `tests/integration/test_pre_push_checkpoint_scan.sh` describes the
    mechanism: if `mktemp -d` fails (a sandboxed shell), its fixture branches
    land in the REAL repo. That is how `branch-dirty` and
    `branch-allowlisted` came about.

    The check is deliberately narrow. A pre-rewrite branch such as `v2.1-mac-thin-client`
    also has a separate history but does have parents; it must not be dragged
    along. And the verdict stays rank 4: this names the probable cause,
    it never gives permission to delete by itself.
    """
    return bool(
        row.root_commit
        and row.local
        and not row.remote
        and row.pr_state is None
        and row.pr_number is None
    )


def _unpushed_verdict(
    row: Row, mods: tuple[str, ...]
) -> tuple[Verdict | None, tuple[str, ...]]:
    """Rank 4, or `mods` extended with "squash-merge" if it is the artefact.

    Pulled out of `judge` to keep its cyclomatic complexity under the
    ruff threshold (C901) after the rank-2 step of #963; the behaviour is
    unchanged.
    """
    if row.local and row.unpushed is None:
        return Verdict(4, "⚠️", "unpushed commits unknown — check", mods), mods
    if row.unpushed:
        if _is_test_leakage(row):
            return (
                Verdict(
                    4, "⚠️", "orphan commit without parent — likely test leakage", mods
                ),
                mods,
            )
        if _squash_artefact(row):
            mods = (*mods, "squash-merge")
        else:
            return (
                Verdict(4, "⚠️", f"{row.unpushed} local commits — check", mods),
                mods,
            )
    return None, mods


def judge(row: Row, *, alive: Callable[[int], bool] = pid_alive) -> Verdict:
    """The ladder: first hit wins, safety on top.

    Rank 3 and 4 are above rank 6 — a merged branch with unpushed commits
    never gets a flat "safe to remove". That is the #544 case and the reason rank 4
    was not merged with rank 6.

    Rank 2 covers two things: a lock with a live or unknown pid, and a
    running session with the worktree as cwd (#963) — regardless of who created
    the worktree. A server lock (`claude agent bridge-…`, see `_lock_state`)
    does not block on its own; only this `row.live` step then decides.
    """
    if is_protected(row):
        return Verdict(1, "🔒", "never touch")

    lock, mods = _lock_state(row, alive)
    if lock is not None:
        return lock

    # A running session with this worktree as cwd (#963). The Remote Control
    # session server makes worktrees that never get a PR; without this step
    # a clean, empty worktree of a RUNNING session is called rank 8 "safe to remove".
    # The signal is "someone is working here now", regardless of who created the
    # worktree — and so also independent of whether `_lock_state` above let a
    # server lock through.
    if row.live:
        return Verdict(2, "⛔", "running session", mods)

    if row.worktree is not None and row.dirty is not False:
        text = "save first" if row.dirty else "tree status unreadable"
        return Verdict(3, "⛔", text, mods)

    verdict, mods = _unpushed_verdict(row, mods)
    if verdict is not None:
        return verdict

    if row.local or row.remote:
        return _branch_verdict(row, mods)

    # Minor 7: git knows a detached worktree (it is in `worktree
    # list`) but does not know an orphan dir — and the orphan dir is the one someone
    # `rm -rf`s by accident. Both render rank 9, but not the same text.
    if row.worktree is not None:
        return Verdict(9, "❓", "detached worktree — manual", mods)
    # Rank 9 is overruled by rank 1 as soon as the dir contains a `.port-registry`.
    # Without that overrule `.claude/worktrees/worktrees/` would be reported as an
    # orphan, and an orphan report is the first step towards an `rm -rf`.
    return Verdict(9, "❓", "orphan — manual", mods)


# Read-only git verbs. The refusal below is no documentation but a
# gate: this script mutates nothing, not even if a later change tries to do so
# by accident. Exactly the verbs this script issues — no "may come in handy
# later" entries: an allowlist that allows more than needed is no
# allowlist any more, and adding to a call must stay a deliberate act
# (Minor 5).
READONLY_GIT_VERBS: frozenset[str] = frozenset(
    {
        "worktree",
        "branch",
        "status",
        "rev-list",
        "diff",
        "cat-file",
        "remote",
    }
)
# `branch` only allows read-only flags. A denylist once knew
# `d/delete/m/move/c/copy` and just let `-f`, `--force` and `-u`/`--set-upstream`
# through — those move or rewrite a ref. An allowlist cannot do that
# by construction: only what is listed here gets through.
#
# `--list`, `--contains`, `--merged` and `--no-merged` are deliberately not here:
# each requires a bare positional (a pattern, a commit) and that is refused
# below anyway, so they were dead entries. `--merged` is
# also no oversight but a project rule — squash-merge makes
# `git branch --merged` a lie (see module doc); merge detection runs via
# the PR head branch, never via this flag.
_READONLY_BRANCH_FLAGS: frozenset[str] = frozenset(
    {
        "-a",
        "--all",
        "-r",
        "--remotes",
        "-v",
        "-vv",
        "--verbose",
        "--show-current",
    }
)
# `worktree` only allows `list`. A denylist with mutation subverbs cannot know a
# future git subcommand; an allowlist does not need to know anything.
_READONLY_WORKTREE_SUBVERBS: frozenset[str] = frozenset({"list"})


def _assert_readonly(args: list[str]) -> None:
    """The four allowlist checks. Separate from `run_git` so that a unit test can
    query the gate without ever touching `subprocess.run`."""
    verb = args[0] if args else ""
    if verb not in READONLY_GIT_VERBS:
        raise ValueError(f"git {verb}: read-only commands only")
    if verb == "worktree" and (
        len(args) < 2 or args[1] not in _READONLY_WORKTREE_SUBVERBS
    ):
        subverb = args[1] if len(args) > 1 else "<none>"
        raise ValueError(f"git worktree {subverb}: read-only commands only")
    if verb == "branch" and any(
        not a.startswith("-")
        or not (a in _READONLY_BRANCH_FLAGS or a.startswith(("--format=", "--sort=")))
        for a in args[1:]
    ):
        raise ValueError("git branch: read-only commands only")
    if verb == "remote" and (len(args) < 2 or args[1] not in {"get-url", "-v"}):
        raise ValueError("git remote: read-only commands only")


def run_git(args: list[str], *, cwd: str | Path | None = None) -> str | None:
    """Run a read-only git command. Returns None if it failed.

    None is a third outcome next to succeeded and empty: the caller must handle it
    conservatively (an unknown state blocks, it is never called "clean").
    """
    _assert_readonly(args)
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return proc.stdout if proc.returncode == 0 else None


@dataclass(frozen=True)
class WorktreeInfo:
    path: str
    branch: str | None
    locked: bool
    lock_pid: int | None
    lock_reason: str | None = None


_LOCK_PID = re.compile(r"\(pid (\d+)")


def parse_worktrees(porcelain: str) -> list[WorktreeInfo]:
    """Parse `git worktree list --porcelain`.

    Blocks are separated by an empty line. `locked` can occur bare or
    with a reason; the Claude lock carries the pid in that reason.
    """
    out: list[WorktreeInfo] = []
    path: str | None = None
    branch: str | None = None
    locked = False
    lock_pid: int | None = None
    lock_reason: str | None = None

    def flush() -> None:
        nonlocal path, branch, locked, lock_pid, lock_reason
        if path is not None:
            out.append(WorktreeInfo(path, branch, locked, lock_pid, lock_reason))
        path, branch, locked, lock_pid, lock_reason = None, None, False, None, None

    for line in porcelain.splitlines():
        if line.startswith("worktree "):
            flush()
            path = line[len("worktree ") :]
        elif line.startswith("branch refs/heads/"):
            branch = line[len("branch refs/heads/") :]
        elif line == "locked" or line.startswith("locked "):
            locked = True
            lock_reason = line[len("locked ") :] if line.startswith("locked ") else None
            match = _LOCK_PID.search(line)
            lock_pid = int(match.group(1)) if match else None
    flush()
    return out


def parse_branches(text: str, *, remote: bool = False) -> list[str]:
    """Parse `git branch --format=%(refname:short)`, remote-variant strip origin/."""
    names: list[str] = []
    for line in text.splitlines():
        name = line.strip()
        if not name:
            continue
        if remote:
            prefix, _, rest = name.partition("/")
            if prefix != "origin" or not rest or rest == "HEAD":
                continue
            name = rest
        names.append(name)
    return names


def _epoch(stamp: object) -> float | None:
    """GitHub ISO ("2026-08-02T08:08:31Z") to epoch seconds.

    `fromisoformat` swallows the "Z" itself since 3.11; a manual replacement to
    "+00:00" is dead weight (FURB162).

    Only epoch leaves this function. An ISO string from GitHub is in UTC and
    a commit date from git carries a local offset; comparing those two as text
    reads "08:24+02:00" as later than "08:08Z" while it is two hours
    earlier. That is no theoretical trap — it gave a full table of false
    "COMMIT AFTER MERGE" lines before this function existed.
    """
    if not isinstance(stamp, str) or not stamp:
        return None
    try:
        return datetime.datetime.fromisoformat(stamp).timestamp()
    except ValueError:
        return None


@dataclass(frozen=True)
class PullInfo:
    number: int
    state: str  # "open" | "merged" | "closed"
    # Epoch seconds, or None if the PR is not merged or GitHub gave no
    # usable `merged_at`. See Row.pr_merged_at for why epoch.
    merged_at: float | None = None


# A head branch can carry several PRs. Open weighs the heaviest (rank 5 pauses),
# then merged, then closed-without-merge; within the same state the highest
# PR number wins.
_PR_RANK = {"open": 0, "merged": 1, "closed": 2}


def parse_pulls(payload: Iterable[dict]) -> dict[str, PullInfo]:
    """The join key is the head branch.

    The REST trap: `state` only knows "open" and "closed" here. You recognise merged
    by `merged_at`, not by `state`.
    """
    best: dict[str, PullInfo] = {}
    for pull in payload:
        ref = pull.get("head", {}).get("ref")
        if not ref:
            continue
        state = "merged" if pull.get("merged_at") else str(pull.get("state", "closed"))
        info = PullInfo(int(pull["number"]), state, _epoch(pull.get("merged_at")))
        current = best.get(ref)
        if current is None or (_PR_RANK[state], -info.number) < (
            _PR_RANK[current.state],
            -current.number,
        ):
            best[ref] = info
    return best


def parse_port_registry(text: str) -> dict[str, str]:
    """`<key>=<port>` per line; the key is an issue number."""
    ports: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip():
            ports[key.strip()] = value.strip()
    return ports


@dataclass(frozen=True)
class SessionInfo:
    pid: int
    session_id: str
    cwd: str


def parse_sessions(records: Iterable[dict]) -> dict[str, SessionInfo]:
    """`~/.claude/sessions/*.json` contains only RUNNING sessions, as measured."""
    out: dict[str, SessionInfo] = {}
    for record in records:
        cwd = record.get("cwd")
        pid = record.get("pid")
        session_id = record.get("sessionId")
        if not (cwd and pid and session_id):
            continue
        out[str(cwd)] = SessionInfo(int(pid), str(session_id), str(cwd))
    return out


def encode_project_dir(path: str) -> str:
    """Path → directory name under `~/.claude/projects/`: `/`, `_` and `.` become `-`."""
    return re.sub(r"[/_.]", "-", path)


def latest_transcript(
    projects_root: Path, worktree_path: str
) -> tuple[float, str] | None:
    """(mtime, sessionId) of the newest transcript for this path, or None.

    AC 7: closed sessions come from here, not from `~/.claude/sessions/`.
    """
    directory = projects_root / encode_project_dir(worktree_path)
    newest: tuple[float, str] | None = None
    # No try around the glob: `Path.glob` swallows both a missing and an
    # unreadable directory and then just gives an empty list (measured on 3.12). An
    # except around it is dead code, and dead defence reads as coverage that is
    # not there. The stat per file CAN race — that guard stays.
    for entry in directory.glob("*.jsonl"):
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            continue
        if newest is None or mtime > newest[0]:
            newest = (mtime, entry.stem)
    return newest


@dataclass(frozen=True)
class BranchProbe:
    """Per-branch measurements. None means "not measured", never "clean"."""

    dirty: bool | None = None
    unpushed: int | None = None
    unique_files: tuple[str, ...] | None = None
    own_commits: int | None = None
    tip_committed_at: float | None = None
    root_commit: bool | None = None


@dataclass(frozen=True)
class OrphanDir:
    """A directory under `.claude/worktrees/` that is not in `git worktree list`."""

    name: str
    path: str
    dir_entries: tuple[str, ...]


_DIGITS = re.compile(r"\d+")


def _port_for(name: str, ports: dict[str, str]) -> str | None:
    """The port block is registered per issue number; match on digits in the name."""
    for chunk in _DIGITS.findall(name):
        if chunk in ports:
            return ports[chunk]
    return None


def build_rows(
    *,
    worktrees: list[WorktreeInfo],
    local_branches: list[str],
    remote_branches: list[str],
    pulls: dict[str, PullInfo],
    probes: dict[str, BranchProbe],
    sessions: dict[str, SessionInfo],
    transcripts: dict[str, tuple[float, str]],
    detached_dirty: dict[str, bool | None],
    orphans: list[OrphanDir],
    ports: dict[str, str],
    pr_lookup_ok: bool = True,
    alive: Callable[[int], bool] = pid_alive,
) -> list[Row]:
    """The join. Pure: no subprocess, no filesystem."""
    worktree_by_branch = {w.branch: w for w in worktrees if w.branch}
    local = set(local_branches)
    remote = set(remote_branches)

    rows: list[Row] = []
    for name in sorted(local | remote):
        worktree = worktree_by_branch.get(name)
        probe = probes.get(name, BranchProbe())
        pull = pulls.get(name)
        session = sessions.get(worktree.path) if worktree else None
        transcript = transcripts.get(worktree.path) if worktree else None
        rows.append(
            Row(
                name=name,
                local=name in local,
                remote=name in remote,
                worktree=worktree.path if worktree else None,
                locked=worktree.locked if worktree else False,
                lock_pid=worktree.lock_pid if worktree else None,
                lock_reason=worktree.lock_reason if worktree else None,
                dirty=probe.dirty,
                unpushed=probe.unpushed,
                pr_number=pull.number if pull else None,
                pr_state=pull.state if pull else None,
                pr_merged_at=pull.merged_at if pull else None,
                tip_committed_at=probe.tip_committed_at,
                root_commit=probe.root_commit,
                pr_lookup_ok=pr_lookup_ok,
                unique_files=probe.unique_files,
                own_commits=probe.own_commits,
                last_session=transcript[0] if transcript else None,
                session_id=(
                    session.session_id
                    if session
                    else (transcript[1] if transcript else None)
                ),
                live=session is not None and alive(session.pid),
                port_block=_port_for(name, ports),
            )
        )

    # Minor 6: a worktree without a branch line (detached HEAD) is not in
    # `local`/`remote` AND is no orphan — git just knows it via
    # `worktree list`. Without a row of its own it vanishes without a trace from the
    # table, and that is the same defect class as a hole in the protect list.
    for worktree in worktrees:
        if worktree.branch is not None:
            continue
        name = Path(worktree.path).name
        session = sessions.get(worktree.path)
        transcript = transcripts.get(worktree.path)
        rows.append(
            Row(
                name=name,
                local=False,
                remote=False,
                worktree=worktree.path,
                locked=worktree.locked,
                lock_pid=worktree.lock_pid,
                lock_reason=worktree.lock_reason,
                dirty=detached_dirty.get(worktree.path),
                last_session=transcript[0] if transcript else None,
                session_id=(
                    session.session_id
                    if session
                    else (transcript[1] if transcript else None)
                ),
                live=session is not None and alive(session.pid),
                port_block=_port_for(name, ports),
            )
        )

    for orphan in orphans:
        rows.append(
            Row(
                name=orphan.name,
                local=False,
                remote=False,
                orphan_dir=orphan.path,
                dir_entries=orphan.dir_entries,
                port_block=_port_for(orphan.name, ports),
            )
        )
    return rows


WORKTREE_DIRNAME = ".claude/worktrees"


def run_gh_pulls(slug: str | None) -> list[dict] | None:
    """`gh api` — never `gh pr list --json`: that breaks under the rtk proxy.

    None means "the lookup failed", not "this repo has no PRs". That
    difference is carried by `Row.pr_lookup_ok`; without that distinction a broken
    `gh` reads as "no branch has a PR" and everything falls to rank 7/8. Without
    `forge.repo` there is no lookup: that too is None.
    """
    if slug is None:
        return None
    try:
        proc = subprocess.run(
            [
                "gh",
                "api",
                "--paginate",
                f"repos/{slug}/pulls?state=all&per_page=100",
                "--jq",
                ".[] | {number, state, merged_at, head: {ref: .head.ref}}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    payload: list[dict] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return payload


def repo_slug() -> str | None:
    """The repo from `lighthouse.toml`; the origin remote is no source."""
    return apply_config()


ISSUE_PRS_QUERY = """query($owner:String!,$name:String!,$nr:Int!){repository(owner:$owner,name:$name){issue(number:$nr){
  closedByPullRequestsReferences(first:50,includeClosedPrs:true){nodes{headRefName merged isCrossRepository repository{nameWithOwner}}}
  timelineItems(itemTypes:[CROSS_REFERENCED_EVENT],first:100){nodes{... on CrossReferencedEvent{source{... on PullRequest{headRefName merged isCrossRepository body repository{nameWithOwner}}}}}}}}}"""


def parse_issue_pr_heads(payload: dict, slug: str, nr: int) -> list[str] | None:
    """The head branches of the merged PRs of issue `nr`, in order of arrival.

    Two sources. `closedByPullRequestsReferences` only knows PRs with a
    closing word; a PR with `Refs #<nr>` is not in it. That does make a
    `CrossReferencedEvent` on the issue with the PR as `source`. Such an event also arises
    for a PR that only mentions the issue ("see #<nr>"), and that does not belong:
    a cross-ref only counts if `refs_targets` (the reader of `pr-create-gate`:
    outside HTML comments, without a reason requirement) finds `<nr>` in the PR body. A
    closing node always counts. Both must be `merged` and come from the same repo:
    `repository` of a PR is the *base* repo, so a fork PR to `slug` passes
    on that; `isCrossRepository` excludes it (a fork branch such as `patch-1` must not
    be confused with a local branch of the same name).

    None = the lookup failed (GraphQL `errors`, or `issue: null` for a
    wrong number): that is not "no PRs".
    """
    if not isinstance(payload, dict) or payload.get("errors"):
        return None
    try:
        issue = payload["data"]["repository"]["issue"]
    except (KeyError, TypeError):
        return None
    if not isinstance(issue, dict):
        return None
    heads: list[str] = []

    def take(node: object, *, needs_refs: bool) -> None:
        if not isinstance(node, dict) or not node.get("merged"):
            return
        head = node.get("headRefName")
        repo = (node.get("repository") or {}).get("nameWithOwner")
        if not head or repo != slug or node.get("isCrossRepository"):
            return
        if needs_refs and nr not in refs_targets(node.get("body") or ""):
            return
        if head not in heads:
            heads.append(head)

    closing = (issue.get("closedByPullRequestsReferences") or {}).get("nodes") or []
    for node in closing:
        take(node, needs_refs=False)
    timeline = (issue.get("timelineItems") or {}).get("nodes") or []
    for item in timeline:
        take((item or {}).get("source"), needs_refs=True)
    return heads


def run_gh_issue_pr_heads(slug: str | None, nr: int) -> list[str] | None:
    """`gh api graphql` for `parse_issue_pr_heads`; None = lookup failed.

    Without `forge.repo` there is no lookup: that too is None. `-F nr=` and not `-f`:
    `$nr` is an `Int!`.
    """
    if slug is None:
        return None
    owner, _, name = slug.partition("/")
    try:
        proc = subprocess.run(
            [
                "gh",
                "api",
                "graphql",
                "-f",
                f"owner={owner}",
                "-f",
                f"name={name}",
                "-F",
                f"nr={nr}",
                "-f",
                f"query={ISSUE_PRS_QUERY}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    return parse_issue_pr_heads(payload, slug, nr)


def _tip_shape(target: str, repo_root: Path) -> tuple[float | None, bool | None]:
    """Commit time (epoch) and "does the tip have a parent" in one git call.

    `git rev-list -1 --timestamp --parents <ref>` delivers
    `<epoch> <sha> [<parent> ...]`. Two fields means: no parent, so the
    tip is its own root — the signature of `git checkout --orphan`.

    `--timestamp` gives epoch and thus no time zone. That is the reason there is
    no `log --format=%cI` here: it carries an offset, and on top of that `log` would have
    to stretch the read-only allowlist.

    If the call fails, everything stays None — not measured is never clean.
    """
    out = run_git(["rev-list", "-1", "--timestamp", "--parents", target], cwd=repo_root)
    if not out or not out.strip():
        return None, None
    fields = out.split("\n")[0].split()
    if len(fields) < 2 or not fields[0].isdigit():
        return None, None
    return float(fields[0]), len(fields) == 2


def probe_branch(
    name: str,
    worktree: WorktreeInfo | None,
    repo_root: Path,
    *,
    ref: str | None = None,
    skip_content: bool = False,
) -> BranchProbe:
    """Measure the three things that prune does not catch.

    `--not --remotes` is the difference between "exists nowhere else"
    (data loss) and "not merged" (normal for a squash lane).

    `ref` is the ref that git measures against. `name` is the row key, and for a
    remote-only branch that is not the same: `parse_branches(remote=True)`
    strips the `origin/` prefix, so the bare name is then no resolvable ref
    any more. Without `ref` git dies on "ambiguous argument" and that becomes via
    `run_git` a `None` — "not measured" — for 93 of the 101 remote
    branches on this repo. `target = ref or name` keeps old callers
    (local branch, `ref` not passed) working.
    """
    target = ref or name

    dirty: bool | None = None
    if worktree is not None:
        status = run_git(["status", "--porcelain"], cwd=worktree.path)
        dirty = None if status is None else bool(status.strip())

    count = run_git(
        ["rev-list", "--count", target, "--not", "--remotes"], cwd=repo_root
    )
    unpushed: int | None = None
    if count is not None and count.strip().isdigit():
        unpushed = int(count.strip())

    tip_at, root = _tip_shape(target, repo_root)

    # skip_content=True: the PR is already merged, so rank 6 wins over
    # rank 7/8 anyway and `unique_files` is never consulted — a diff plus a
    # `cat-file -e` per changed file is then pure waste (~106
    # branches × several git calls on this repo). `None` stays for
    # "not measured, and not needed"; it must never become `()` here, because that
    # would claim a measurement that never took place.
    unique: tuple[str, ...] | None = None
    own: int | None = None
    if not skip_content:
        ahead = run_git(
            ["rev-list", "--count", f"{DEFAULT_BRANCH}..{target}"], cwd=repo_root
        )
        if ahead is not None and ahead.strip().isdigit():
            own = int(ahead.strip())
        changed = run_git(
            ["diff", "--name-only", f"{DEFAULT_BRANCH}...{target}"], cwd=repo_root
        )
        if changed is not None:
            missing = []
            for path in changed.splitlines():
                path = path.strip()
                if (
                    path
                    and run_git(
                        ["cat-file", "-e", f"{DEFAULT_BRANCH}:{path}"], cwd=repo_root
                    )
                    is None
                ):
                    missing.append(path)
            unique = tuple(missing)
    return BranchProbe(
        dirty=dirty,
        unpushed=unpushed,
        unique_files=unique,
        own_commits=own,
        tip_committed_at=tip_at,
        root_commit=root,
    )


def _probe_all_branches(
    local: list[str],
    remote: list[str],
    worktree_by_branch: dict[str, WorktreeInfo],
    pulls: dict[str, PullInfo],
    root: Path,
) -> dict[str, BranchProbe]:
    """Builds the probe per branch: the right `ref` (Critical 1) and `skip_content`
    for an already merged PR (Minor 5)."""
    local_set = set(local)
    result: dict[str, BranchProbe] = {}
    for name in sorted(local_set | set(remote)):
        if any(fnmatch.fnmatch(name, p) for p in PROTECTED_REFS):
            continue
        pull = pulls.get(name)
        result[name] = probe_branch(
            name,
            worktree_by_branch.get(name),
            root,
            ref=name if name in local_set else f"origin/{name}",
            skip_content=pull is not None and pull.state == "merged",
        )
    return result


def find_orphans(repo_root: Path, worktrees: list[WorktreeInfo]) -> list[OrphanDir]:
    """Directories under `.claude/worktrees/` that git does not know.

    Orphans from the devcontainer: their `.git` points to `/workspaces/...`, so
    `git worktree prune` does not touch them and only this difference finds them.
    """
    known = {w.path for w in worktrees}
    base = repo_root / WORKTREE_DIRNAME
    orphans: list[OrphanDir] = []
    try:
        entries = sorted(base.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.is_dir() or str(entry) in known:
            continue
        try:
            names = tuple(sorted(child.name for child in entry.iterdir()))
        except OSError:
            names = ()
        orphans.append(OrphanDir(entry.name, str(entry), names))
    return orphans


def _load_sessions(sessions_dir: Path) -> dict[str, SessionInfo]:
    """Reads `~/.claude/sessions/*.json`.

    No try around the glob: `Path.glob` gives an empty list for both a
    missing and an unreadable directory (measured on 3.12, see also
    `latest_transcript`) — an except around it is dead code and dead defence
    reads as coverage that is not there. The try per file stays: `read_text`
    / `json.loads` CAN really fail.
    """
    records: list[dict] = []
    for path in sorted(sessions_dir.glob("*.json")):
        try:
            records.append(json.loads(path.read_text()))
        except (OSError, json.JSONDecodeError):
            continue
    return parse_sessions(records)


def _detached_dirty(worktrees: list[WorktreeInfo]) -> dict[str, bool | None]:
    """Tree status per detached-HEAD worktree (Minor 6).

    Such a worktree has no branch and thus no `probe_branch` result; it
    would otherwise always land on rank 3 (dirty is never `False`) instead of
    rank 3 for a really dirty tree and rank 9 ("orphan") for a clean one.
    """
    result: dict[str, bool | None] = {}
    for worktree in worktrees:
        if worktree.branch is not None:
            continue
        status = run_git(["status", "--porcelain"], cwd=worktree.path)
        result[worktree.path] = None if status is None else bool(status.strip())
    return result


def collect(repo_root: Path | None = None) -> list[Row]:
    # Minor 4: `or ""` hid a failed git call as an empty, clean
    # world — and then the rank-3 dirty-tree guard no longer fires. A
    # tool that cannot read the repo must say so, not give an empty answer
    # that looks like "nothing to do".
    porcelain = run_git(["worktree", "list", "--porcelain"])
    if porcelain is None:
        raise SystemExit(
            "git worktree list --porcelain failed — cannot read the repo."
        )
    worktrees = parse_worktrees(porcelain)
    # `git worktree list --porcelain` always puts the main worktree as the first
    # block: entry 0 is the main checkout, and that is what the orphan scan and the
    # branch listing must run against — also when this script is called from a
    # linked worktree.
    root = repo_root or (Path(worktrees[0].path) if worktrees else Path.cwd())

    local_raw = run_git(["branch", "--format=%(refname:short)"], cwd=root)
    if local_raw is None:
        raise SystemExit(
            "git branch --format=%(refname:short) failed — cannot read the repo."
        )
    local = parse_branches(local_raw)

    remote_raw = run_git(["branch", "-r", "--format=%(refname:short)"], cwd=root)
    if remote_raw is None:
        raise SystemExit(
            "git branch -r --format=%(refname:short) failed — cannot read the repo."
        )
    remote = parse_branches(remote_raw, remote=True)
    try:
        slug = repo_slug()
    except ConfigError as exc:
        # rc 2, not the rc 1 of SystemExit(<text>): under `--only` rc 1 already means
        # "not cleanable".
        print(f"worktree-audit: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    if slug is None:
        print(
            "worktree-audit: PR lookup skipped — [forge] repo is not set, so every PR state is unknown",
            file=sys.stderr,
        )
    raw_pulls = run_gh_pulls(slug)
    pr_lookup_ok = raw_pulls is not None
    pulls = parse_pulls(raw_pulls or [])

    worktree_by_branch = {w.branch: w for w in worktrees if w.branch}
    probes = _probe_all_branches(local, remote, worktree_by_branch, pulls, root)

    sessions = _load_sessions(Path.home() / ".claude" / "sessions")

    projects_root = Path.home() / ".claude" / "projects"
    transcripts: dict[str, tuple[float, str]] = {}
    for worktree in worktrees:
        found = latest_transcript(projects_root, worktree.path)
        if found is not None:
            transcripts[worktree.path] = found

    detached_dirty = _detached_dirty(worktrees)

    orphans = find_orphans(root, worktrees)
    ports: dict[str, str] = {}
    for orphan in orphans:
        if ".port-registry" in orphan.dir_entries:
            try:
                ports = parse_port_registry(
                    (Path(orphan.path) / ".port-registry").read_text()
                )
            except OSError:
                ports = {}

    return build_rows(
        worktrees=worktrees,
        local_branches=local,
        remote_branches=remote,
        pulls=pulls,
        probes=probes,
        sessions=sessions,
        transcripts=transcripts,
        detached_dirty=detached_dirty,
        orphans=orphans,
        ports=ports,
        pr_lookup_ok=pr_lookup_ok,
    )


TABLE_HEADER = (
    "| worktree | branch | PR state | unpushed commits | dirty tree "
    "| last session | live? | verdict |"
)
TABLE_RULE = "|---|---|---|---|---|---|---|---|"


def _fmt_time(stamp: float | None) -> str:
    """Local time, built tz-aware: a naive fromtimestamp triggers ruff's
    DTZ006, and DTZ is in the linted set."""
    if stamp is None:
        return "—"
    moment = datetime.datetime.fromtimestamp(stamp, tz=datetime.UTC).astimezone()
    return moment.strftime("%m-%d %H:%M")


def _row_line(row: Row, verdict: Verdict) -> str:
    location = row.worktree or row.orphan_dir
    worktree = Path(location).name if location else "—"
    pr = f"#{row.pr_number} {row.pr_state}" if row.pr_number else "—"
    unpushed = "?" if row.unpushed is None else str(row.unpushed)
    dirty = {True: "yes", False: "no", None: "?"}[row.dirty]
    return (
        f"| {worktree} | {row.name} | {pr} | {unpushed} | {dirty} "
        f"| {_fmt_time(row.last_session)} | {'LIVE' if row.live else '—'} "
        f"| {verdict.render()} |"
    )


def _judged(rows: list[Row], alive: Callable[[int], bool]) -> list[tuple[Row, Verdict]]:
    return [(r, judge(r, alive=alive)) for r in rows]


# Modifiers of a row that is still locked: git refuses `worktree remove` and
# `branch -D` on it, so never in the paste lists.
_LOCKED_MODIFIERS = frozenset({"stale lock", "server-lock"})


def _safe_lists(
    pairs: list[tuple[Row, Verdict]],
) -> tuple[list[str], list[str], list[str]]:
    """The three paste lists. Important 1: a row with a stale lock is still
    locked — git refuses `worktree remove`/`branch -D` on it, so such a
    row is excluded here, even though its verdict is "safe to remove". The row
    does stay in the table; only the pasteable commands skip it.

    The same holds for a server lock (#963, final review F1): the verdict
    "safe to remove" is right on content, but the lock of the session server is still
    there, so git refuses just as well. Those rows get their own section.
    """
    eligible = [
        (r, v)
        for r, v in pairs
        if v.safe and not _LOCKED_MODIFIERS.intersection(v.modifiers)
    ]
    safe_worktrees = [r.worktree for r, _ in eligible if r.worktree]
    safe_local = [r.name for r, _ in eligible if r.local]
    safe_remote = [r.name for r, _ in eligible if r.remote]
    return safe_worktrees, safe_local, safe_remote


def _render_command_blocks(
    safe_worktrees: list[str], safe_local: list[str], safe_remote: list[str]
) -> list[str]:
    lines = ["Commands — read the table first; this script runs nothing.", ""]
    # Paste order, not arbitrary: a branch that is checked out in a worktree
    # cannot be deleted by git ("Cannot delete branch … checked
    # out at …"), so worktree remove must come before branch -D. The reverse
    # order breaks a top-to-bottom paste halfway.
    if safe_worktrees:
        lines.append("```bash")
        lines += [f'git worktree remove "{path}"' for path in safe_worktrees]
        lines.append("```")
    if safe_local:
        lines.append("```bash")
        lines.append("git branch -D " + " ".join(safe_local))
        lines.append("```")
    if safe_remote:
        lines.append("```bash")
        lines.append("git push origin --delete " + " ".join(safe_remote))
        lines.append("```")
    if not (safe_remote or safe_local or safe_worktrees):
        lines.append("Nothing with verdict ✅ — no commands.")
    return lines


def _render_stale_locked_section(pairs: list[tuple[Row, Verdict]]) -> list[str]:
    """Important 1: the rows that `_safe_lists` deliberately left out — an orphaned
    lock is information, not permission. Never offer `git worktree unlock`:
    this script never unlocks, not even as a suggestion.
    """
    stale = [(r, v) for r, v in pairs if v.safe and "stale lock" in v.modifiers]
    if not stale:
        return []
    lines = [
        "",
        "Locked with a dead pid — release the lock by hand first, then run again:",
    ]
    lines += [
        f"  {r.worktree}   (lock-pid {r.lock_pid}, no longer in /proc)"
        for r, _ in stale
    ]
    return lines


def _render_server_locked_section(pairs: list[tuple[Row, Verdict]]) -> list[str]:
    """Final review F1 (#963): the rows that `_safe_lists` left out because the
    session server still keeps them locked. The session is over (otherwise it would be
    rank 2), but the server pid stays alive for weeks (spike R1). As with the
    stale lock: report, never offer an unlock command.
    """
    held = [(r, v) for r, v in pairs if v.safe and "server-lock" in v.modifiers]
    if not held:
        return []
    lines = [
        "",
        (
            "Locked by the session server — session ended; release the lock by hand "
            "if you want it gone (or wait for the next server start), then "
            "run again:"
        ),
    ]
    lines += [
        f"  {r.worktree}   (server-pid "
        f"{'unknown' if r.lock_pid is None else r.lock_pid}, session ended)"
        for r, _ in held
    ]
    return lines


def _render_port_blocks_section(rows: list[Row]) -> list[str]:
    """Important 2: `port_block` was measured and tested, but never shown.
    This is only the registration from `.port-registry` — which process
    actually holds the port is not looked up here (that is #438); an empty
    list therefore does not mean "nothing runs".
    """
    ported = sorted((r for r in rows if r.port_block is not None), key=lambda r: r.name)
    if not ported:
        return []
    lines = [
        "",
        "Port blocks (registered in worktrees/.port-registry, read-only):",
    ]
    lines += [
        f"  {r.name}  port {r.port_block}  {r.worktree or r.orphan_dir or 'no worktree'}"
        for r in ported
    ]
    lines.append(
        "The process that actually holds the port is not looked up here "
        "— an empty list above does not mean that nothing runs."
    )
    return lines


def render_cleanup(rows: list[Row], *, alive: Callable[[int], bool] = pid_alive) -> str:
    pairs = sorted(_judged(rows, alive), key=lambda p: (p[1].rank, p[0].name))
    lines = [TABLE_HEADER, TABLE_RULE]
    lines += [_row_line(row, verdict) for row, verdict in pairs]
    lines.append("")

    safe_worktrees, safe_local, safe_remote = _safe_lists(pairs)
    lines += _render_command_blocks(safe_worktrees, safe_local, safe_remote)
    lines += _render_stale_locked_section(pairs)
    lines += _render_server_locked_section(pairs)
    lines += _render_port_blocks_section(rows)
    return "\n".join(lines) + "\n"


def render_sessions(
    rows: list[Row], *, alive: Callable[[int], bool] = pid_alive
) -> str:
    pairs = [(r, v) for r, v in _judged(rows, alive) if r.session_id]
    # Live sessions first, also without a transcript (last_session=None): a live
    # session without a .jsonl file is more relevant than a dead session from
    # long ago, and must not sink to the bottom on a missing time.
    pairs.sort(key=lambda p: (p[0].live, p[0].last_session or 0.0), reverse=True)
    lines = [
        "| last session | live? | worktree | resume | verdict |",
        "|---|---|---|---|---|",
    ]
    for row, verdict in pairs:
        worktree = Path(row.worktree).name if row.worktree else "—"
        lines.append(
            f"| {_fmt_time(row.last_session)} | {'LIVE' if row.live else 'dead'} "
            f"| {worktree} | `claude --resume {row.session_id}` | {verdict.render()} |"
        )
    if len(lines) == 2:
        lines.append("| — | — | — | no sessions found | — |")
    return "\n".join(lines) + "\n"


def render_origin(rows: list[Row], *, alive: Callable[[int], bool] = pid_alive) -> str:
    return render_cleanup([r for r in rows if r.remote], alive=alive)


def render_only(
    rows: list[Row],
    branch: str,
    *,
    cwd: Path,
    alive: Callable[[int], bool] = pid_alive,
) -> tuple[str, bool]:
    """The view for lighthouse-close-issue: one row plus `cleanup: yes|no`.

    Cleanable = verdict "safe to remove", not locked, and the own cwd is not
    in the worktree — git refuses the first two, and the third saws off the branch
    the session sits on. On `yes` at most two commands follow, in
    paste order; never `push origin --delete`: the remote branch is the business of
    the merge, not of closing. This script does not run them.
    """
    match = [r for r in rows if r.name == branch]
    if not match:
        return f"cleanup: no — branch {branch} not found\n", False
    if len(match) > 1:
        # A detached worktree or orphan dir carries its directory name as name,
        # and that can be equal to the branch. Choosing would silently skip the other
        # row — with perhaps a running session.
        pairs = _judged(match, alive)
        lines = [TABLE_HEADER, TABLE_RULE]
        lines += [_row_line(r, v) for r, v in pairs]
        reason = (
            f"{len(match)} rows named {branch} (detached worktree or orphan dir)"
        )
        lines += ["", f"cleanup: no — {reason}"]
        return "\n".join(lines) + "\n", False
    target = match[0]
    ((_, verdict),) = _judged([target], alive)
    lines = [TABLE_HEADER, TABLE_RULE, _row_line(target, verdict), ""]

    wt = Path(target.worktree) if target.worktree else None
    if not verdict.safe:
        reason = verdict.render()
    elif _LOCKED_MODIFIERS.intersection(verdict.modifiers):
        reason = f"{verdict.render()} (locked)"
    elif wt is not None and (wt == cwd or wt in cwd.parents):
        reason = f"own cwd is in {wt}"
    elif wt is None and not target.local:
        reason = "nothing local to clean up"
    else:
        reason = None

    if reason is not None:
        lines.append(f"cleanup: no — {reason}")
        return "\n".join(lines) + "\n", False
    lines.append("cleanup: yes")
    if wt is not None:
        lines.append(f'git worktree remove "{target.worktree}"')
    if target.local:
        lines.append(f"git branch -D {target.name}")
    return "\n".join(lines) + "\n", True


def issue_branches(rows: list[Row], nr: int, pr_heads: list[str]) -> list[str]:
    """Branches of issue `nr`: PR heads plus rows with prefix `issue-<nr>-`.

    The dash belongs to the prefix: `issue-3-` must not match `issue-32-x`.
    Sorted, without duplicates.
    """
    prefix = f"issue-{nr}-"
    found = {r.name for r in rows if r.name.startswith(prefix)}
    return sorted(found | set(pr_heads))


def render_issue(
    rows: list[Row],
    nr: int,
    pr_heads: list[str] | None,
    *,
    cwd: Path,
    alive: Callable[[int], bool] = pid_alive,
) -> str:
    """A block per branch of issue `nr`: `branch: <b>` plus the `render_only` output.

    No second ladder: `render_only` decides. One addition: a branch whose
    row has no merged PR is never cleanable, even if the ladder says "safe to
    remove" (a local `issue-<nr>-` branch without a PR). `pr_heads is None`
    means the PR lookup failed; then there is no signal at all, and no
    signal is not a yes.
    """
    if pr_heads is None:
        return f"cleanup: no — PR lookup for issue #{nr} failed\n"
    branches = issue_branches(rows, nr, pr_heads)
    if not branches:
        return f"cleanup: no — no branches found for issue #{nr}\n"
    blocks: list[str] = []
    for branch in branches:
        text, ok = render_only(rows, branch, cwd=cwd, alive=alive)
        match = [r for r in rows if r.name == branch]
        if ok and not (len(match) == 1 and match[0].pr_state == "merged"):
            # Everything from `cleanup: yes` on (also the commands below it) is dropped.
            lines = text.splitlines()
            head = lines[: lines.index("cleanup: yes")]
            text = "\n".join(head + ["cleanup: no — no merged PR"]) + "\n"
        blocks.append(f"branch: {branch}\n{text}")
    return "\n".join(blocks)


def render_kpi(rows: list[Row], *, alive: Callable[[int], bool] = pid_alive) -> str:
    pairs = _judged(rows, alive)
    # Minor 6: the main checkout has `worktree` set too, but the skill
    # labels this number "Worktrees under `.claude/worktrees/`" — so only
    # what really sits under it counts.
    worktrees = sum(
        1 for r, _ in pairs if r.worktree and WORKTREE_DIRNAME in r.worktree
    )
    safe = sum(1 for _, v in pairs if v.safe)
    remote = sum(1 for r, _ in pairs if r.remote)
    warn = sum(1 for _, v in pairs if v.rank in (4, 7))
    return f"worktrees={worktrees} safe={safe} remote_branches={remote} warn={warn}\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit worktrees, branches, PR state and sessions. Changes nothing."
    )
    view = parser.add_mutually_exclusive_group()
    view.add_argument(
        "--cleanup",
        action="store_true",
        help="full table + command blocks (default)",
    )
    view.add_argument(
        "--sessions",
        action="store_true",
        help="claude --resume per row, sorted by last session",
    )
    view.add_argument("--origin", action="store_true", help="remote rows only")
    view.add_argument("--kpi", action="store_true", help="the KPI numbers, one line")
    view.add_argument(
        "--only",
        metavar="BRANCH",
        help="one branch + `cleanup: yes|no` (exit 0 only on yes), for lighthouse-close-issue",
    )
    view.add_argument(
        "--issue",
        type=int,
        metavar="NR",
        help="all branches of an issue, one block each with `cleanup: yes|no` "
        "(exit 2 if the PR lookup fails), for lighthouse-close-issue",
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=None,
        help="path to the main checkout (default: from git worktree list)",
    )
    args = parser.parse_args(argv)

    rows = collect(args.repo)
    if args.only is not None:
        text, ok = render_only(rows, args.only, cwd=Path.cwd().resolve())
        print(text, end="")
        return 0 if ok else 1
    if args.issue is not None:
        pr_heads = run_gh_issue_pr_heads(repo_slug(), args.issue)
        print(
            render_issue(rows, args.issue, pr_heads, cwd=Path.cwd().resolve()), end=""
        )
        return 2 if pr_heads is None else 0
    if args.sessions:
        print(render_sessions(rows), end="")
    elif args.origin:
        print(render_origin(rows), end="")
    elif args.kpi:
        print(render_kpi(rows), end="")
    else:
        print(render_cleanup(rows), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

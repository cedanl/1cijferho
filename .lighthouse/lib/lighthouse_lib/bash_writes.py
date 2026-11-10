# lighthouse: core@v0.8.1
"""`bash-write-check` (#871): roots, git records, reflog, the per-session
ledger, `pre`, and `post` with the dispatch to the existing gates.

See `docs/developer/planning/specs/2026-09-26-bash-write-gates-design.md`,
the sections on data flow (roots, post source 1/2), state, lock, pre, post,
the pool, dispatch, output and error handling for the semantics.
Called as `python3 bash_writes.py pre|post` with the hook payload on
stdin, via the wrapper `.claude/hooks/lighthouse/bash-write-check.sh`. The gates
live in `<repo-root>/.claude/hooks/lighthouse/` (`config.repo_root()`); a gate
that is not there, because its language pack is missing, is reported by post per file
without blocking (decision 19).

Two environment variables are test-only seams: `LIGHTHOUSE_BASH_GATES_DEADLINE`
(seconds, override of the post deadline, default 90, read by
`run_post`) and `LIGHTHOUSE_BASH_GATES_START_NS` (override of the pre start time, in
nanoseconds, read by `run_pre`).
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import config, git_invocations, repo_identity
from lighthouse_lib.deadline import Interrupted, run_until

# The directory of the gates, under the repo root.
HOOK_DIR = Path(".claude/hooks/lighthouse")

# The gates of each extension, in the order they run. A rewriting gate
# (`_SIGN_AFTER_GATE`) always comes before `code-language-check.sh`, so that one
# reads the version after `ruff format` or `gofmt`.
GATES: dict[str, tuple[str, ...]] = {
    ".sh": ("shell-lint-check.sh", "code-language-check.sh"),
    ".bash": ("shell-lint-check.sh", "code-language-check.sh"),
    ".py": ("ruff-format-check.sh", "code-language-check.sh"),
    ".go": ("gofmt-check.sh", "code-language-check.sh"),
    ".md": ("md-link-check.sh",),
    ".markdown": ("md-link-check.sh",),
}

# Set for every gate `run_gate` runs; `code_language.UNDER_GATES_ENV` reads it and
# then skips layer 2 (#37, decision 19).
UNDER_GATES_ENV = "LIGHTHOUSE_UNDER_BASH_WRITE_CHECK"

# The package that each gate supplies; if the gate is missing, the notice names it.
GATE_PACKS: dict[str, str] = {
    "shell-lint-check.sh": "shell",
    "ruff-format-check.sh": "python",
    "gofmt-check.sh": "go",
    "md-link-check.sh": "core",
    "code-language-check.sh": "core",
}


def gate_path(root: str, gate: str) -> Path:
    """The path of `gate` under `root`."""
    return Path(root) / HOOK_DIR / gate


# Fallback tokenizer for an Unparseable command (open quote): no real
# shell parse is possible any more, so a rough word split on whatever is never
# a path character anyway.
_TOKEN_RE = re.compile(r"[^\s'\"=;&|<>()]+")

# `%gd` of `git reflog --format=...%gd...` is `HEAD@{<epoch>}` with
# `--date=unix`.
_EPOCH_RE = re.compile(r"HEAD@\{(\d+)\}")

# Sentinel in the session directory: post once replaced an unreadable ledger. As long as
# it is there, pre seeds no missing roots (see `Ledger`, `_run_pre`).
_RESET_NAME = "ledger.reset"

# Markers older than this are cleaned up by `prune_markers` (spec: 3600 s).
_MARKER_MAX_AGE_S = 3600.0

# Pre timeout is 5s (spec); the deadline for the git reads within it leaves room
# for writing the marker itself.
_PRE_DEADLINE_S = 4.0


class GitError(Exception):
    """A git read action failed: non-zero exit, timeout, or the process does not start.

    `returncode` is `None` on a timeout or an `OSError` (the process then never
    ended with an exit code); the exception message is the
    stderr text of git, or the fixed text `git read past the deadline`
    on a timeout.
    """

    def __init__(self, root: str, returncode: int | None, message: str) -> None:
        super().__init__(message)
        self.root = root
        self.returncode = returncode


def git(root: str, *args: str, deadline: float) -> bytes:
    """`git --no-optional-locks -C root <args>`, stdout as bytes.

    Every non-zero exit, `TimeoutExpired` or `OSError` becomes a `GitError`.
    The timeout of the underlying `subprocess.run` is whatever remains of
    `deadline`, with a floor of 0.1s so that an already elapsed
    deadline does not reach `subprocess` as a negative or zero timeout.
    """
    argv = ["git", "--no-optional-locks", "-C", root, *args]
    timeout = max(0.1, deadline - time.time())
    try:
        result = subprocess.run(argv, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise GitError(root, None, "git read past the deadline") from exc
    except OSError as exc:
        raise GitError(root, None, str(exc)) from exc
    if result.returncode != 0:
        message = result.stderr.decode(errors="replace").strip()
        raise GitError(root, result.returncode, message)
    return result.stdout


def _root_of(path: str | None) -> str | None:
    """Repo root of `path`: nearest existing ancestor, then `repo_of`."""
    if path is None:
        return None
    ancestor = git_invocations.existing_ancestor(path)
    if ancestor is None:
        return None
    return git_invocations.repo_of(ancestor)


def _resolved_path(word: str, literal: bool, tilde: bool, base: str) -> str | None:
    """`resolve(word, ...)`, then make absolute against `base` if it is not yet."""
    resolved = git_invocations.resolve(word, literal, tilde)
    if resolved is None:
        return None
    if os.path.isabs(resolved):
        return os.path.normpath(resolved)
    return os.path.normpath(os.path.join(base, resolved))


def derive_roots(command: str, cwd: str) -> set[str]:
    """The root rules from the spec (Datastroom, *Wortels*).  # lighthouse: allow-language — title of a Dutch spec section

    Sources: the payload `cwd`; per `Step` of `git_invocations.walk` the
    directory state `dirs["here"]`; for the verb `git` the `git_target`; every
    word with a `/` or starting with `~`, resolved against the directory state of that
    moment (or `cwd` without a directory state). For an `Unparseable` command (open
    quote) this falls back to the cwd plus a rough tokenisation of the whole
    command, against the same path rule. Every root goes through
    `os.path.realpath`. Not filtered by project — `project_filter` does that.

    `step.dirs` is the LIVE directory state of `walk` (see `git_invocations.Step`):
    outside a sub-shell all Steps share the same dict object, which mutates
    further after each `yield` as soon as the consumer asks for the next Step. Each
    Step must therefore be PROCESSED inside this `for` loop, before the loop itself
    moves on to the next Step — not via `list(walk(...))` and only then
    reading, because then all Steps not separated by a sub-shell see the
    FINAL state of the whole walk instead of their own moment.
    """
    roots: set[str] = set()

    def add(path: str | None) -> None:
        root = _root_of(path)
        if root is not None:
            roots.add(root)

    add(cwd)
    try:
        for step in git_invocations.walk(command, cwd):
            here = step.dirs["here"]
            add(here)
            if git_invocations.basename(step.words[step.idx]) == "git":
                target, _raw, _from_new_dir, _action_idx = git_invocations.git_target(
                    step
                )
                add(target)
            base = here if here is not None else cwd
            for k, word in enumerate(step.words):
                if "/" in word or word.startswith("~"):
                    add(_resolved_path(word, step.literal[k], step.tildes[k], base))
    except git_invocations.Unparseable:
        for token in _TOKEN_RE.findall(command):
            if "/" in token or token.startswith("~"):
                add(_resolved_path(token, True, token.startswith("~"), cwd))

    return {os.path.realpath(root) for root in roots}


def project_filter(
    roots: set[str], project_dir: str, *, deadline: float
) -> tuple[set[str], list[str]]:
    """Limit `roots` to roots with the same `--git-common-dir` as `project_dir`,
    or with the same `origin` (`repo_identity.same_origin`, #36).

    Literally `rev-parse --path-format=absolute --git-common-dir`, passed through
    `os.path.realpath` — without that flag the main checkout gives a relative
    `.git` and a linked worktree an absolute path, and the worktree would
    wrongly drop out (`commit-test-gate.sh`). A root for which that
    call fails comes back as an error line, not as a `GitError`; if it fails
    for `project_dir` itself, this function does raise.
    """

    def common_dir(root: str) -> str:
        out = git(
            root,
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
            deadline=deadline,
        )
        return os.path.realpath(out.decode(errors="replace").strip())

    ref_common = common_dir(project_dir)
    kept: set[str] = set()
    errors: list[str] = []
    for root in roots:
        try:
            common = common_dir(root)
        except GitError as exc:
            errors.append(f"COULD NOT CHECK: {root} ({exc})")
            continue
        if common == ref_common:
            kept.add(root)
            continue
        # A worktree of a separate clone (forge) with the same `origin`
        # belongs too (#36).
        try:
            same, _reason = repo_identity.same_origin(
                project_dir, root, timeout=deadline - time.time()
            )
        except repo_identity.OriginError as exc:
            errors.append(f"COULD NOT CHECK: {root} ({exc})")
            continue
        if same:
            kept.add(root)
    return kept, errors


class ManifestError(Exception):
    """`.lighthouse/manifest.toml` exists but cannot be read as a manifest."""


def managed_files(root: str) -> dict[str, str]:
    """The file units in `<root>/.lighthouse/manifest.toml` (#88): repo-relative path to sha256.

    Those files are lighthouse's own: a gate that rewrites one (`ruff format`, `gofmt`)
    makes `init --upgrade` refuse it as locally modified. A block, hook or permission
    unit sits inside a file of the project (`AGENTS.md`, `.claude/settings.json`), so it
    does not count. No manifest is an empty dict; an unreadable one raises
    `ManifestError`.
    """
    import tomllib  # lazily, like config._read_toml: Python < 3.11 must still import this module

    manifest = Path(root) / ".lighthouse" / "manifest.toml"
    if not manifest.is_file():
        return {}
    try:
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        return {
            unit["path"]: unit["sha256"]
            for unit in data.get("unit", [])
            if unit.get("kind") == "file"
        }
    except (
        OSError, UnicodeDecodeError, tomllib.TOMLDecodeError, KeyError, TypeError, AttributeError
    ) as exc:
        raise ManifestError(f"cannot read {manifest}: {exc}") from exc


def _as_installed(root: str, rel: str, managed: dict[str, str]) -> bool:
    """Is `rel` a file unit whose bytes are still the sha256 that `init` recorded?

    Only then is there nothing for a gate to judge. A changed file, and a manifest entry
    that names a file of the project, still go to the gates.
    """
    want = managed.get(rel)
    if want is None:
        return False
    try:
        return hashlib.sha256((Path(root) / rel).read_bytes()).hexdigest() == want
    except OSError:
        return False


def head_of(root: str, *, deadline: float) -> str:
    """The current `HEAD` sha, or `"unborn"` in a repo without commits.

    `rev-parse --verify -q HEAD` normally succeeds with the sha. If it fails with
    exactly returncode 1, that is an empty repo: `symbolic-ref -q HEAD`
    confirms that there is a branch name (just no commit yet) and the
    result is `"unborn"`. Every other `GitError` — another returncode,
    `None` (timeout/OSError), or a failing `symbolic-ref` — is
    re-raised: that is a broken `.git`, not an empty repo.
    """
    try:
        out = git(root, "rev-parse", "--verify", "-q", "HEAD", deadline=deadline)
    except GitError as exc:
        if exc.returncode == 1:
            git(root, "symbolic-ref", "-q", "HEAD", deadline=deadline)
            return "unborn"
        raise
    return out.decode(errors="replace").strip()


def parse_porcelain(data: bytes) -> tuple[set[str], set[str]]:
    """`git status --porcelain -z -uall` output to `(changed, deleted)`.

    Record grammar: `XY path\\0`, for an `R`/`C` in X or Y followed by one more
    field with the old path. A `D` in X or Y means deleted. The old
    path of an `R` counts as deleted: it is still in HEAD but no longer
    on disk, and the net filter of post would otherwise report it as `COULD NOT
    CHECK` instead of letting it drop out. The old path of a
    `C` (copy) still exists and is skipped. Neither is ever a
    candidate. Paths come through `os.fsdecode`, never `.decode()` or
    `splitlines()`: a filename with a newline or non-ASCII bytes must not
    fall apart.
    """
    changed: set[str] = set()
    removed: set[str] = set()
    fields = data.split(b"\0")
    if fields and fields[-1] == b"":
        fields.pop()
    records = iter(fields)
    for field in records:
        if len(field) < 3:
            continue
        status = field[:2]
        path = os.fsdecode(field[3:])
        if b"R" in status or b"C" in status:
            old = next(records, None)
            if old is not None and b"R" in status:
                removed.add(os.fsdecode(old))
        if b"D" in status:
            removed.add(path)
        else:
            changed.add(path)
    return changed, removed


def parse_diff_tree(data: bytes) -> tuple[set[str], set[str]]:
    """`diff-tree --name-status -z` output to `(changed, deleted)`.

    Records are `status\\0path\\0` pairs (with `--no-renames` there is no
    separate old-path field). With a combined diff (`-c`, a merge) there is
    one status letter per parent in the same field (`"MM"`); a `D` anywhere
    in it means deleted.
    """
    changed: set[str] = set()
    removed: set[str] = set()
    fields = data.split(b"\0")
    if fields and fields[-1] == b"":
        fields.pop()
    records = iter(fields)
    for status in records:
        path_bytes = next(records, None)
        if path_bytes is None:
            break
        path = os.fsdecode(path_bytes)
        if b"D" in status:
            removed.add(path)
        else:
            changed.add(path)
    return changed, removed


class ReflogEntry(NamedTuple):
    """One line of `git reflog --format='%H%x1f%gd%x1f%gs'`."""

    sha: str
    epoch: int
    subject: str


def parse_reflog(text: str) -> list[ReflogEntry]:
    """`%H\\x1f%gd\\x1f%gs` lines to `ReflogEntry`s, newest first.

    The epoch comes from `%gd` (`HEAD@{<epoch>}` with `--date=unix`), not from
    a separate date field: that is exactly what `%gd` already contains.

    Splits on `"\\n"`, never `.splitlines()`: the latter also breaks on
    `\\x1c`–`\\x1e`, `\\x0c`, `\\x85`, U+2028 and U+2029, and git keeps those
    literally in `%gs` if the commit subject contains them. Such a line fragment
    then lacks its `\\x1f` separators and the unpack below crashes with
    a bare `ValueError` instead of the caller being able to treat it as a
    `GitError` (`COULD NOT CHECK`).
    """
    entries: list[ReflogEntry] = []
    for line in text.split("\n"):
        if not line:
            continue
        sha, gd, subject = line.split("\x1f", 2)
        match = _EPOCH_RE.search(gd)
        epoch = int(match.group(1)) if match else 0
        entries.append(ReflogEntry(sha, epoch, subject))
    return entries


def _is_merge_commit(subject: str) -> bool:
    return subject.startswith("commit (merge)")


def _commits_until_stop(
    entries: list[ReflogEntry], stop: str
) -> tuple[list[tuple[str, bool]], list[str]]:
    """`stop` branch of `commits_to_check`: walk to the entry with `sha == stop`.

    An entry with an empty subject (the first entry of a new worktree)
    is no movement: it sets no notice and does not block the notice of a
    later real movement.
    """
    collected: list[tuple[str, bool]] = []
    moved_subject: str | None = None
    for entry in entries:
        if entry.sha == stop:
            notes = (
                [f"HEAD moved without a commit ({moved_subject})"]
                if moved_subject
                else []
            )
            return collected, notes
        if entry.subject.startswith("commit"):
            collected.append((entry.sha, _is_merge_commit(entry.subject)))
        elif moved_subject is None and entry.subject:
            moved_subject = entry.subject
    return [], [f"reflog gap: ledger HEAD {stop} not found"]


def _commits_from_unborn(
    entries: list[ReflogEntry],
) -> tuple[list[tuple[str, bool]], list[str]]:
    """`stop == "unborn"` branch: all commit entries, no gap notice possible."""
    collected = [
        (entry.sha, _is_merge_commit(entry.subject))
        for entry in entries
        if entry.subject.startswith("commit")
    ]
    return collected, []


def _commits_since(
    entries: list[ReflogEntry], since: int | None
) -> tuple[list[tuple[str, bool]], list[str]]:
    """`stop is None` branch: fall back to reflog time, commit entries with epoch `>= since`.

    `since` is in whole epoch SECONDS, like the reflog epoch of
    `--date=unix`; the caller rounds the pre start time (nanoseconds) DOWN —
    rounding up would drop a commit from the same second.
    """
    collected: list[tuple[str, bool]] = []
    for entry in entries:
        if since is not None and entry.epoch < since:
            break
        if entry.subject.startswith("commit"):
            collected.append((entry.sha, _is_merge_commit(entry.subject)))
    return collected, []


def commits_to_check(
    entries: list[ReflogEntry], stop: str | None, since: int | None
) -> tuple[list[tuple[str, bool]], list[str]]:
    """Which reflog commits belong to this walk: `([(sha, is_merge)], notices)`.

    - `stop` set (not `"unborn"`): walk to the first entry with
      `sha == stop`. Entries with a subject that begins with `commit` are
      included; if there is also an entry that is not a commit (`checkout`, `pull`, …),
      then one notice for the first such movement. If `stop` never occurs
      in `entries`, that is a reflog gap: empty list plus notice.
    - `stop == "unborn"`: all commit entries count, no gap notice
      possible — there was no earlier HEAD yet to look for.
    - `stop is None`: fall back to reflog time. Commit entries count
      as long as their epoch is `>= since` (newest first, so this stops as soon as the
      time limit is passed). `since` is in whole epoch seconds, rounded
      down from the pre start time (`start_ns // 10**9`).

    With `stop` set (not `"unborn"`) AND a `since` the result is the
    union of both walks, deduplicated on sha (stop walk
    first). The stop walk alone is blind as soon as the call brings HEAD back
    to, or past, the ledger sha: `switch -c x; commit; switch main` already begins
    the reflog with that sha, and a `merge --ff-only` back stops before the
    commit. A reflog gap (fresh worktree on an old path) also keeps its
    notice that way, but no longer its blind spot. Only commit entries count,
    so a checkout or pull still yields nothing. Both walks
    are a prefix of `entries`, so the union stays newest-first.

    `is_merge` is `subject.startswith("commit (merge)")`.
    """
    if stop is not None and stop != "unborn":
        collected, notes = _commits_until_stop(entries, stop)
        if since is None:
            return collected, notes
        known = {sha for sha, _merge in collected}
        extra, _none = _commits_since(entries, since)
        for sha, is_merge in extra:
            if sha not in known:
                known.add(sha)
                collected.append((sha, is_merge))
        return collected, notes
    if stop == "unborn":
        return _commits_from_unborn(entries)
    return _commits_since(entries, since)


def read_reflog(root: str, *, deadline: float) -> list[ReflogEntry]:
    """`git reflog --date=unix --format='%H%x1f%gd%x1f%gs'`, parsed."""
    data = git(
        root,
        "reflog",
        "--date=unix",
        "--format=%H%x1f%gd%x1f%gs",
        deadline=deadline,
    )
    return parse_reflog(data.decode(errors="replace"))


def diff_commit(
    root: str, sha: str, is_merge: bool, *, deadline: float
) -> tuple[set[str], set[str]]:
    """What `sha` changed: `(changed, deleted)`, repo-relative.

    An ordinary commit uses `--root` (so that a root commit also gives a diff);
    a merge uses the combined diff (`-c`), which names only paths
    that differ from EVERY parent — conflict resolutions, not what the
    other branch brought in.
    """
    args = [
        "diff-tree",
        "-z",
        "-r",
        "--no-commit-id",
        "--name-status",
        "--no-renames",
        "-c" if is_merge else "--root",
        sha,
    ]
    data = git(root, *args, deadline=deadline)
    return parse_diff_tree(data)


# --- State (task 6): signatures, session directory, ledger, marker, pre --------

Signature = tuple[int, int, int, int]


def signature(path: str) -> Signature | None:
    """`(st_mtime_ns, st_ctime_ns, st_ino, st_size)` of `path`, `None` if it is not there."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_ctime_ns, st.st_ino, st.st_size)


def state_dir(session_id: str) -> Path:
    """`${XDG_RUNTIME_DIR:-/tmp}/lighthouse-bash-gates/<session_id>/`, created with mode `0700`.

    A `session_id` with a `/` or equal to `..` could escape the state
    directory; that is refused. `mkdir(..., parents=True)` creates
    missing parent directories (`lighthouse-bash-gates/` itself) with the default
    mode — only the session directory itself must be `0700`, and that mode is set
    once more explicitly after creation: a `umask` can influence the mode of
    `mkdir` itself.
    """
    if "/" in session_id or session_id == "..":
        raise ValueError(f"invalid session_id: {session_id!r}")
    base = Path(os.environ.get("XDG_RUNTIME_DIR") or "/tmp") / "lighthouse-bash-gates"
    path = base / session_id
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def marker_key(payload: dict) -> str:
    """`tool_use_id`, otherwise `session_id + "-" + sha256(tool_input.command)[:16]`.

    A `/` in the result would make the marker name fall outside the state directory
    (`<state>/<key>.json`); that is replaced, the key is otherwise not
    sanitised.
    """
    tool_use_id = payload.get("tool_use_id")
    if tool_use_id:
        key = str(tool_use_id)
    else:
        command = payload.get("tool_input", {}).get("command", "")
        digest = hashlib.sha256(command.encode("utf-8")).hexdigest()[:16]
        key = f"{payload.get('session_id', '')}-{digest}"
    return key.replace("/", "_")


class Ledger:
    """One session ledger: `roots` (path → root rule) plus `unreadable`.

    `unreadable` is `True` if `ledger.json` WAS there but contained no valid
    JSON or did not have the form `{root: {...}}` — an empty but usable
    ledger, with a signal for the caller that something was
    broken. `locked_ledger` never writes back as long as `unreadable` is still `True`
    on leaving (see there): a caller that deliberately wants to replace the
    broken ledger sets `unreadable = False` before the `with` closes.

    Whoever does that (only post, in `_claim`) also creates the sentinel
    `<state>/ledger.reset`: the fresh ledger knows only the roots that
    that post walked, and a root that is missing may have had earlier session writes.
    As long as the sentinel exists (the rest of the session) pre seeds no
    missing root; post then falls back for one and adds it.
    Overchecking once per root, never blind.
    """

    def __init__(
        self, roots: dict[str, dict] | None = None, unreadable: bool = False
    ) -> None:
        self.roots: dict[str, dict] = roots if roots is not None else {}
        self.unreadable = unreadable


def _read_ledger(path: Path) -> Ledger:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return Ledger()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return Ledger(unreadable=True)
    if not isinstance(data, dict):
        return Ledger(unreadable=True)
    return Ledger(roots=data)


def _atomic_write_json(directory: Path, target: Path, data: object) -> None:
    """Write `data` as JSON to `target`, atomically via a tempfile + `os.replace`."""
    fd, tmp_name = tempfile.mkstemp(dir=str(directory), prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
        os.replace(tmp_name, str(target))
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)
        raise


@contextlib.contextmanager
def locked_ledger(state: Path) -> Iterator[Ledger]:
    """Read/write `ledger.json` under an exclusive `flock` on `ledger.lock`.

    The lock covers only the read, the body of the `with` and the
    write-back — never a gate: pre (timeout 5s) must not wait for a
    running post. The write-back happens atomically (tempfile in
    the same directory, then `os.replace`) and only if the body
    ends without an exception AND `ledger.unreadable` is `False` at that moment; if the body
    raises an exception, or `unreadable` stayed `True`, `ledger.json`
    stays unchanged.

    The second is deliberate: pre must not silently replace a broken `ledger.json`
    with a fresh, empty ledger — then the spec's
    "Ledger onleesbaar" fallback (source 1 without `seen` filter, plus notice)
    loses its signal, and what this session already wrote earlier becomes unseen
    baseline. If a caller does want to replace the broken ledger deliberately (the
    later `post` fallback does that), it sets `ledger.unreadable = False`
    inside the `with` before the end of the body, and creates the sentinel `ledger.reset`
    under the same lock (see `Ledger`).
    """
    state = Path(state)
    lock_path = state / "ledger.lock"
    ledger_path = state / "ledger.json"
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            ledger = _read_ledger(ledger_path)
            clean_exit = False
            try:
                yield ledger
                clean_exit = True
            finally:
                # Write back only on a clean yield AND a readable
                # ledger: an `except`/`else` here would only re-raise
                # (TRY203), so the flag carries the distinction.
                if clean_exit and not ledger.unreadable:
                    _atomic_write_json(state, ledger_path, ledger.roots)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        os.close(lock_fd)


def seed_root(root: str, start_ns: int, *, deadline: float) -> dict:
    """The baseline for `root`: gate files already dirty from before `start_ns`, plus `head`.

    Source is `git status --porcelain -z -uall`, limited to paths with a
    gate extension (`GATES`) that still exist AND whose `ctime_ns < start_ns`
    — a path touched at or after the start time of pre may already have been written by
    the command itself (if pre overran, the command
    is already running) and does not belong in the baseline. Deleted paths (`D` in
    `parse_porcelain`) do not end up in `changed` and so never in
    `seen`.
    """
    data = git(root, "status", "--porcelain", "-z", "-uall", deadline=deadline)
    changed, _deleted = parse_porcelain(data)
    seen: dict[str, list[int]] = {}
    for rel in changed:
        if os.path.splitext(rel)[1] not in GATES:
            continue
        sig = signature(os.path.join(root, rel))
        if sig is None or sig[1] >= start_ns:
            continue
        seen[rel] = list(sig)
    return {
        "seen": seen,
        "head": head_of(root, deadline=deadline),
        "pending": [],
        "claims": {},
    }


def write_marker(state: Path, key: str, start_ns: int, roots: list[str]) -> None:
    """`<state>/<key>.json` <- `{"start_ns": start_ns, "roots": roots}`, atomically."""
    state = Path(state)
    target = state / f"{key}.json"
    _atomic_write_json(state, target, {"start_ns": start_ns, "roots": list(roots)})


def read_marker(state: Path, key: str) -> dict | None:
    """The marker of `key`, or `None` if it is missing or not a valid JSON dict."""
    path = Path(state) / f"{key}.json"
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def prune_markers(state: Path, now: float) -> None:
    """Remove marker files older than an hour.

    `ledger.json`, `ledger.lock` and the sentinel `ledger.reset` stay.
    """
    state = Path(state)
    try:
        names = os.listdir(state)
    except OSError:
        return
    for name in names:
        if name in ("ledger.json", "ledger.lock", _RESET_NAME):
            continue
        if not name.endswith(".json"):
            continue
        path = state / name
        try:
            age = now - path.stat().st_mtime
        except OSError:
            continue
        if age > _MARKER_MAX_AGE_S:
            with contextlib.suppress(OSError):
                path.unlink()


def _run_pre(payload: dict) -> None:
    """The actual steps of `run_pre`; exceptions go uncaught to the caller."""
    project_dir = os.environ.get("CLAUDE_PROJECT_DIR")
    if not project_dir:
        return
    start_ns_raw = os.environ.get("LIGHTHOUSE_BASH_GATES_START_NS")
    start_ns = int(start_ns_raw) if start_ns_raw else time.time_ns()
    deadline = time.time() + _PRE_DEADLINE_S

    command = payload["tool_input"]["command"]
    cwd = payload["cwd"]
    roots = derive_roots(command, cwd)
    kept, _errors = project_filter(roots, project_dir, deadline=deadline)

    state = state_dir(payload.get("session_id", ""))

    # The marker first: if pre is killed at its 5 s timeout during
    # seeding, post still has the roots and the start time (`since`,
    # the lower bound of source 2). Seeding stops at the pre deadline; a root
    # without a seed falls back in post, overchecking but never blind.
    write_marker(state, marker_key(payload), start_ns, sorted(kept))

    # Best-effort peek without a lock: saves a git status call per already
    # known root. An unreadable ledger is not seeded by pre at all — that is
    # the fallback of `post` (source 1 without `seen` filter), and a seed now
    # would never see its result written away anyway (`locked_ledger`
    # does not write back as long as `unreadable` stays set), so the
    # git status calls are then a pure waste of the pre deadline. The
    # real "not present yet?" decision falls under the lock, right before
    # registering — that also covers the race in which the ledger breaks
    # between this peek and that lock after all.
    #
    # If the sentinel `ledger.reset` is there (post replaced an unreadable
    # ledger), pre seeds nothing: a missing root may have had earlier
    # session writes, and a seed would make those the baseline. Post falls back
    # for such a root (it IS in the marker). Checked again under the lock
    # too: post creates the sentinel under that same lock.
    reset = state / _RESET_NAME
    peek = _read_ledger(state / "ledger.json")
    if not peek.unreadable and not reset.exists():
        existing = peek.roots
        for root in sorted(kept):
            if time.time() >= deadline:
                break
            if root in existing:
                continue
            try:
                record = seed_root(root, start_ns, deadline=deadline)
            except Exception:  # noqa: BLE001, S112 -- a failing seed skips post, not pre
                continue
            with locked_ledger(state) as ledger:
                if root not in ledger.roots and not reset.exists():
                    ledger.roots[root] = record

    prune_markers(state, time.time())


def run_pre(payload: dict) -> int:
    """PreToolUse step: write the marker, seed new roots, clean up old markers.

    The marker comes before the seeding, and the seeding stops at the pre deadline:
    a pre that dies on its timeout never leaves post without `since`.

    No seeding with an unreadable ledger, and not as long as the sentinel
    `ledger.reset` exists (post replaced the ledger; see `Ledger`). The marker
    IS written then: that makes post fall back for such a root.

    Always `0` — a PreToolUse exit 2 blocks the Bash call itself, and a
    broken seed or marker step must not hold up any work (post notices the
    gap). Every exception is therefore swallowed.
    """
    try:
        _run_pre(payload)
    except Exception:  # noqa: BLE001, S110 -- pre must never block the Bash call
        pass
    return 0


# --- post (task 7): source 1, claims, dispatch, output ------------------------

_PREFIX = "bash-write-check:"
_CANNOT = "COULD NOT CHECK"
# A gate with rc 0 whose FIRST output line holds this token has a notice for the user and
# the agent (#37, decision 19); any other output of a gate with rc 0 is dropped.
_NOTICE = "NOTICE"

# Spec *Dispatch*: one deadline of 90 s after the start of post, 10 s grace per
# gate — together well within the hook timeout of 120 s.
_POST_DEADLINE_S = 90.0
_GATE_GRACE_S = 10.0

# ruff-format-check.sh and gofmt-check.sh rewrite the file themselves: the file's verdict
# holds for the version after the last such gate that ran. The other gates only
# read: without a rewriting gate the verdict holds for the version from just
# before the first gate, so a write during a gate stays new.
_SIGN_AFTER_GATE = frozenset({"ruff-format-check.sh", "gofmt-check.sh"})

# `run_until` raises `Interrupted` if post itself received a forwarded signal
# (the hook timeout, a Ctrl-C): the gate was aborted, and post itself
# is stopping. `run_gate` reports that as this verdict. A gate that itself
# ends with 128 + n is not a signal but an ordinary non-verdict exit.
_STOPPED = "stopped"

# `run_gate`: the gate is not there, because its language pack is missing (decision 19).
# No block, but a notice per file; the file counts as seen afterwards.
_MISSING = "missing"


def _now() -> float:
    """The clock of `run_post`; a module function so that a test can replace it."""
    return time.time()


def _post_deadline_s() -> float:
    """`LIGHTHOUSE_BASH_GATES_DEADLINE` (test-only seam) or the default of 90 s."""
    raw = os.environ.get("LIGHTHOUSE_BASH_GATES_DEADLINE")
    try:
        return float(raw) if raw else _POST_DEADLINE_S
    except ValueError:
        return _POST_DEADLINE_S


def classify(rc: int, output: bytes) -> str:
    """The verdict of a gate: `"ok"`, `"finding"` or `"unchecked"`.

    rc 0 is ok. rc 2 is a finding, unless the FIRST output line contains the
    token `COULD NOT CHECK`: the not-looked-at form of every gate
    (`<gate>: COULD NOT CHECK — <reason>`, decision 18). Only the first line:
    shellcheck and ruff quote source lines, so a file that itself contains that token
    may not undo a real verdict. Every other rc (deadline 124,
    missing 127, a crash) is unchecked.
    """
    if rc == 0:
        return "ok"
    if rc != 2:
        return "unchecked"
    first = output.decode(errors="replace").split("\n", 1)[0]
    if _CANNOT in first:
        return "unchecked"
    return "finding"


def run_gate(root: str, rel: str, gate: str, deadline: float) -> tuple[str, bytes]:
    """Run `gate` on `rel` with a mock Edit-like payload.

    Payload `{"tool_name":"Bash","cwd":root,"tool_input":{"file_path":<abs>}}`,
    process cwd = `root`: `ruff-format-check.sh` looks for its `.ruff-version` from
    the process cwd, not through the payload. The gate gets `UNDER_GATES_ENV`
    set in its environment. Returns `(classify(rc, output),
    output)`, or `(_STOPPED, output)` when a forwarded signal stopped the gate,
    or `(_MISSING, message)` when the gate is not under the repo root.
    """
    path = gate_path(str(config.repo_root()), gate)
    if not path.is_file():
        return _MISSING, f"no {GATE_PACKS[gate]} check installed".encode()
    payload = {
        "tool_name": "Bash",
        "cwd": root,
        "tool_input": {"file_path": os.path.join(root, rel)},
    }
    try:
        rc, output = run_until(
            deadline,
            [str(path)],
            grace=_GATE_GRACE_S,
            cwd=root,
            stdin_bytes=json.dumps(payload).encode("utf-8"),
            capture=True,
            raise_on_signal=True,
            env={**os.environ, UNDER_GATES_ENV: "1"},
        )
    except Interrupted as stopped:
        return _STOPPED, stopped.output
    return classify(rc, output), output


def _printable(text: str) -> str:
    """`text` without surrogates: a non-UTF-8 filename (`os.fsdecode`) as `\\xNN`.

    Otherwise writing to stdout/stderr crashes on a
    `UnicodeEncodeError`, and then the notice is gone.
    """
    try:
        raw = text.encode("utf-8", "surrogateescape")
    except UnicodeEncodeError:
        return text.encode("utf-8", "backslashreplace").decode("utf-8")
    return raw.decode("utf-8", "backslashreplace")


class _Outcome:
    """What post reports: blocking parts (exit 2) and notices (exit 0).

    Spec *Uitvoer*, plus the decision from the plan: at exit 2 the bundle goes to
    stderr AND as `additionalContext` to stdout; at exit 0 only the
    latter, and with nothing to report an empty stdout. A part noted with
    `notice=True` also goes to the user, as a top-level `systemMessage`, at exit
    0 and at exit 2 (#37, decision 11 and 19).
    """

    def __init__(self) -> None:
        self.parts: list[str] = []
        self.notices: list[str] = []
        self.blocking = False

    def block(self, text: str) -> None:
        self.parts.append(text)
        self.blocking = True

    def note(self, text: str, notice: bool = False) -> None:
        self.parts.append(text)
        if notice:
            self.notices.append(text)

    def cannot(self, what: str) -> None:
        self.block(f"{_PREFIX} {_CANNOT}: {what}")

    def render(self, event: str) -> tuple[int, str, str]:
        if not self.parts:
            return 0, "", ""
        text = _printable("\n\n".join(self.parts))
        context: dict[str, object] = {
            "hookSpecificOutput": {
                "hookEventName": event,
                "additionalContext": text,
            }
        }
        if self.notices:
            context["systemMessage"] = _printable("\n\n".join(self.notices))
        stdout = json.dumps(context) + "\n"
        if self.blocking:
            return 2, stdout, text + "\n"
        return 0, stdout, ""


class _RootView(NamedTuple):
    """What post knows before the pool of one root.

    `fallback` is `True` if the ledger had no usable line for the root
    (absent, broken, or the whole ledger unreadable): then `seen` is empty —
    everything that is dirty is a candidate, overchecking is allowed, being blind is not.
    `head` is the HEAD as read now: the end point of the walk of source 2,
    the new ledger `head` (compare-and-swap) and what post registers a
    fallback root with. `start_head` is the ledger `head` with which this
    post began (`None` without a usable ledger line), `pending` the
    `pending` list from then. `since` is the pre start time from the marker in
    whole epoch seconds (rounded down), or `None` without a marker: the
    lower bound of source 2 in the fallback, and next to `start_head` the second
    walk that covers a return to the ledger sha or a reflog gap.
    """

    root: str
    seen: dict
    fallback: bool
    head: str
    start_head: str | None
    pending: list[str]
    since: int | None = None


def _valid_record(record: object) -> dict | None:
    """`record` if it has the form of a ledger line, otherwise `None` (= absent)."""
    if not isinstance(record, dict) or not isinstance(record.get("seen"), dict):
        return None
    if not isinstance(record.setdefault("claims", {}), dict):
        return None
    if not isinstance(record.setdefault("pending", []), list):
        return None
    return record


def _root_view(
    ledger: Ledger, root: str, since: int | None, *, deadline: float
) -> _RootView:
    """`seen`, `head` and `pending` of `root` from the ledger, plus HEAD now.

    The one place that decides what post already knows per root. Without a usable
    ledger line it is the fallback: `seen` empty, and source 2 walks from
    `since` (the pre start time from the marker). WITH a ledger line `since`
    goes along too: source 2 then walks to the ledger `head` AND since the start (see
    `commits_to_check`). Raises `GitError` if HEAD cannot be read.
    """
    head = head_of(root, deadline=deadline)
    record = None if ledger.unreadable else _valid_record(ledger.roots.get(root))
    if record is None:
        return _RootView(root, {}, True, head, None, [], since)
    start = record.get("head")
    pending = [p for p in record["pending"] if isinstance(p, str)]
    return _RootView(
        root,
        record["seen"],
        False,
        head,
        start if isinstance(start, str) else None,
        pending,
        since,
    )


_NO_START_NOTE = (
    "source 2 skipped: no ledger HEAD and no marker with a start time"
    " to walk back to"
)


class _Committed(NamedTuple):
    """Source 2 of one root.

    `paths`: everything a walked commit changed. `stranded`: per path the
    newest walked commit that changed it, unless a still newer one
    deleted it — if such a path is neither on disk nor in HEAD, it is
    not gone but only committed elsewhere (`switch -c x; commit; switch main`).
    `notes`: the notices of the walk.
    """

    paths: set[str]
    stranded: dict[str, str]
    notes: list[str]


def _committed_paths(view: _RootView, *, deadline: float) -> _Committed:
    """Source 2: what was committed in the call (see `_Committed`).

    Walks the reflog of the HEAD as read now back to the ledger `head` and,
    with a marker, also to the pre start time (the union, see
    `commits_to_check`); in the fallback (no ledger `head`) only to that
    start time. Without a marker AND without a ledger `head` there is no lower bound
    and source 2 is skipped with a notice. `HEAD == ledger-head` is
    a short-circuit only without a marker: WITH a marker the call may have committed
    along another branch and returned. Only commit entries count,
    per entry `diff_commit` (a merge via the combined diff). Deleted
    paths drop out; a path that a later commit removed drops out only in the
    net filter. An unborn HEAD yields nothing and is not an error — and
    `git reflog` may not even run there (rc 128). Raises `GitError` if
    reflog or diff-tree fails.
    """
    stop = view.start_head
    if view.head == "unborn":
        return _Committed(set(), {}, [])
    if stop == view.head and view.since is None:
        return _Committed(set(), {}, [])
    if stop is None and view.since is None:
        return _Committed(set(), {}, [_NO_START_NOTE])
    entries = read_reflog(view.root, deadline=deadline)
    commits, notes = commits_to_check(entries, stop, view.since)
    committed: set[str] = set()
    # Newest first: the first commit that names a path determines its fate.
    fate: dict[str, str | None] = {}
    for sha, is_merge in commits:
        changed, deleted = diff_commit(view.root, sha, is_merge, deadline=deadline)
        committed |= changed
        for rel in changed:
            fate.setdefault(rel, sha)
        for rel in deleted:
            fate.setdefault(rel, None)
    stranded = {rel: sha for rel, sha in fate.items() if sha is not None}
    return _Committed(committed, stranded, notes)


def _in_head(root: str, head: str, paths: list[str], *, deadline: float) -> set[str]:
    """Which of `paths` are in the tree of `head` (`git cat-file -e <head>:<path>`).

    Against the sha as read, not against `HEAD`: the same tree as where source 2
    ended. A missing path gives rc 128, just like a broken repo; but
    HEAD was just read, so every exit counts as "not in HEAD". Only a
    timeout or a git that does not start (`returncode is None`) raises.
    """
    if head == "unborn":
        return set()
    present: set[str] = set()
    for rel in paths:
        try:
            git(root, "cat-file", "-e", f"{head}:{rel}", deadline=deadline)
        except GitError as exc:
            if exc.returncode is None:
                raise
            continue
        present.add(rel)
    return present


def _in_a_ref(root: str, shas: set[str], *, deadline: float) -> set[str]:
    """Which of `shas` a ref still contains (`git for-each-ref --contains <sha>`).

    A commit that only the reflog still knows was deliberately thrown away (`reset
    --hard`, `--amend`, a rebase that drops it): its paths are not
    elsewhere, they are gone. Raises `GitError` if for-each-ref fails.
    """
    kept: set[str] = set()
    for sha in shas:
        out = git(
            root,
            "for-each-ref",
            "--count=1",
            "--format=%(refname)",
            "--contains",
            sha,
            deadline=deadline,
        )
        if out.strip():
            kept.add(sha)
    return kept


def _is_utf8(rel: str) -> bool:
    """Is the path valid UTF-8? The gates read it via jq, which turns it into U+FFFD."""
    try:
        os.fsencode(rel).decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


class _Pool(NamedTuple):
    """The pool of one root, after the net filter and `seen`.

    `candidates` go to the dispatch; `unchecked` are `(path, reason)` for
    paths that cannot be looked at (in HEAD but not on disk, only
    committed elsewhere, or not valid UTF-8); `keep` are the source 2 and `pending`
    paths without a verdict (they stay in `pending`); `settled` are paths that may leave `pending`
    (deleted, or already so in `seen`). `notes` are the notices of source 2.
    """

    view: _RootView
    candidates: list[str]
    unchecked: list[tuple[str, str]]
    keep: list[str]
    settled: list[str]
    notes: list[str]


def _root_pool(view: _RootView, *, deadline: float) -> _Pool:
    """Source 1, source 2 and `pending`, filtered on extension and net, sorted.

    `git status --porcelain -z -uall` is repo-relative, even if the Bash call
    wrote from a subdirectory. Net: a path that is neither on disk nor in
    HEAD is deleted and drops out, just like a path that source 1 names as
    deleted; if it IS in HEAD, it is `unchecked`. An
    exception to "deleted": a path that this walk saw committed and
    that no later walked commit deleted (`_Committed.stranded`)
    exists only elsewhere, e.g. on the branch the call returned from — also
    `unchecked`, with the commit attached. But only if a ref still
    contains that commit (`_in_a_ref`): if only the reflog knows it, the call threw it away
    and the path is simply deleted. A file unit of the manifest that is still as
    installed (`_as_installed`) is never a candidate and leaves `pending`. Raises
    `GitError` if status, reflog, diff-tree or for-each-ref fails, and `ManifestError`
    on an unreadable manifest.
    """
    managed = managed_files(view.root)
    data = git(view.root, "status", "--porcelain", "-z", "-uall", deadline=deadline)
    dirty, removed = parse_porcelain(data)
    committed, stranded, notes = _committed_paths(view, deadline=deadline)
    from_source_2 = committed | set(view.pending)
    installed = {
        rel for rel in dirty | from_source_2 if _as_installed(view.root, rel, managed)
    }
    pool = sorted(
        rel
        for rel in dirty | from_source_2
        if os.path.splitext(rel)[1] in GATES and rel not in installed
    )
    sigs = {rel: signature(os.path.join(view.root, rel)) for rel in pool}
    missing = [rel for rel in pool if sigs[rel] is None and rel not in removed]
    in_head = _in_head(view.root, view.head, missing, deadline=deadline)
    elsewhere_shas = {
        stranded[r] for r in missing if r in stranded and r not in in_head
    }
    kept = _in_a_ref(view.root, elsewhere_shas, deadline=deadline)
    stranded = {rel: sha for rel, sha in stranded.items() if sha in kept}

    result = _Pool(view, [], [], [], sorted(from_source_2 & installed), notes)
    for rel in pool:
        sig = sigs[rel]
        elsewhere = sig is None and rel not in in_head
        if elsewhere and (rel in removed or rel not in stranded):
            result.settled.append(rel)
            continue
        if sig is not None and view.seen.get(rel) == list(sig):
            result.settled.append(rel)
            continue
        if elsewhere:
            why = f"committed in {stranded[rel][:12]}, not on disk and not in HEAD"
            result.unchecked.append((rel, why))
        elif sig is None:
            result.unchecked.append((rel, "in HEAD, not on disk"))
        elif not _is_utf8(rel):
            result.unchecked.append((rel, "name is not valid UTF-8"))
        else:
            result.candidates.append(rel)
        if rel in from_source_2:
            result.keep.append(rel)
    return result


def _record_for(ledger: Ledger, view: _RootView) -> dict:
    """The ledger line of `view.root`; if it is missing, registered here.

    A fallback root (or a line that disappeared in the meantime) gets a
    fresh line with the HEAD as read now.
    """
    record = _valid_record(ledger.roots.get(view.root))
    if record is None:
        record = {"seen": {}, "head": view.head, "pending": [], "claims": {}}
        ledger.roots[view.root] = record
    return record


def _advance(record: dict, pool: _Pool) -> None:
    """Compare-and-swap on `head`, then update `pending` (under the lock).

    `head` becomes the HEAD as read only if the ledger still has the value
    with which this post began: a parallel post that was already further wins.
    `pending` gets the source 2 paths without a verdict; whatever was added in the meantime by
    another post stays.
    """
    view = pool.view
    if record.get("head") == view.start_head:
        record["head"] = view.head
    pending = {p for p in record["pending"] if isinstance(p, str)}
    record["pending"] = sorted((pending | set(pool.keep)) - set(pool.settled))


def _claimed_by_other(holder: object, key: str, now: float) -> bool:
    """Does `holder` carry a live claim of another post?

    A claim older than deadline plus grace counts as absent: that post was
    aborted without cleaning up. A malformed claim also counts as absent.
    """
    if not isinstance(holder, list) or len(holder) != 2:
        return False
    owner, epoch = holder
    if owner == key or not isinstance(epoch, (int, float)):
        return False
    return now - epoch <= _post_deadline_s() + _GATE_GRACE_S


def _claim(
    state: Path, key: str, pools: list[_Pool]
) -> tuple[list[tuple[_RootView, str]], bool]:
    """Under the lock: per root `_advance`, then claim what nobody is checking yet.

    A candidate that is meanwhile in `seen` with the current signature
    drops out (and out of `pending`: it has its verdict). An unreadable ledger
    is deliberately replaced here by a fresh one, with only the roots that
    this post walked (each with the walked HEAD): otherwise every
    following post falls back again. Under the same lock comes the sentinel
    `ledger.reset` (see `Ledger`), so that pre does not seed the roots that are
    missing here. Returns `(claimed, replaced)`.
    """
    now = _now()
    claimed: list[tuple[_RootView, str]] = []
    with locked_ledger(state) as ledger:
        replaced = ledger.unreadable
        if replaced:
            os.close(os.open(str(state / _RESET_NAME), os.O_CREAT | os.O_WRONLY, 0o600))
            ledger.roots = {}
            ledger.unreadable = False
        for pool in pools:
            view = pool.view
            record = _record_for(ledger, view)
            _advance(record, pool)
            for rel in pool.candidates:
                sig = signature(os.path.join(view.root, rel))
                if sig is not None and record["seen"].get(rel) == list(sig):
                    _drop_pending(record, rel)
                    continue
                if _claimed_by_other(record["claims"].get(rel), key, now):
                    continue
                record["claims"][rel] = [key, now]
                claimed.append((view, rel))
    return claimed, replaced


def _drop_pending(record: dict, rel: str) -> None:
    record["pending"] = [p for p in record["pending"] if p != rel]


def _drop_claim(record: dict, rel: str, key: str) -> None:
    holder = record["claims"].get(rel)
    if isinstance(holder, list) and holder and holder[0] == key:
        del record["claims"][rel]


def _record_verdict(
    state: Path,
    key: str,
    view: _RootView,
    rel: str,
    verdict: str,
    sig: Signature | None,
) -> None:
    """After all gates of a file, under the lock: drop the claim.

    On a real verdict, also out of `pending` and into `seen`.
    """
    with locked_ledger(state) as ledger:
        if ledger.unreadable:
            return
        record = _record_for(ledger, view)
        _drop_claim(record, rel, key)
        if verdict == "unchecked":
            return
        _drop_pending(record, rel)
        if sig is not None:
            record["seen"][rel] = list(sig)


def _release_claims(state: Path, key: str) -> None:
    """Remove every claim that still carries `key`: a claim exists only during the gate."""
    with locked_ledger(state) as ledger:
        if ledger.unreadable:
            return
        for raw in ledger.roots.values():
            record = _valid_record(raw)
            if record is None:
                continue
            for rel in list(record["claims"]):
                _drop_claim(record, rel, key)


def _report_verdict(
    outcome: _Outcome, label: str, gate: str, verdict: str, output: bytes
) -> None:
    """One gate in the bundle: `== <label> (<gate>) ==` plus its output.

    `label` is the repo-relative path, or the absolute path if the post covers more
    than one root — two roots can have the same relative path.
    """
    if verdict == "ok":
        text = output.decode(errors="replace").rstrip("\n")
        if _NOTICE in text.split("\n", 1)[0]:
            outcome.note(f"== {label} ({gate}) ==\n{text}", notice=True)
        return
    if verdict == _MISSING:
        outcome.note(f"{_PREFIX} {label} not checked — {output.decode()}")
        return
    lines = [f"== {label} ({gate}) =="]
    if verdict == "unchecked":
        lines.append(f"{_PREFIX} {_CANNOT}: {label} — {gate} gave no verdict")
    text = output.decode(errors="replace").rstrip("\n")
    if text:
        lines.append(text)
    outcome.block("\n".join(lines))


def _cannot_left(
    outcome: _Outcome, why: str, left: list[tuple[_RootView, str]]
) -> None:
    """One `COULD NOT CHECK` with `why` and every remaining absolute path."""
    outcome.cannot(
        f"{why}, not checked:\n"
        + "\n".join(f"  {os.path.join(v.root, r)}" for v, r in left)
    )


def _vanished(
    state: Path,
    key: str,
    view: _RootView,
    rel: str,
    outcome: _Outcome,
    *,
    deadline: float,
) -> None:
    """A claimed candidate is no longer on disk: no gate, no verdict.

    The net filter of the pool, now at the moment of the gate: if the path is
    in HEAD, it is `COULD NOT CHECK` and stays in `pending`;
    otherwise it is deleted and drops out (also from `pending`). If HEAD
    cannot be read, it counts as "in HEAD": loud, never silent.
    """
    try:
        in_head = bool(_in_head(view.root, view.head, [rel], deadline=deadline))
    except GitError:
        in_head = True
    with locked_ledger(state) as ledger:
        if not ledger.unreadable:
            record = _record_for(ledger, view)
            _drop_claim(record, rel, key)
            if not in_head:
                _drop_pending(record, rel)
    if in_head:
        path = os.path.join(view.root, rel)
        outcome.cannot(f"{path} (vanished before the gate; in HEAD, not on disk)")


def _dispatch(
    claimed: list[tuple[_RootView, str]],
    state: Path,
    key: str,
    deadline: float,
    outcome: _Outcome,
    *,
    absolute: bool,
) -> None:
    """Sequential, outside the lock, in sorted order, until the deadline.

    Sorted plus updating `seen` immediately means: even with many dirty
    files and a tight deadline every post makes progress, and the
    next post continues where this one stopped. With `absolute` (the post
    covers more than one root) the bundle header names the absolute path.

    A candidate that vanished between pool and gate does not go to the gate:
    it would see `[ -f ]` fail and give exit 0, and that is no verdict (see
    `_vanished`). If a forwarded signal aborts a gate, the dispatch stops:
    the rest is `COULD NOT CHECK`, just as after the deadline.
    """
    for index, (view, rel) in enumerate(claimed):
        # Per file, not per gate: a file whose first gate started gets all of
        # its gates (each `run_gate` is bounded by `deadline` itself), so every
        # post judges at least one whole file.
        if _now() >= deadline:
            _cannot_left(outcome, "deadline passed", claimed[index:])
            return
        path = os.path.join(view.root, rel)
        sig = before = signature(path)
        if before is None:
            _vanished(state, key, view, rel, outcome, deadline=deadline)
            continue
        combined = "ok"
        for gate in GATES[os.path.splitext(rel)[1]]:
            verdict, output = run_gate(view.root, rel, gate, deadline)
            if verdict == _STOPPED:
                _record_verdict(state, key, view, rel, "unchecked", None)
                _cannot_left(outcome, "signal received", claimed[index:])
                return
            if gate in _SIGN_AFTER_GATE and verdict != _MISSING:
                sig = signature(path)
            if combined != "unchecked":
                combined = verdict
            _report_verdict(outcome, path if absolute else rel, gate, verdict, output)
        _record_verdict(state, key, view, rel, combined, sig)


def _marker_roots(marker: dict | None) -> set[str]:
    """The roots that pre derived; empty without a (usable) marker."""
    if marker is None or not isinstance(marker.get("roots"), list):
        return set()
    return {r for r in marker["roots"] if isinstance(r, str)}


def _marker_since(marker: dict | None) -> int | None:
    """The pre start time from the marker in whole epoch seconds, rounded down.

    The reflog epoch (`--date=unix`) is in whole seconds; rounding up
    would drop a commit from the same second as pre. `None` without a
    marker or without a valid `start_ns`.
    """
    start_ns = marker.get("start_ns") if marker is not None else None
    if isinstance(start_ns, bool) or not isinstance(start_ns, int):
        return None
    return start_ns // 10**9


def _post_roots(
    payload: dict,
    marker_roots: set[str],
    project_dir: str,
    deadline: float,
    outcome: _Outcome,
) -> tuple[list[str], list[str]]:
    """`(existing roots, vanished roots)`, both sorted.

    Marker roots ∪ the project roots from the post payload: the post `cwd` may have
    shifted and a root in which a commit was made and then left
    must not drop out. A root that no longer exists (a deleted
    worktree) can no longer be checked and comes back separately, so that post
    can name it. Raises `GitError` if `project_dir` itself cannot be read.
    """
    roots = set(marker_roots)
    tool_input = payload.get("tool_input")
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    cwd = payload.get("cwd")
    derived = derive_roots(
        command if isinstance(command, str) else "",
        cwd if isinstance(cwd, str) and cwd else project_dir,
    )
    kept, errors = project_filter(derived, project_dir, deadline=deadline)
    for line in sorted(errors):
        outcome.block(f"{_PREFIX} {line}")
    roots.update(kept)
    present = sorted(root for root in roots if os.path.isdir(root))
    gone = sorted(roots.difference(present))
    return present, gone


def _post_in_state(
    payload: dict,
    outcome: _Outcome,
    project_dir: str,
    state: Path,
    key: str,
    deadline: float,
) -> None:
    marker = read_marker(state, key)
    marker_roots = _marker_roots(marker)
    try:
        roots, gone = _post_roots(payload, marker_roots, project_dir, deadline, outcome)
    except GitError as exc:
        outcome.cannot(f"{project_dir} ({exc})")
        return
    for root in gone:
        # Exit 0: the files vanished along with the root.
        outcome.note(f"{_PREFIX} {root} no longer exists; not checked")

    ledger = _read_ledger(state / "ledger.json")
    if ledger.unreadable and roots:
        outcome.note(f"{_PREFIX} ledger unreadable; fallback for every root")
    pools = _root_pools(
        roots, ledger, marker_roots, _marker_since(marker), deadline, outcome
    )
    if pools:
        claimed, replaced = _claim(state, key, pools)
        if replaced:
            outcome.note(
                f"{_PREFIX} unreadable ledger replaced; a root this call did not"
                " touch falls back at its next call"
            )
        _dispatch(claimed, state, key, deadline, outcome, absolute=len(pools) > 1)


def _root_pools(
    roots: list[str],
    ledger: Ledger,
    marker_roots: set[str],
    since: int | None,
    deadline: float,
    outcome: _Outcome,
) -> list[_Pool]:
    """The pool per root, with the notices and the paths that cannot be looked at.

    A fallback root (no usable ledger line) gets a notice
    only if it is in the marker — then it already existed before pre. A
    root that came into being during the call (`git worktree add`, `git init`)
    falls back silently.
    """
    pools: list[_Pool] = []
    for root in roots:
        try:
            view = _root_view(ledger, root, since, deadline=deadline)
            pool = _root_pool(view, deadline=deadline)
        except (GitError, ManifestError) as exc:
            outcome.cannot(f"{root} ({exc})")
            continue
        pools.append(pool)
        if view.fallback and root in marker_roots:
            outcome.note(f"{_PREFIX} fallback without ledger for {root}")
        for note in pool.notes:
            outcome.note(f"{_PREFIX} {root}: {note}")
        for rel, why in pool.unchecked:
            outcome.cannot(f"{os.path.join(root, rel)} ({why})")
    return pools


def _post(payload: dict, outcome: _Outcome) -> None:
    project_dir = os.environ.get("CLAUDE_PROJECT_DIR")
    if not project_dir:
        outcome.cannot("CLAUDE_PROJECT_DIR is missing, the project roots are unknown")
        return
    deadline = _now() + _post_deadline_s()
    state = state_dir(str(payload.get("session_id", "")))
    key = marker_key(payload)
    try:
        _post_in_state(payload, outcome, project_dir, state, key, deadline)
    finally:
        _release_claims(state, key)
        with contextlib.suppress(OSError):
            (state / f"{key}.json").unlink()


def run_post(payload: dict) -> tuple[int, str, str]:
    """PostToolUse(Failure) step: `(exitcode, stdout, stderr)`.

    Source 1 (dirty) per root, filtered on `seen`, claim, dispatch
    sorted until the deadline, bundle. Claims that still carry the key of this
    post go away before it returns — normally, after the deadline or via
    an exception — and the marker is cleaned up. An unexpected exception is
    `COULD NOT CHECK` with exit 2, never a traceback with exit 1.
    """
    event = payload.get("hook_event_name") or "PostToolUse"
    outcome = _Outcome()
    try:
        _post(payload, outcome)
    except Exception as exc:  # noqa: BLE001 -- post fails loudly, never with a traceback
        outcome.cannot(f"internal error in the dispatcher ({type(exc).__name__}: {exc})")
    return outcome.render(str(event))


def main(argv: list[str]) -> int:
    """`bash_writes.py pre|post`, hook payload on stdin.

    `pre` always returns 0. `post` writes its output itself; a payload
    that is not a JSON object is `COULD NOT CHECK` with exit 2.
    """
    mode = argv[0] if argv else ""
    if mode not in ("pre", "post"):
        sys.stderr.write(f"{_PREFIX} usage: bash_writes.py pre|post\n")
        return 2
    try:
        payload = json.loads(sys.stdin.buffer.read())
    except (ValueError, OSError):
        payload = None
    if mode == "pre":
        return run_pre(payload) if isinstance(payload, dict) else 0
    if isinstance(payload, dict):
        code, out, err = run_post(payload)
    else:
        outcome = _Outcome()
        outcome.cannot("the hook payload on stdin is not a JSON object")
        code, out, err = outcome.render("PostToolUse")
    sys.stdout.write(out)
    sys.stderr.write(err)
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

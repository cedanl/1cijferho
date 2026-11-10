# lighthouse: core@v0.8.1
"""Check a `gh pr create` body for a valid form (#1091).

Pure logic under the PreToolUse hook `pr-create-gate.sh`; design:
`docs/developer/planning/specs/2026-10-03-pr-verifieert-issue-design.md`,
section 3. `evidence.py` (the runner that executes the Evidence rows) and
`pr-create-gate.sh` share this lib, so the form is defined in one place.

A body must have at least one of three forms (mixing is allowed):

- `Closes #N` with a heading `### Evidence #N` and a table `AC | Command | Result`
  with one row per acceptance criterion of the issue;
- `Refs #N — <reason>` (reason at least `MIN_REASON` characters);
- `No issue: <reason>` (same).

Stdlib only: the hook runs under the host `python3`. The repo comes from
`lighthouse.toml` (`lighthouse_lib.config`) on every call; if that toml is
broken, the check fails loudly with a `ConfigError`. Without `forge.repo` only
`#N` counts, and the AC count reports that it is not measurable.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import config, headings, repo_identity
from lighthouse_lib.git_invocations import (
    Step,
    Unparseable,
    basename,
    existing_ancestor,
    resolve,
    walk,
)


def repo() -> str | None:
    """`forge.repo` from `lighthouse.toml`, or `None` if it is not there.

    No try/except: a `ConfigError` is the loud failure that `pr-create-gate.sh`
    reports as a blocker.
    """
    return config.load(config.config_path()).repo


CLOSE_WORDS = r"(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)"


def _closes_re() -> re.Pattern[str]:
    """Target: #N, <repo>#N or the full issue URL; without a repo only #N."""
    own_repo = repo()
    if own_repo is None:
        target = r"#(\d+)(?![\w/])"
    else:
        target = (
            rf"(?:https://github\.com/{re.escape(own_repo)}/issues/"
            rf"|{re.escape(own_repo)}#|#)(\d+)(?![\w/])"
        )
    return re.compile(rf"(?<![\w/]){CLOSE_WORDS}:?[ \t]+{target}", re.IGNORECASE)

REFS_SEP = ("—", "-", ":")  # on the same line as `Refs #N`
MIN_REASON = 10  # characters, after strip(), for Refs and No issue
_REFS_RE = re.compile(r"(?<![\w/])Refs[ \t]+#(\d+)(?![\w/])(.*)", re.IGNORECASE)
_NO_ISSUE_RE = re.compile(r"^[ \t]*No issue[ \t]*:(.*)$", re.IGNORECASE | re.MULTILINE)

EVIDENCE_HEADER = ("AC", "Command", "Result")
_SEPARATOR_CELL_RE = re.compile(r"^:?-+:?$")
_CELL_SPLIT_RE = re.compile(r"(?<!\\)\|")



def _ac_heading_re() -> re.Pattern[str]:
    """The AC heading in every language of `headings.toml` (an issue can have another language)."""
    return re.compile(
        rf"^#{{2,4}}[ \t]+{headings.any_of('acceptance_criteria')}[ \t]*$",
        re.IGNORECASE,
    )


_HEADING_RE = re.compile(r"^#{1,6}[ \t]")
_CHECKBOX_RE = re.compile(r"^[ \t]*[-*][ \t]+\[[ xX]\]")

AC_TIMEOUT_S = 5
# Total deadline per `main` call across all subprocess calls (gh and git). Must
# stay under the hook timeout (15 s in .claude/settings.json): a command hook
# that expires lets the tool call through, and that would be fail-open.
DEADLINE_S = 12
FALLBACK_NOTICE = (
    "pr-create-gate: AC count not measurable for #{nr} ({cause})"
    " — structure checked only"
)


class Row(NamedTuple):
    """One data row of the Evidence block; `command` is the raw cell, with backticks."""

    ac: str
    command: str
    result: str


class Verdict(NamedTuple):
    blockers: list[str]
    notices: list[str]


def strip_comments(body: str) -> str:
    """`\\r\\n` to `\\n` (web UI edit), then drop `<!-- … -->`."""
    body = body.replace("\r\n", "\n")
    return re.sub(r"<!--.*?-->", "", body, flags=re.DOTALL)


def closes_targets(body: str) -> list[int]:
    """The issues for which the body has a closing word, unique, in order of appearance."""
    seen: list[int] = []
    for m in _closes_re().finditer(strip_comments(body)):
        nr = int(m.group(1))
        if nr not in seen:
            seen.append(nr)
    return seen


def _parse_reason(rest: str) -> str:
    """The reason after a separator, or "" without a separator."""
    rest = rest.strip()
    if not rest.startswith(REFS_SEP):
        return ""
    return rest[1:].strip()


def _all_refs(body: str) -> list[tuple[int, str]]:
    """Every `Refs #N` with its reason ("" if the separator or the reason is missing)."""
    return [
        (int(m.group(1)), _parse_reason(m.group(2)))
        for m in _REFS_RE.finditer(strip_comments(body))
    ]


def refs_targets(body: str) -> list[int]:
    """Every issue number after a `Refs #N` outside comments, in order, with no reason required.

    Unlike `refs_issues`, a `Refs #N` without a (long) reason counts too: whoever only
    wants to know *whether* a PR body refers to an issue reads the gate form this way.
    """
    return [nr for nr, _ in _all_refs(body)]


def refs_issues(body: str) -> list[int]:
    """Only the `Refs #N` with a reason of at least `MIN_REASON` characters."""
    found: list[int] = []
    for nr, reason in _all_refs(body):
        if len(reason) >= MIN_REASON and nr not in found:
            found.append(nr)
    return found


def no_issue(body: str) -> bool:
    return any(
        len(m.group(1).strip()) >= MIN_REASON
        for m in _NO_ISSUE_RE.finditer(strip_comments(body))
    )


def _cells(line: str) -> list[str]:
    """Split a table line on unescaped `|`; the outer pipes are dropped."""
    s = line.strip()
    s = s.removeprefix("|")
    if s.endswith("|") and not s.endswith("\\|"):
        s = s[:-1]
    return [c.strip() for c in _CELL_SPLIT_RE.split(s)]


def _row(line: str, seq: int) -> Row | str:
    """One data row as `Row`, or the reason why it is invalid."""
    cells = _cells(line)
    if len(cells) != len(EVIDENCE_HEADER):
        return (
            f"row {seq} has {len(cells)} cells, expected {len(EVIDENCE_HEADER)}"
            " (write a `|` in a cell as `\\|`)"
        )
    if not all(cells):
        return f"row {seq} has an empty cell"
    ac, command, result = cells
    if len(command) < 3 or not (command.startswith("`") and command.endswith("`")):
        return f"the Command cell of row {seq} must be in backticks"
    return Row(ac, command, result)


def evidence_rows(body: str, nr: int) -> list[Row] | str:
    """The rows of `### Evidence #nr`, or the reason why the block is wrong."""
    lines = strip_comments(body).split("\n")
    heading = re.compile(rf"^###[ \t]+Evidence[ \t]+#{nr}[ \t]*$")
    start = next((i for i, r in enumerate(lines) if heading.match(r)), None)
    if start is None:
        return f"heading `### Evidence #{nr}` is missing"

    i = start + 1
    while i < len(lines) and not lines[i].strip():
        i += 1
    if i >= len(lines) or not lines[i].strip().startswith("|"):
        return f"no table under `### Evidence #{nr}`"
    if tuple(_cells(lines[i])) != EVIDENCE_HEADER:
        return f"the table header must be `| {' | '.join(EVIDENCE_HEADER)} |`"
    i += 1
    if i >= len(lines) or not all(
        _SEPARATOR_CELL_RE.match(c) for c in _cells(lines[i])
    ):
        return "the separator line `|---|---|---|` under the table header is missing"

    rows: list[Row] = []
    for line in lines[i + 1 :]:
        if not line.strip().startswith("|"):
            break
        row = _row(line, len(rows) + 1)
        if isinstance(row, str):
            return row
        rows.append(row)
    return rows or "the Evidence block has no data row"


def count_acs(issue_body: str) -> int | None:
    """Number of checkbox lines under the AC heading (`#{2,4}`, any language); `None` for no heading or zero."""
    lines = issue_body.replace("\r\n", "\n").split("\n")
    heading = _ac_heading_re()
    start = next((i for i, r in enumerate(lines) if heading.match(r)), None)
    if start is None:
        return None
    count = 0
    for line in lines[start + 1 :]:
        if _HEADING_RE.match(line):
            break
        if _CHECKBOX_RE.match(line):
            count += 1
    return count or None


_deadline: float | None = None  # `time.monotonic()` moment; set by `main`


def _budget(own_timeout: float) -> float:
    """`min(own timeout, remaining deadline)`; `<= 0` means: the budget is spent."""
    if _deadline is None:
        return own_timeout
    return min(own_timeout, _deadline - time.monotonic())


def fetch_ac_count(nr: int) -> tuple[int | None, str]:
    """`(count, "")` or `(None, cause)`; never an exception for an ordinary failure."""
    timeout = _budget(AC_TIMEOUT_S)
    if timeout <= 0:
        return None, "deadline reached"
    own_repo = repo()
    if own_repo is None:
        return None, "no forge.repo in lighthouse.toml"
    try:
        proc = subprocess.run(
            ["gh", "api", f"repos/{own_repo}/issues/{nr}", "--jq", ".body"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError:
        return None, "gh is missing"
    except subprocess.TimeoutExpired:
        if timeout < AC_TIMEOUT_S:
            return None, "deadline reached"
        return None, f"timeout after {AC_TIMEOUT_S:g} s"
    if proc.returncode != 0:
        return None, f"gh api failed (rc {proc.returncode})"
    count = count_acs(proc.stdout)
    if count is None:
        return None, "no acceptance criteria heading"
    return count, ""


def check_body(
    body: str,
    ac_count: Callable[[int], tuple[int | None, str]] = fetch_ac_count,
) -> Verdict:
    blockers: list[str] = []
    notices: list[str] = []

    targets = closes_targets(body)
    for nr in targets:
        rows = evidence_rows(body, nr)
        if isinstance(rows, str):
            blockers.append(f"Closes #{nr}: {rows}")
            continue
        count, cause = ac_count(nr)
        if count is None:
            notices.append(FALLBACK_NOTICE.format(nr=nr, cause=cause))
        elif count != len(rows):
            blockers.append(
                f"Closes #{nr}: the issue has {count} acceptance criteria,"
                f" the Evidence block {len(rows)} rows — one row per AC"
            )

    if not (targets or refs_issues(body) or no_issue(body)):
        hint = ""
        if _all_refs(body):
            hint = (
                f" `Refs #N` lacks a reason of at least {MIN_REASON} characters"
                f" (`Refs #N — <reason>`; separator {', '.join(REFS_SEP)})."
            )
        blockers.append(
            "no valid form in the PR body: `Closes #N` with `### Evidence #N`,"
            " `Refs #N — <reason>` or `No issue: <reason>`." + hint
        )
    return Verdict(blockers, notices)


# --- finding gh pr create, body source, scope -------------------------------

# Options with a value (separate or `=`); `--recover` takes a value too.
_VALUE_LONG = frozenset(
    {
        "--base", "--head", "--title", "--body", "--body-file", "--assignee",
        "--label", "--milestone", "--project", "--reviewer", "--repo",
        "--template", "--recover",
    }
)  # fmt: skip
_VALUE_SHORT = frozenset(
    {"-B", "-H", "-t", "-b", "-F", "-a", "-l", "-m", "-p", "-r", "-R", "-T"}
)
# Flags that yield no checkable body (the PR text then comes from elsewhere).
_NO_BODY_LONG = frozenset(
    {"--fill", "--fill-first", "--fill-verbose", "--web", "--editor"}
)
_NO_BODY_SHORT = frozenset("fwe")
GIT_TIMEOUT_S = 10
NOT_A_REPO = "not a repo"


class PrCreate(NamedTuple):
    """One `gh pr create` (or `gh pr new`) with the state of its own step.

    `cwd` is `step.dirs["here"]` at the moment of the step (None = unknown);
    `body_file` is already resolved against it. `problem` is a reason to
    block before any content check. `repo_problem` blocks regardless of the
    cwd: with a non-literal repo we do not know whether the PR goes here.
    """

    cwd: str | None
    repo: str | None
    body: str | None
    body_file: str | None
    problem: str | None
    repo_problem: str | None = None


Option = tuple[str, bool, bool]  # (value, literal, tilde)


def _option(step: Step, k: int) -> tuple[str | None, Option | None, list[str], int]:
    """The word at `k` as an option: `(name, value, no-body flags, next k)`."""
    words, literal, tildes = step.words, step.literal, step.tildes
    w = words[k]
    if w.startswith("--"):
        heading, eq, rest = w.partition("=")
        if heading in _VALUE_LONG and eq:
            return heading, (rest, literal[k], False), [], k + 1
        if heading in _VALUE_LONG:
            name = heading
        else:
            return None, None, [w] if w in _NO_BODY_LONG else [], k + 1
    elif w.startswith("-") and len(w) > 1:
        if w[:2] in _VALUE_SHORT and w not in _VALUE_SHORT:  # `-Fbody.md`, `-F=body.md`
            return w[:2], (w[2:].removeprefix("="), literal[k], False), [], k + 1
        if w not in _VALUE_SHORT:
            flags = [f"-{c}" for c in w[1:]] if set(w[1:]) <= _NO_BODY_SHORT else []
            return None, None, flags, k + 1
        name = w
    else:
        return None, None, [], k + 1
    if k + 1 >= len(words):  # option without a value
        return name, None, [], k + 1
    return name, (words[k + 1], literal[k + 1], tildes[k + 1]), [], k + 2


def _options(step: Step) -> tuple[dict[str, Option], list[str]]:
    """Option values per option name and the no-body flags found."""
    values: dict[str, Option] = {}
    flags: list[str] = []
    k = step.idx + 3
    while k < len(step.words) and step.words[k] != "--":
        name, value, new_flags, k = _option(step, k)
        flags.extend(new_flags)
        if name is not None and value is not None:
            values[name] = value
    return values, flags


def _first_value(values: dict[str, Option], *names: str) -> Option | None:
    return next((values[n] for n in names if n in values), None)


def _body_source(
    cwd: str | None, body_w: Option | None, file_w: Option | None, flags: list[str]
) -> tuple[str | None, str | None, str | None]:
    """`(body, body_file, problem)` per the body-source table of the spec."""
    if flags:
        return None, None, f"{flags[0]} gives no body to check"
    if body_w and file_w:
        return None, None, "choose one body source"
    source = file_w or body_w
    if source is None:
        return None, None, "no body"
    value, lit, til = source
    if file_w and value == "-":
        return None, None, "a body via stdin cannot be checked"
    if not lit:
        return None, None, "non-literal body — use --body-file"
    if not file_w:
        return value, None, None
    path = resolve(value, lit, til)
    if path is not None and os.path.isabs(path):
        return None, os.path.normpath(path), None
    if path is None or cwd is None:
        return None, None, "path of the body file cannot be resolved"
    return None, os.path.normpath(os.path.join(cwd, path)), None


def _gh_repo_env(step: Step) -> Option | None:
    """A `GH_REPO=…` before `gh` (also after `env`), as an option value; the last one wins."""
    found = None
    for k in range(step.idx):
        name, equals, value = step.words[k].partition("=")
        if name == "GH_REPO" and equals:
            found = (value, step.literal[k], False)
    return found


def _pr_create(step: Step) -> PrCreate:
    cwd = step.dirs["here"]  # LIVE dict: copy now, before the next next()
    values, flags = _options(step)
    # gh: `--repo`/`-R` beats `GH_REPO`.
    flag = _first_value(values, "--repo", "-R")
    repo_w = flag or _gh_repo_env(step)
    body, body_file, problem = _body_source(
        cwd,
        _first_value(values, "--body", "-b"),
        _first_value(values, "--body-file", "-F"),
        flags,
    )
    if repo_w and not repo_w[1]:
        # Does the PR go to this repo or another? We cannot tell, not even
        # from the cwd: block, wherever the command runs.
        source = "--repo" if flag else "GH_REPO"
        return PrCreate(
            cwd,
            None,
            body,
            body_file,
            problem,
            f"non-literal {source} — spell the repo out",
        )
    return PrCreate(cwd, repo_w[0] if repo_w else None, body, body_file, problem)


def find_pr_creates(command: str, cwd: str | None) -> list[PrCreate]:
    """Every `gh pr create`/`gh pr new` in `command`; raises `Unparseable` on an open quote."""
    found: list[PrCreate] = []
    for step in walk(command, cwd):
        w = step.words
        if (
            basename(w[step.idx]) == "gh"
            and step.idx + 2 < len(w)
            and w[step.idx + 1] == "pr"
            and w[step.idx + 2] in ("create", "new")
        ):
            found.append(_pr_create(step))
    return found


def _common_dir(path: str) -> tuple[str | None, str]:
    """`(git-common-dir, "")`, `(None, NOT_A_REPO)` or `(None, <git error>)`."""
    timeout = _budget(GIT_TIMEOUT_S)
    if timeout <= 0:
        return None, "COULD NOT CHECK: deadline reached"
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                path,
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            # The error text is matched below, and git translates it.
            env=dict(os.environ, LC_ALL="C"),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, "COULD NOT CHECK: git timeout or deadline reached"
    if proc.returncode == 0:
        return os.path.realpath(proc.stdout.strip()), ""
    if "not a git repository" in proc.stderr:
        return None, NOT_A_REPO
    return None, f"git failed (rc {proc.returncode}): {proc.stderr.strip()}"


def _normalize_repo(value: str) -> str | None:
    """`owner/repo` (lowercase) from a `--repo` value, or `None` if that form is missing.

    gh accepts `[HOST/]OWNER/REPO` and URLs (`https://…`, `ssh://git@…`, `git@host:o/r`),
    with or without `.git` or a trailing `/`.
    """
    rest = re.split(r"[?#]", value.strip(), maxsplit=1)[0]  # drop query and anchor
    has_scheme = "://" in rest
    rest = rest.split("://", 1)[-1]
    user = "@" in rest.split("/", 1)[0]
    if user:
        rest = rest.split("@", 1)[-1]
    has_host = has_scheme or user
    if user and not has_scheme:
        rest = rest.replace(":", "/", 1)  # scp form `git@host:owner/repo`
    elif has_scheme:
        host, _, path = rest.partition("/")
        rest = f"{host.split(':', 1)[0]}/{path}"  # drop the port
    parts = [d for d in rest.split("/") if d]
    if len(parts) > 2:
        has_host = True
    if has_host:
        parts = parts[1:]
    if len(parts) < 2:
        return None
    owner, name = parts[0], parts[1].removesuffix(".git")
    if not owner or not name:
        return None
    return f"{owner}/{name}".casefold()


def _scope_repo(repo_value: str) -> tuple[str, str]:
    """The scope decision for a literal repo (`--repo`/`-R` or `GH_REPO`).

    Without `forge.repo` there is no telling whether it is another repo: then check.
    """
    normalized = _normalize_repo(repo_value)
    if normalized is None:
        return "blokkeren", f"--repo {repo_value!r} does not have the form [HOST/]OWNER/REPO"
    own_repo = repo()
    if own_repo is None or normalized == own_repo.casefold():
        return "toetsen", ""
    return "overslaan", f"skipped — other repo {repo_value}"


def in_scope(pc: PrCreate, project_dir: str) -> tuple[str, str]:
    """`("toetsen"|"overslaan"|"blokkeren", reason)` for this `gh pr create`."""
    if pc.repo_problem is not None:
        return "blokkeren", pc.repo_problem
    if pc.repo is not None:
        return _scope_repo(pc.repo)
    if pc.cwd is None:
        return "blokkeren", "working directory of `gh pr create` cannot be resolved"
    # A `cd` to a directory that the command itself still creates: the nearest
    # existing ancestor determines the repo.
    path = existing_ancestor(pc.cwd)
    if path is None:
        return "blokkeren", f"working directory {pc.cwd} cannot be resolved"
    own_repo, error = _common_dir(project_dir)
    if own_repo is None:
        return "blokkeren", f"repo of the project cannot be determined ({error})"
    dir_, error = _common_dir(path)
    if dir_ is None:
        if error == NOT_A_REPO:
            return "overslaan", "skipped — outside this repo"
        return "blokkeren", f"repo of {path} cannot be determined ({error})"
    if dir_ == own_repo:
        return "toetsen", ""
    # Another common dir: a worktree of a separate clone (forge) with the
    # same `origin` is this repo after all (#36).
    timeout = _budget(GIT_TIMEOUT_S)
    if timeout <= 0:
        return "blokkeren", "COULD NOT CHECK: deadline reached"
    try:
        same, reason = repo_identity.same_origin(project_dir, path, timeout=timeout)
    except repo_identity.OriginError as exc:
        return "blokkeren", f"origin of {path} cannot be determined ({exc})"
    if same:
        return "toetsen", ""
    return "overslaan", f"skipped — outside this repo (git-common-dir {dir_} vs {own_repo}; {reason})"


BLOCKED_MESSAGE = (
    "pr-create-gate: BLOCKED — {reason}\n\n"
    "Write the body with /lighthouse-write-pr <nr> and pass it with --body-file <path>.\n"
    "Forms: `Closes #N` + `### Evidence #N` (one row per AC), `Refs #N — <reason>`,"
    " `No issue: <reason>`.\n"
)


def _forms(body: str, counts: dict[int, int | None]) -> str:
    forms = []
    for nr in closes_targets(body):
        rows = evidence_rows(body, nr)
        n_rows = len(rows) if isinstance(rows, list) else 0
        count = counts.get(nr)
        ac_text = f"{count} AC, " if count is not None else "AC not counted, "
        forms.append(f"Closes #{nr} ({ac_text}{n_rows} rows)")
    forms.extend(f"Refs #{nr}" for nr in refs_issues(body))
    if no_issue(body):
        forms.append("No issue")
    return ", ".join(forms)


def _check_create(pc: PrCreate) -> tuple[list[str], list[str]]:
    """`(blockers, lines for stdout)` for one in-scope `gh pr create`."""
    if pc.problem:
        return [pc.problem], []
    body = pc.body
    if pc.body_file is not None:
        try:
            body = Path(pc.body_file).read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return [f"body file {pc.body_file} does not exist"], []
        except OSError as e:
            return [f"body file {pc.body_file} cannot be read ({e.strerror})"], []
    assert body is not None  # problem is None => body or body_file is set
    counts: dict[int, int | None] = {}

    def ac_text(nr: int) -> tuple[int | None, str]:
        result = fetch_ac_count(nr)
        counts[nr] = result[0]
        return result

    verdict = check_body(body, ac_text)
    if verdict.blockers:
        return verdict.blockers, []
    return [], [
        f"pr-create-gate: body checked — {_forms(body, counts)}",
        *verdict.notices,
    ]


def main(argv: list[str] | None = None) -> int:
    """CLI: `pr_body_gate.py <cwd>`, command on stdin.

    Exit 0 (stdout: pass lines), 2 (stderr: blocker), 3 (open quote).
    Any other outcome, an exception included, is `COULD NOT CHECK` for the hook;
    a configuration error gives 1 with one line on stderr.
    """
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: pr_body_gate.py <cwd>  (command on stdin)", file=sys.stderr)
        return 64
    cwd = args[0] or None
    global _deadline
    _deadline = time.monotonic() + DEADLINE_S
    try:
        return _main(cwd)
    except config.ConfigError as exc:
        print(f"pr-create-gate: {exc}", file=sys.stderr)
        return 1


def _main(cwd: str | None) -> int:
    project = os.environ.get("CLAUDE_PROJECT_DIR") or str(config.repo_root())
    try:
        creates = find_pr_creates(sys.stdin.read(), cwd)
    except Unparseable:
        return 3

    blockers: list[str] = []
    lines: list[str] = []
    for pc in creates:
        decision, reason = in_scope(pc, project)
        if decision == "overslaan":
            lines.append(f"pr-create-gate: {reason}")
        elif decision == "blokkeren":
            blockers.append(reason)
        else:
            b, r = _check_create(pc)
            blockers.extend(b)
            lines.extend(r)
    if blockers:
        sys.stderr.write(BLOCKED_MESSAGE.format(reason="; ".join(blockers)))
        return 2
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())

# lighthouse: core@v0.8.1
"""Evidence allowlist and runner for the PR recheck (#1091).

A PR body is untrusted text. This lib therefore runs an Evidence row (the
Command cell of `### Evidence #N`, see `pr_body_gate.py`) only when its argv
fits the allowlist literally: no shell (never `shell=True`), no operators, no
write options, no options that start another program. What does not fit never
runs; the row is then `invalid`.

Extending the allowlist is a change of this lib *and* of the table in
`/lighthouse-write-pr`, never something a PR body can do. Design:
`docs/developer/planning/specs/2026-10-03-pr-verifieert-issue-design.md`,
section 2 (allowlist) and 4 (comparison rule).

CLI (tasks 6 and 7 call it):

    evidence.py check '<Command cell>'
    evidence.py run --issue N --body-file PATH --cwd DIR [--before-pr]

The outcomes and the table head are in `headings.toml` and follow `[project]
language`; the pytest path comes from `[test_gate] evidence_test_dir`.

Stdlib only: runs under the host `python3`.
"""

from __future__ import annotations

import argparse
import os
import posixpath
import re
import select
import shlex
import signal
import subprocess
import sys
import time

from pathlib import Path

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import config, headings
from lighthouse_lib.pr_body_gate import Row, evidence_rows

# Options without a value, per command; "VAL" = options that take one value (separate or with '=').
GREP_FLAGS = {
    "-F", "-E", "-G", "-i", "-n", "-c", "-l", "-L", "-q", "-r", "-R", "-w", "-x",
    "-o", "-s", "-h", "-H", "-v",
    "--fixed-strings", "--extended-regexp", "--basic-regexp", "--ignore-case",
    "--line-number", "--count", "--files-with-matches", "--files-without-match",
    "--quiet", "--recursive", "--dereference-recursive", "--word-regexp",
    "--line-regexp", "--only-matching", "--no-messages", "--no-filename",
    "--with-filename", "--invert-match",
}  # fmt: skip
GREP_VAL = {"-e", "--regexp", "--include", "--exclude"}
GIT_GREP_FLAGS = GREP_FLAGS - {
    "-r", "-R", "-s", "--recursive", "--dereference-recursive", "--no-messages",
}  # fmt: skip
GIT_GREP_VAL = {"-e"}
LS_FLAGS = {"-l", "-a", "-A", "-1", "-d", "-R", "-h"}
WC_FLAGS = {"-l", "-w", "-c", "-m"}
CAT_FLAGS = {"-n"}
# test: all arguments are one expression; none of them writes or executes.
GIT_LOG_FLAGS = {"--oneline", "--stat", "--name-only", "--"}
GIT_LOG_VAL = {
    "-n",
    "--format",
    "--pretty",
    "--grep",
}  # -n only with a number; -n5 is allowed too
GIT_LOG_EQUALS_ONLY = {"--format", "--pretty"}
GIT_SUBS = {"log", "show", "ls-files", "grep"}
PYTEST_FLAGS = {"-q", "-v"}  # paths: normpath starts with `[test_gate] evidence_test_dir`
GH_API_VAL = {"--jq", "-q"}  # graphql: plus exactly one -f query=…
GH_VIEW_FLAGS = {"--comments", "-c"}
GH_VIEW_VAL = {"--json", "--jq", "-q", "--repo", "-R"}

PUNCTUATION = set("();<>|&")
# The outcomes are keys of `headings.toml`; the text follows `[project] language`.
HOLDS = "evidence_holds"
FAILS = "evidence_fails"
INVALID = "evidence_invalid"
UNMEASURABLE = "evidence_unmeasurable"
AFTER_PR_UNMEASURED = "evidence_after_pr_unmeasured"
OUTCOMES = (HOLDS, FAILS, INVALID, UNMEASURABLE, AFTER_PR_UNMEASURED)
GH_PROBE_TIMEOUT_S = 10
MAX_OUTPUT = 4 * 1024 * 1024  # bytes, stdout and stderr together
_READ_CHUNK = 65536
# gojq (`gh --jq`) can read the environment and stdin; an Evidence row never needs that.
_JQ_FORBIDDEN_RE = re.compile(r"\benv\b|\$ENV|\binputs?\b")
_REF_FORBIDDEN = ("..", "@{", "~", "^")
_SUB_ENV = {
    "GIT_PAGER": "cat",
    "PAGER": "cat",
    "GH_PAGER": "cat",
    "GH_PROMPT_DISABLED": "1",
    "NO_COLOR": "1",
}


def split_command(cell: str) -> list[str] | str:
    """The Command cell as argv, or the reason why it is not a bare argv."""
    s = cell.strip()
    if len(s) >= 2 and s.startswith("`") and s.endswith("`"):
        s = s[1:-1]
    s = s.replace("\\|", "|")
    if "$(" in s:
        return "`$(` (command substitution) is not allowed"
    if "`" in s:
        return "a backtick in the command is not allowed"
    lexer = shlex.shlex(s, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        argv = list(lexer)
    except ValueError as error:
        return f"the command cannot be split ({error})"
    for token in argv:
        if token and set(token) <= PUNCTUATION:
            return f"shell operator `{token}` is not allowed"
    if not argv:
        return "empty command"
    return argv


class _Parsed:
    """Result of `_parse`: positional args, args after `--` and (option, value) pairs."""

    def __init__(self) -> None:
        self.pos: list[str] = []
        self.after: list[str] = []
        self.vals: list[tuple[str, str]] = []


def _value(
    args: list[str], i: int, name: str, rest: str, *, attached: bool
) -> tuple[str, int] | str:
    """The value of option `name`; `(value, new index)` or a reason.

    `attached`: the value is attached to the option (`--name=rest`, `-nrest`).
    Then that is the whole value, even when `rest` is empty: git, GNU grep and
    pflag read `--name=` as an empty value and parse the next word as a new
    option. If we swallowed that word as the value, `--output=x` would slip
    past. An empty value is therefore refused, as is a short value that starts
    with `=` (`-q=x`: pflag strips the `=`, getopt does not).
    """
    if attached:
        if not rest:
            return f"`{name}=` with an empty value is not allowed"
        if not name.startswith("--") and rest.startswith("="):
            return f"`{name}{rest}`: a value that starts with `=` is not allowed"
        return rest, i
    if i >= len(args):
        return f"`{name}` lacks a value"
    if not args[i]:
        return f"`{name}` with an empty value is not allowed"
    return args[i], i + 1


def _parse_long(
    args: list[str], i: int, flags: set[str], val: set[str], out: _Parsed
) -> int | str:
    """One `--long` word (`args[i-1]`); returns the new index or a reason."""
    tok = args[i - 1]
    name, equals, value = tok.partition("=")
    if name not in val:
        return i if tok in flags else f"option `{tok}` is not in the allowlist"
    got = _value(args, i, name, value, attached=bool(equals))
    if isinstance(got, str):
        return got
    out.vals.append((name, got[0]))
    return got[1]


def _parse_short(
    args: list[str], i: int, flags: set[str], val: set[str], out: _Parsed
) -> int | str:
    """One short cluster (`-nF`, `-n5`, `-efoo`; `args[i-1]`); the new index or a reason."""
    tok = args[i - 1]
    for pos, letter in enumerate(tok[1:], start=1):
        short_opt = f"-{letter}"
        if short_opt in val:
            glued = tok[pos + 1 :]
            got = _value(args, i, short_opt, glued, attached=bool(glued))
            if isinstance(got, str):
                return got
            out.vals.append((short_opt, got[0]))
            return got[1]
        if short_opt not in flags:
            return f"option `{short_opt}` (in `{tok}`) is not in the allowlist"
    return i


def _parse(
    args: list[str], flags: set[str], val: set[str], *, dd_ok: bool = False
) -> _Parsed | str:
    """Split `args` into flags, value options and positional args; a reason for anything else.

    A word that starts with `-` and is on neither list is refused. Short
    clusters (`-nF`) are allowed when every letter is in `flags`; a value letter
    (`-n5`, `-efoo`) swallows the rest of the word, or else the next word.
    """
    out = _Parsed()
    i = 0
    while i < len(args):
        tok = args[i]
        i += 1
        if tok == "--":
            if not dd_ok:
                return "`--` is not allowed here"
            out.after = args[i:]
            return out
        if tok == "-" or not tok.startswith("-"):
            out.pos.append(tok)
            continue
        if tok in flags:
            continue
        step = (_parse_long if tok.startswith("--") else _parse_short)(
            args, i, flags, val, out
        )
        if isinstance(step, str):
            return step
        i = step
    return out


def _jq_reason(vals: list[tuple[str, str]]) -> str | None:
    for name, value in vals:
        if name in {"--jq", "-q"} and _JQ_FORBIDDEN_RE.search(value):
            return f"`{name}` expression reads the environment or input"
    return None


def _ref_reason(args: list[str]) -> str | None:
    for a in args:
        if any(t in a for t in _REF_FORBIDDEN):
            return f"`{a}` is branch-relative (`..`, `@{{`, `~`, `^`)"
    return None


def _check_grep(args: list[str]) -> str | None:
    parsed = _parse(args, GREP_FLAGS, GREP_VAL, dd_ok=True)
    return parsed if isinstance(parsed, str) else None


def _check_git(args: list[str]) -> str | None:
    if not args or args[0] not in GIT_SUBS:
        return "git: only log, show, ls-files and grep, without global options"
    sub, rest = args[0], args[1:]
    if sub == "grep":
        parsed = _parse(rest, GIT_GREP_FLAGS, GIT_GREP_VAL, dd_ok=True)
        if isinstance(parsed, str):
            return parsed
        has_e = any(name == "-e" for name, _ in parsed.vals)
        # The first positional word without `-e` is the pattern, not a ref.
        return _ref_reason(parsed.pos[0 if has_e else 1 :] + parsed.after)
    # Without `=`, git reads `--pretty` as a bare flag and `--format` as an error: the
    # next word is then an option, not a value. Only the `=` form is unambiguous.
    bare = next((a for a in rest if a in GIT_LOG_EQUALS_ONLY), None)
    if bare is not None:
        return f"`{bare}` only as `{bare}=<value>`"
    parsed = _parse(rest, GIT_LOG_FLAGS, GIT_LOG_VAL, dd_ok=True)
    if isinstance(parsed, str):
        return parsed
    for name, value in parsed.vals:
        if name == "-n" and not value.isdigit():
            return "`-n` accepts only a number"
        if name in {"--format", "--pretty"} and "%G" in value:
            return f"`{name}` with `%G` starts gpg"
    return _ref_reason(parsed.pos + parsed.after)


def _check_pytest(args: list[str]) -> str | None:
    if args[:2] != ["run", "pytest"]:
        return "uv: only `uv run pytest`, without options between `run` and `pytest`"
    parsed = _parse(args[2:], PYTEST_FLAGS, set())
    if isinstance(parsed, str):
        return parsed
    if not parsed.pos:
        return "uv run pytest without a path would run the whole suite"
    test_dir = _pytest_dir()
    for tok in parsed.pos:
        path = posixpath.normpath(tok.split("::", 1)[0])
        if not path.startswith(test_dir):
            return f"path `{tok}` is not under `{test_dir}`"
    return None


def _pytest_dir() -> str:
    """`[test_gate] evidence_test_dir` as `dir/`; a `ConfigError` propagates."""
    value = config.lookup(config.load(config.config_path()), "test_gate.evidence_test_dir")
    return posixpath.normpath(str(value)) + "/"


def _graphql_field(args: list[str]) -> tuple[list[str], str] | str:
    """Take the one `-f query=…` out of `args`; `(rest, field)` or a reason."""
    fields: list[str] = []
    rest: list[str] = []
    i = 0
    while i < len(args):
        if args[i] != "-f":
            rest.append(args[i])
            i += 1
        elif i + 1 < len(args):
            fields.append(args[i + 1])
            i += 2
        else:
            return "`-f` lacks a value"
    if len(fields) != 1:
        return "gh api graphql: exactly one `-f query=…`"
    if not fields[0].startswith("query="):
        return "gh api graphql: the `-f` value must start with `query=`"
    if "mutation" in fields[0].lower():
        return "gh api graphql: a mutation is not allowed"
    return rest, fields[0]


def _check_gh_api(args: list[str]) -> str | None:
    # `-f` is for graphql only; without `graphql`, `_parse` refuses it.
    graphql = "graphql" in args
    if graphql:
        field = _graphql_field(args)
        if isinstance(field, str):
            return field
        args = field[0]
    parsed = _parse(args, set(), GH_API_VAL)
    if isinstance(parsed, str):
        return parsed
    if len(parsed.pos) != 1:
        return "gh api: exactly one path or `graphql`"
    path = parsed.pos[0]
    if graphql and path != "graphql":
        return "gh api graphql: `graphql` must be the only path"
    if "://" in path or path.startswith("//"):
        return "gh api: a path only, not a URL"
    return _jq_reason(parsed.vals)


def _check_gh_view(args: list[str]) -> str | None:
    parsed = _parse(args, GH_VIEW_FLAGS, GH_VIEW_VAL)
    if isinstance(parsed, str):
        return parsed
    # Without a number, gh reads the branch of the cwd. Entry C measures in a detached
    # worktree on `origin/main`: there is no branch there, and the row fails after the merge.
    if len(parsed.pos) != 1:
        return "gh view: exactly one issue or PR number (or github.com URL)"
    for tok in parsed.pos:
        if "://" in tok and not tok.startswith("https://github.com/"):
            return "gh view: a github.com URL only"
    return _jq_reason(parsed.vals)


def _check_gh(args: list[str]) -> str | None:
    if args[:1] == ["api"]:
        return _check_gh_api(args[1:])
    if args[:1] in (["issue"], ["pr"]) and args[1:2] == ["view"]:
        return _check_gh_view(args[2:])
    return "gh: only `gh api`, `gh issue view` and `gh pr view`"


def check(argv: list[str]) -> str | None:
    """`None` = allowed, otherwise the reason. `argv[0]` is the bare name, no path or assignment."""
    if not argv:
        return "empty command"
    name, args = argv[0], argv[1:]
    if name == "grep":
        return _check_grep(args)
    if name == "git":
        return _check_git(args)
    if name == "ls":
        return _flags_only(args, LS_FLAGS)
    if name == "wc":
        return _flags_only(args, WC_FLAGS)
    if name == "cat":
        return _flags_only(args, CAT_FLAGS)
    if name == "test":
        return None
    if name == "uv":
        return _check_pytest(args)
    if name == "gh":
        return _check_gh(args)
    return f"`{name}` is not in the allowlist"


def _flags_only(args: list[str], flags: set[str]) -> str | None:
    parsed = _parse(args, flags, set())
    return parsed if isinstance(parsed, str) else None


def _strip_backticks(cell: str) -> str:
    s = cell.strip()
    if len(s) >= 2 and s.startswith("`") and s.endswith("`"):
        s = s[1:-1]
    return s


def expected(result: str) -> tuple[str, bool]:
    """The Result cell normalised: `(expected fragment, was_after_pr)`.

    The prefix `na PR: ` counts in every language of `headings.toml`.
    """
    s = _strip_backticks(result).replace("\\|", "|")
    for after_pr in headings.variants("evidence_after_pr"):
        if s.startswith(after_pr):
            return s[len(after_pr) :], True
    return s, False


def outcome_of(cell: str) -> str | None:
    """The outcome a table cell names, in any language; `None` when the cell names none.

    The whole cell decides: `klopt` is a prefix of `klopt niet`.
    """
    for key in OUTCOMES:
        if re.fullmatch(rf"\s*{headings.any_of(key)}\s*", cell):
            return key
    return None


def _stop(proc: subprocess.Popen[bytes]) -> None:
    """Kill the process and its process group (`uv run pytest` starts children) and clean up."""
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        proc.kill()
    proc.wait()
    if proc.stdout is not None:
        proc.stdout.close()


def _run_capped(argv: list[str], cwd: str, timeout: int) -> tuple[int, str] | str:
    """Run `argv` (no shell) with a time limit and an output limit.

    Stdout and stderr go through one pipe, which we read in chunks: `cat /dev/zero`
    must not fill up the host. Returns `(returncode, output)`, or a reason
    (`not measurable`) on too much output; `TimeoutExpired` when the deadline
    passes.
    """
    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=dict(os.environ, **_SUB_ENV),
        start_new_session=True,
    )
    assert proc.stdout is not None
    deadline = time.monotonic() + timeout
    buf = bytearray()
    try:
        fd = proc.stdout.fileno()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout)
            if not select.select([fd], [], [], remaining)[0]:
                continue
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            buf += chunk
            if len(buf) > MAX_OUTPUT:
                return f"output larger than {MAX_OUTPUT} bytes"
        proc.wait(timeout=max(deadline - time.monotonic(), 0.001))
    finally:
        _stop(proc)
    return proc.returncode, buf.decode("utf-8", errors="replace")


def _run_or_reason(argv: list[str], cwd: str, timeout: int) -> tuple[int, str] | str:
    """`_run_capped`, but every reason for not being able to measure comes back as text."""
    try:
        return _run_capped(argv, cwd, timeout)
    except FileNotFoundError as error:
        return f"not found: {error.filename or argv[0]}"
    except subprocess.TimeoutExpired:
        return f"timeout after {timeout} s"
    except OSError as error:
        return f"not executable: {error}"


def _row_argv(row: Row) -> list[str] | str:
    """The argv of a valid row, or the reason why the row is `invalid`.

    One check for before and after the PR: a row that entry C calls `invalid`
    (outside the allowlist, or an empty expectation such as `na PR: `) must not
    pass as `after PR, not measured` under `--before-pr`.
    """
    argv = split_command(row.command)
    if isinstance(argv, str):
        return argv
    reason = check(argv)
    if reason is None and not expected(row.result)[0]:
        reason = "empty Result cell"
    return argv if reason is None else reason


def run_row(row: Row, cwd: str, gh_ok: bool, timeout: int = 300) -> tuple[str, str]:
    """Run one Evidence row; `(outcome, detail)`.

    The outcome is `HOLDS`, `FAILS`, `INVALID` (outside the allowlist, does not
    run) or `UNMEASURABLE` (binary missing, timeout, `gh` unreachable): a key
    of `headings.toml`.
    """
    argv = _row_argv(row)
    if isinstance(argv, str):
        return INVALID, f"Evidence outside the allowlist: {argv}"
    fragment, _ = expected(row.result)
    if argv[0] == "gh" and not gh_ok:
        return UNMEASURABLE, "gh not reachable"
    outcome_read = _run_or_reason(argv, cwd, timeout)
    if isinstance(outcome_read, str):
        return UNMEASURABLE, outcome_read
    returncode, output = outcome_read
    parts: list[str] = []
    if returncode != 0:
        parts.append(f"exit code {returncode}")
    if fragment not in output:
        parts.append(f"missing from output: {fragment!r}")
    if parts:
        return FAILS, "; ".join(parts)
    return HOLDS, f"exit code 0, `{fragment}` found"


def _gh_reachable() -> bool:
    try:
        proc = subprocess.run(
            ["gh", "api", "rate_limit", "--jq", ".rate.remaining"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=GH_PROBE_TIMEOUT_S,
            env=dict(os.environ, **_SUB_ENV),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def _runs_gh(row: Row, before_pr: bool) -> bool:
    """Does this row count for the `gh` probe: valid, a `gh` row and not skipped."""
    argv = _row_argv(row)
    if isinstance(argv, str) or argv[0] != "gh":
        return False
    return not (before_pr and expected(row.result)[1])


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        with open(args.body_file, encoding="utf-8") as f:
            body = f.read()
    except OSError as error:
        print(f"COULD NOT CHECK: body file unreadable ({error})")
        return 3
    if not os.path.isdir(args.cwd):
        print(f"COULD NOT CHECK: --cwd {args.cwd} is not a directory")
        return 3
    language = config.load(config.config_path()).language
    rows = evidence_rows(body, args.issue)
    if isinstance(rows, str):
        print(f"{headings.heading('evidence_closes_without_block', language)} {rows}")
        return 2
    gh_ok = any(_runs_gh(r, args.before_pr) for r in rows) and _gh_reachable()
    results: list[tuple[Row, str, str]] = []
    for row in rows:
        if args.before_pr and expected(row.result)[1]:
            argv = _row_argv(row)
            if isinstance(argv, str):
                results.append(
                    (row, INVALID, f"Evidence outside the allowlist: {argv}")
                )
            else:
                results.append((row, AFTER_PR_UNMEASURED, "runs only after the PR"))
            continue
        outcome, detail = run_row(row, args.cwd, gh_ok)
        results.append((row, outcome, detail))
    print(headings.heading("evidence_table_head", language))
    print("|---|---|---|")
    for row, outcome, detail in results:
        text = headings.heading(outcome, language)
        print(f"| {_cell(row.ac)} | {text} | {_cell(detail)} |")
    outcomes = {u for _, u, _ in results}
    if outcomes & {FAILS, INVALID}:
        return 1
    if UNMEASURABLE in outcomes:
        return 4
    return 0


def _cmd_check(args: argparse.Namespace) -> int:
    argv = split_command(args.cell)
    reason = argv if isinstance(argv, str) else check(argv)
    if reason is None:
        return 0
    print(reason)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_check = sub.add_parser("check", help="check one Command cell against the allowlist")
    p_check.add_argument("cell")
    p_check.set_defaults(func=_cmd_check)
    p_run = sub.add_parser("run", help="run the Evidence rows of a PR body")
    p_run.add_argument("--issue", type=int, required=True)
    p_run.add_argument("--body-file", required=True)
    p_run.add_argument("--cwd", required=True)
    p_run.add_argument("--before-pr", action="store_true")
    p_run.set_defaults(func=_cmd_run)
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except config.ConfigError as exc:
        print(f"COULD NOT CHECK: {exc}")
        return 3


if __name__ == "__main__":
    sys.exit(main())

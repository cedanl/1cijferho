#!/usr/bin/env python3
# lighthouse: python@v0.8.1
"""Quality ratchet — counts ruff rules and complexity, and judges diff coverage.

Usage:

    python3 .lighthouse/scripts/quality/ratchet.py ruff
    python3 .lighthouse/scripts/quality/ratchet.py show
    python3 .lighthouse/scripts/quality/ratchet.py tighten
    python3 .lighthouse/scripts/quality/ratchet.py coverage [--base REF] [--report coverage.json]
    python3 .lighthouse/scripts/quality/ratchet.py nightly --report coverage.json [--since SHA] [--min-diff-pct 90] [--dry-run]

The directories come from lighthouse.toml: python.lint_dirs (ruff, complexipy),
python.source_dirs (coverage) and python.coverage_command (the suite). The base of
`coverage` defaults to origin/<forge.default_branch>.

Messages are English whatever `[project] language` says: script output stays
literal English (decision 7 of the M0 package spec).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import ModuleType

# scripts/ is not an installed package. Put it on sys.path ourselves, so the
# invocation form above works without an outside PYTHONPATH.
_SCRIPTS = Path(__file__).resolve().parents[1]
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
# `.lighthouse/lib` next to `.lighthouse/scripts` (design choice 6 of (b)).
_LIB = Path(__file__).resolve().parents[2] / "lib"
if _LIB.is_dir() and str(_LIB) not in sys.path:
    sys.path.insert(0, str(_LIB))

from lighthouse_lib import config

from quality import diffcov
from quality.baseline import (
    COMPLEXIPY_KEY,
    COUNTED_RULES,
    ZERO_RULES,
    Baseline,
    BaselineError,
    load,
)
from quality.gitenv import suite_env

ALL_RULES: tuple[str, ...] = ZERO_RULES + COUNTED_RULES

# The same signature as quality.prcov_issues.Gh, defined separately here instead
# of imported — cmd_nightly imports prcov_issues only inside itself,
# so the other subcommands do not load that module.
Gh = Callable[[Sequence[str], Mapping[str, str] | None, str | None], str]

# How many locations go in the message per rule. Printing all 64 C901 locations
# makes the message unreadable; the first handful shows the way.
MAX_LOCATIONS = 20

BASELINE_NAME = "quality-baseline.toml"

# The counter counts functions with a cognitive complexity strictly above this number.
COGNITIVE_COUNTER_THRESHOLD = 15

# The two hard limits per function: no baseline, no bump, not even
# with a noqa comment. A score equal to the limit is green, strictly above it red.
CYCLOMATIC_LIMIT = 20
COGNITIVE_LIMIT = 25
# The marker in the error line of a hard limit; `_failure_footer` recognises it.
HARD_LIMIT_MARK = "hard limit, no bump possible"

# ruff carries the cyclomatic score in the message of every C901 finding:
# `f` is too complex (22 > 10). ruff only reports functions above the threshold, so
# the ratchet pins that at 10 (`--config`, see _ruff_findings): a higher
# max-complexity in pyproject.toml would otherwise hide scores 21 and higher up to that threshold
# from the hard limit, and make the C901 counter count something different.
C901_THRESHOLD = 10
C901_SCORE = re.compile(r"is too complex \((\d+) > \d+\)")
C901_NAME = re.compile(r"`([^`]+)`")


def repo_root() -> Path:
    """LIGHTHOUSE_REPO_ROOT, otherwise `git rev-parse --show-toplevel`."""
    try:
        return config.repo_root()
    except config.ConfigError as exc:
        raise SystemExit(f"ratchet: {exc}") from exc


def setting(key: str) -> object:
    """A key from lighthouse.toml, with the default of the config reader."""
    try:
        return config.lookup(config.load(config.config_path()), key)
    except config.ConfigError as exc:
        raise SystemExit(f"ratchet: {exc}") from exc
    except KeyError as exc:
        raise SystemExit(f"ratchet: {key} is not set in lighthouse.toml") from exc


def _dirs(key: str) -> list[str]:
    """The directories under `key`; an empty list stops the ratchet. Without paths
    ruff, complexipy and `git diff --` would take the whole repo and pytest would get no `--cov`."""
    value = setting(key)
    assert isinstance(value, list)  # the config reader checks the type
    if not value:
        raise SystemExit(f"ratchet: {key} is empty; the ratchet needs at least one directory")
    return [str(item) for item in value]


def _read_pin(root: Path, name: str) -> str:
    """Read a version pin from a file of its own in the repo root.

    A file of its own and not a variable in three scripts: one place means nothing
    can drift apart. The hook ruff-format-check.sh reads the same
    file; CI reads it transitively via this module.

    Unpinned was fine as long as ruff only autofixed. As soon as the same ruff counts,
    a release shifts the numbers and turns CI red on a PR that changed nothing
    — the same problem the workflow audit solved by pinning zizmor,
    actionlint and shellcheck.
    """
    try:
        pin = (root / name).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit(f"ratchet: {name} is unreadable: {exc}") from exc
    if not re.fullmatch(r"\d+\.\d+\.\d+", pin):
        raise SystemExit(f"ratchet: {name} holds no version: {pin!r}")
    return pin


def ruff_pin(root: Path) -> str:
    """The one source of truth for the ruff version: .ruff-version."""
    return _read_pin(root, ".ruff-version")


def complexipy_pin(root: Path) -> str:
    """The one source of truth for the complexipy version: .complexipy-version."""
    return _read_pin(root, ".complexipy-version")


def _ruff_findings(root: Path) -> list[dict]:
    """The ruff findings (JSON) over `python.lint_dirs`, or SystemExit if ruff does not run.

    --ignore-noqa is not optional: without that flag a single
    `# noqa: F401` suppresses the counter and the gate can be bypassed with one comment.

    No --isolated: the PT011 setting from pyproject.toml must take part. But
    an explicit --select, so the counted set stays exactly these fifteen,
    even if extend-select grows later. For the same reason the
    mccabe threshold is fixed via --config: what C901 counts, and which scores the hard
    cyclomatic limit sees, then does not depend on pyproject.toml.
    """
    version = ruff_pin(root)
    cmd = [
        "uvx",
        f"ruff@{version}",
        "check",
        "--ignore-noqa",
        "--output-format",
        "json",
        "--select",
        ",".join(ALL_RULES),
        "--config",
        f"lint.mccabe.max-complexity={C901_THRESHOLD}",
        *_dirs("python.lint_dirs"),
    ]
    proc = subprocess.run(cmd, cwd=root, capture_output=True, text=True, check=False)
    # ruff gives 0 (clean) or 1 (findings); everything else is "the tool could
    # not run" and that is red, not green. Fail closed.
    if proc.returncode not in (0, 1):
        raise SystemExit(
            f"ratchet: ruff@{version} could not run (rc={proc.returncode}) — the pin "
            f"does not resolve, or a directory in python.lint_dirs does not exist.\n{proc.stderr}"
        )
    try:
        return json.loads(proc.stdout or "[]")
    except ValueError as exc:
        raise SystemExit(
            f"ratchet: ruff output is unreadable: {exc}\n{proc.stderr}"
        ) from exc


def _relative_name(finding: dict, root: Path) -> str:
    filename = finding.get("filename", "?")
    try:
        return str(Path(filename).relative_to(root))
    except ValueError:
        return filename


def _c901_score(finding: dict, location: str) -> tuple[str, int]:
    """(`location function`, score) from the message of one C901 finding.

    A message without a readable score is red: after a ruff bump a silent
    zero would switch off the hard cyclomatic limit (fail closed).
    """
    message = finding.get("message", "")
    match = C901_SCORE.search(message)
    if match is None:
        raise SystemExit(
            f"ratchet: the C901 score is unreadable at {location}: {message!r} — "
            "ruff changed its message format; adjust C901_SCORE."
        )
    name = C901_NAME.search(message)
    return f"{location} {name.group(1) if name else '?'}", int(match.group(1))


def count_ruff(
    root: Path,
) -> tuple[dict[str, int], dict[str, list[str]], list[tuple[str, int]]]:
    """({code: count}, {code: [locations]}, [(location function, C901 score)]).

    The third element carries the cyclomatic score per function, for the hard
    limit in `judge_complexity`; the first two feed the counters.
    """
    counts = dict.fromkeys(ALL_RULES, 0)
    where: dict[str, list[str]] = {code: [] for code in ALL_RULES}
    cyclomatic: list[tuple[str, int]] = []
    invalid_syntax_files: list[str] = []
    for finding in _ruff_findings(root):
        code = finding.get("code")
        filename = _relative_name(finding, root)
        if code == "invalid-syntax":
            # ruff could not parse this file: every real hit in it stays
            # uncounted and the arm would silently report green — fail closed
            # instead of "the baseline can go down".
            invalid_syntax_files.append(filename)
            continue
        if code not in counts:
            continue
        counts[code] += 1
        position = finding.get("location") or {}
        location = f"{filename}:{position.get('row')}:{position.get('column')}"
        where[code].append(location)
        if code == "C901":
            cyclomatic.append(_c901_score(finding, location))
    if invalid_syntax_files:
        names = "\n".join(f"  {name}" for name in sorted(set(invalid_syntax_files)))
        raise SystemExit(
            "ratchet: ruff could not parse these files (invalid-syntax) — the "
            "count in them is unreliable, so red instead of silently green:\n"
            f"{names}"
        )
    return counts, where, cyclomatic


def _failed_files(output: str) -> list[str]:
    """The paths from the `Failed to process <path>` lines of complexipy."""
    return sorted(set(re.findall(r"Failed to process (.+)", output)))


def _complexipy_entry(entry: object, version: str) -> tuple[str, int]:
    """(`path::function`, score) from one element, or SystemExit on a deviating shape."""
    if isinstance(entry, dict):
        path = entry.get("path")
        name = entry.get("function_name")
        score = entry.get("complexity")
        # type() and not isinstance(): bool inherits from int.
        if isinstance(path, str) and isinstance(name, str) and type(score) is int:
            return f"{path}::{name}", score
    raise SystemExit(
        f"ratchet: complexipy@{version} gave an unreadable element: {entry!r}"
    )


def _complexipy_scores(output: Path, version: str) -> list[tuple[str, int]]:
    """(`path::function`, score) per function from the JSON file of complexipy.

    An element without a path, name or integer score is red: a silently skipped
    function would keep the counter artificially low. Duplicate names stay;
    the counter counts lines, not unique names.
    """
    try:
        data = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(
            f"ratchet: complexipy@{version} wrote no readable output: {exc}"
        ) from exc
    if not isinstance(data, list):
        raise SystemExit(f"ratchet: complexipy@{version} gave no list: {data!r:.200}")
    return [_complexipy_entry(entry, version) for entry in data]


def count_complexipy(root: Path) -> list[tuple[str, int]]:
    """Per function (`path::function`, cognitive score) over `python.lint_dirs`.

    --no-ignore is not optional, for the same reason as --ignore-noqa with ruff:
    a single `# noqa: complexipy` after a `def` would take the function out of the
    count. Output and cache go to a temporary directory, so the working tree
    stays clean and an old cache cannot influence the count.

    The threshold is set at a million: complexipy gives rc 1 for a function above
    that threshold and for a file it could not process, and only the
    second may turn red here. That second would silently count fewer functions, so
    it is fail closed and not "the baseline can go down".

    The cwd is the temporary directory, not the repo: complexipy reads `[tool.complexipy]
    exclude` from pyproject.toml, or a (.)complexipy.toml, from its cwd, and
    --no-ignore does not stop that. One config line would otherwise silently take files out of the
    counter and the hard limit. The directories go in absolute, the paths
    in the output come back relative; .gitignore still applies as usual.
    """
    version = complexipy_pin(root)
    with tempfile.TemporaryDirectory(prefix="ratchet-complexipy-") as tmp:
        output = Path(tmp) / "complexipy.json"
        cmd = [
            "uvx",
            f"complexipy@{version}",
            *(str(root / d) for d in _dirs("python.lint_dirs")),
            "--no-ignore",
            "--plain",
            "--max-complexity-allowed",
            "1000000",
            "--output",
            str(output),
            "--output-format",
            "json",
            "--cache-dir",
            str(Path(tmp) / "cache"),
        ]
        proc = subprocess.run(cmd, cwd=tmp, capture_output=True, text=True, check=False)
        failed = _failed_files(f"{proc.stdout}\n{proc.stderr}")
        if failed:
            names = "\n".join(f"  {name}" for name in failed)
            raise SystemExit(
                f"ratchet: complexipy@{version} could not process these files — "
                "the count in them is unreliable, so red instead of silently "
                f"green:\n{names}"
            )
        if proc.returncode != 0:
            raise SystemExit(
                f"ratchet: complexipy@{version} could not run "
                f"(rc={proc.returncode}).\n{proc.stderr}"
            )
        return [
            (_relative_to_root(name, root), score)
            for name, score in _complexipy_scores(output, version)
        ]


def _relative_to_root(name: str, root: Path) -> str:
    """`path::function` with the path relative to the repo, as with ruff."""
    path, sep, function = name.partition("::")
    try:
        path = str(Path(path).relative_to(root))
    except ValueError:
        pass
    return f"{path}{sep}{function}"


def _over_threshold(scores: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """The functions with a score strictly above COGNITIVE_COUNTER_THRESHOLD, highest first."""
    return _over_limit(scores, COGNITIVE_COUNTER_THRESHOLD)


def _over_limit(scores: list[tuple[str, int]], limit: int) -> list[tuple[str, int]]:
    """The functions with a score strictly above `limit`, highest first."""
    over = [item for item in scores if item[1] > limit]
    return sorted(over, key=lambda item: (-item[1], item[0]))


def _hard_limit_failures(
    axis: str, scores: list[tuple[str, int]], limit: int
) -> list[str]:
    """One error line per function above the hard limit, at most MAX_LOCATIONS."""
    over = _over_limit(scores, limit)
    lines = [
        f"{axis} {score} > {limit}: {name} — {HARD_LIMIT_MARK}"
        for name, score in over[:MAX_LOCATIONS]
    ]
    if len(over) > MAX_LOCATIONS:
        lines.append(f"… and {len(over) - MAX_LOCATIONS} more")
    return lines


def judge_complexity(
    baseline: Baseline,
    cyclomatic: list[tuple[str, int]],
    cognitive: list[tuple[str, int]],
) -> tuple[list[str], list[str]]:
    """(failures, notes) for the complexity. An empty failures list is green.

    Three verdicts: the two hard limits per function (cyclomatic 20,
    cognitive 25; no baseline, no bump) and the counter `cognitive_over_15`,
    which may not rise.
    """
    failures = _hard_limit_failures("cyclomatic", cyclomatic, CYCLOMATIC_LIMIT)
    failures += _hard_limit_failures("cognitive", cognitive, COGNITIVE_LIMIT)
    over = _over_threshold(cognitive)
    base = baseline.cognitive_over_15
    now = len(over)
    where = {COMPLEXIPY_KEY: [f"{name} (cognitive {score})" for name, score in over]}
    notes: list[str] = []
    if now > base:
        failures.append(
            f"{COMPLEXIPY_KEY}: {base} -> {now} (+{now - base})\n"
            + _locations(where, COMPLEXIPY_KEY)
        )
    elif now < base:
        notes.append(f"{COMPLEXIPY_KEY}: {base} -> {now}; the baseline can go down")
    return failures, notes


def _locations(where: dict[str, list[str]], rule: str) -> str:
    hits = where.get(rule, [])
    shown = "\n".join(f"    {hit}" for hit in hits[:MAX_LOCATIONS])
    if len(hits) > MAX_LOCATIONS:
        shown += f"\n    … and {len(hits) - MAX_LOCATIONS} more"
    return shown


def compare(
    baseline: Baseline, counts: dict[str, int], where: dict[str, list[str]]
) -> tuple[list[str], list[str]]:
    """(failures, notes). An empty failures list is green."""
    failures: list[str] = []
    notes: list[str] = []
    for rule in ZERO_RULES:
        if counts.get(rule):
            failures.append(
                f"{rule}: {counts[rule]} hit(s) on a zero rule — no bump possible\n"
                + _locations(where, rule)
            )
    for rule in COUNTED_RULES:
        base = baseline.counts[rule]
        now = counts.get(rule, 0)
        if now > base:
            failures.append(
                f"{rule}: {base} -> {now} (+{now - base})\n" + _locations(where, rule)
            )
        elif now < base:
            # Without this message the line never drops and the ratchet is
            # one-way traffic that rusts stuck at the opening level.
            notes.append(f"{rule}: {base} -> {now}; the baseline can go down")
    return failures, notes


def coverage_argv() -> list[str]:
    """The suite with coverage: python.coverage_command, with the --cov flags from
    python.source_dirs after it. No shell: the command is split with shlex."""
    command = setting("python.coverage_command")
    assert isinstance(command, str)
    return [
        *shlex.split(command),
        *(f"--cov={d}" for d in _dirs("python.source_dirs")),
        "--cov-report=json:coverage.json",
    ]


def _git(root: Path, args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise SystemExit(f"ratchet: `git {' '.join(args)}` failed:\n{proc.stderr}")
    return proc.stdout


def _merge_base(root: Path, base_ref: str) -> str:
    proc = subprocess.run(
        ["git", "merge-base", "HEAD", base_ref],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"ratchet: no merge base between HEAD and {base_ref}. "
            f"Fetch {base_ref} (git fetch) and try again.\n{proc.stderr}"
        )
    return proc.stdout.strip()


def judge_coverage(baseline: Baseline, result: diffcov.DiffCoverage) -> list[str]:
    """Empty list is green. The message names the uncovered lines, not just the %."""
    if result.total == 0:
        return []
    if result.pct >= baseline.min_diff_pct:
        return []
    lines = [
        (
            f"diff coverage {result.pct:.1f}% < {baseline.min_diff_pct}% "
            f"({result.covered}/{result.total} changed statements covered)"
        )
    ]
    for path, numbers in sorted(result.uncovered.items()):
        lines.append(f"  {path}: uncovered at {', '.join(str(n) for n in numbers)}")
    return lines


def _acquire_report(root: Path, report_arg: str | None) -> tuple[Path | None, int]:
    """(path, 0) on success; (None, rc) if the caller must stop at once with rc.

    Pulled out purely of cmd_coverage to get its complexity under the C901 limit
    — same two forms, same messages, no behaviour change:
    without --report the suite runs and yields a fresh report, with --report
    an existing report is read.
    """
    if report_arg is None:
        report = root / "coverage.json"
        # A coverage.json from an earlier, crashed run would give silent green
        # numbers on stale data. Remove it before the run.
        report.unlink(missing_ok=True)
        proc = subprocess.run(coverage_argv(), cwd=root, check=False, env=suite_env())
        if proc.returncode != 0:
            print(
                "ratchet: the suite fails — a green ratchet on a red suite is a lie.",
                file=sys.stderr,
            )
            return None, proc.returncode
        if not report.exists():
            print(
                "ratchet: the suite returned 0 but there is no coverage.json; the "
                "report was not written.",
                file=sys.stderr,
            )
            return None, 1
        return report, 0

    report = Path(report_arg)
    if not report.is_absolute():
        report = root / report
    if not report.exists():
        print(f"ratchet: {report} is missing", file=sys.stderr)
        return None, 1
    return report, 0


def judge_total(baseline: Baseline, total_pct: float) -> list[str]:
    """Empty list is green. The global src coverage may not drop below the floor.

    Unlike the diff threshold this is a pinned measurement: it stands at what
    the repo reached when the gate went on and creeps up as soon as new code lifts the
    coverage. The price is known and accepted: removing a large
    well-tested file lets the average drop and is then red.
    """
    if total_pct >= baseline.min_total_pct:
        return []
    return [
        f"total coverage {total_pct:.2f}% < {baseline.min_total_pct}% — "
        "the floor is a measured level, not a target; it may not drop"
    ]


def cmd_coverage(root: Path, base_ref: str, report_arg: str | None) -> int:
    baseline = load(root / BASELINE_NAME, root)
    merge_base = _merge_base(root, base_ref)
    added = diffcov.added_lines(
        _git(
            root,
            ["diff", "--unified=0", f"{merge_base}..HEAD", "--", *_dirs("python.source_dirs")],
        )
    )

    report, rc = _acquire_report(root, report_arg)
    if report is None:
        return rc

    try:
        data = json.loads(report.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"ratchet: {report} is unreadable: {exc}", file=sys.stderr)
        return 1

    try:
        result = diffcov.diff_coverage(
            added,
            data,
            read_source=lambda rel: (root / rel).read_text(encoding="utf-8"),
            exclude=baseline.exclude,
        )
    except diffcov.CoverageReportError as exc:
        print(f"ratchet: {exc}", file=sys.stderr)
        return 1

    total_pct = float(data.get("totals", {}).get("percent_covered", 0.0))
    failures = judge_coverage(baseline, result) + judge_total(baseline, total_pct)
    if failures:
        print(file=sys.stderr)
        for line in failures:
            print(f"ratchet: {line}", file=sys.stderr)
        print(
            "\nWrite a test that hits these lines. If the code really cannot run "
            "here, use `# pragma: no cover` with a reason, per block.",
            file=sys.stderr,
        )
        return 1
    print(f"ratchet: total coverage {total_pct:.2f}% >= {baseline.min_total_pct}%")
    if result.total == 0:
        print(
            "ratchet: no executable changed lines — coverage does not apply"
        )
        return 0
    print(
        f"ratchet: diff coverage {result.pct:.1f}% >= {baseline.min_diff_pct}% "
        f"({result.covered}/{result.total})"
    )
    return 0


def _run_url() -> str | None:
    """The permalink to this Actions run, or None outside Actions."""
    server = os.environ.get("GITHUB_SERVER_URL")
    repo = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    if server and repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return None


def _pr_report_line(pr: int, result: diffcov.DiffCoverage) -> str:
    if result.total == 0:
        return f"ratchet: PR #{pr} no executable source lines"
    return f"ratchet: PR #{pr} {result.pct:.1f}% ({result.covered}/{result.total})"


def _remediate_pr(
    prcov_issues: ModuleType,
    gh: Gh,
    pr: int,
    result: diffcov.DiffCoverage,
    threshold: int,
    run_url: str | None,
    dry_run: bool,
) -> bool:
    """Issue, handoff and board for one PR below the threshold — step 5.

    Prints its own lines. Returns True if this PR makes step 5 fail
    (rc 1 in the caller) — a missing or empty board token counts
    here: a green run would otherwise lift the watermark (`last_success_sha`),
    and then the next night never reconciles the board step again.
    Without `[forge.board]` there is no board step and so no token needed.

    An error from `ensure_issue`/`ensure_handoff` is thrown on to the
    caller, which reports it generically. Only the `ensure_on_board` error is
    caught locally here, so the message names the board token
    instead of only the raw gh error message.
    """
    if dry_run:
        print(f"ratchet: PR #{pr}: --dry-run, would update issue/handoff/board")
        return False
    action = prcov_issues.ensure_issue(gh, pr, result, threshold, run_url)
    if action is None:
        print(f"ratchet: PR #{pr}: already handled (closed issue)")
        return False
    _, issue_number, issue_url = action
    prcov_issues.ensure_handoff(gh, issue_number, pr, threshold)
    if prcov_issues.board() is None:
        print(f"ratchet: PR #{pr}: board: skipped — no [forge.board] configured")
        return False
    token = os.environ.get(prcov_issues.BOARD_TOKEN_ENV, "")
    if not token:
        print(
            f"ratchet: PR #{pr}: {prcov_issues.BOARD_TOKEN_ENV} is missing or empty "
            "— no board call; the issue and the handoff are there",
            file=sys.stderr,
        )
        return True
    try:
        line = prcov_issues.ensure_on_board(gh, issue_number, issue_url, token)
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        OSError,
        ValueError,
        KeyError,
    ) as exc:
        print(
            f"ratchet: PR #{pr}: the board step failed "
            f"(may {prcov_issues.BOARD_TOKEN_ENV} not write?): {exc}",
            file=sys.stderr,
        )
        return True
    print(f"ratchet: PR #{pr}: {line}")
    return False


def _watermark(prcov_issues: ModuleType, gh: Gh) -> str | None:
    """The `headSha` of the last successful nightly, or `None` (then the last 24
    hours, with a message on stderr: a silent fallback hides a broken
    configuration)."""
    workflow = prcov_issues.nightly_workflow()
    if workflow is None:
        print(
            "nightly: no python.nightly_workflow; judging the merges of the last 24 hours",
            file=sys.stderr,
        )
        return None
    sha = prcov_issues.last_success_sha(gh, workflow)
    if sha is None:
        print(
            f"nightly: no successful run of {workflow} on the default branch; "
            "judging the merges of the last 24 hours",
            file=sys.stderr,
        )
    return sha


def cmd_nightly(
    root: Path,
    report: str,
    since: str | None,
    min_diff_pct: int | None,
    dry_run: bool,
    gh: Gh | None = None,
) -> int:
    """Nightly arm: diff coverage per PR since the watermark, plus the global
    coverage floor on the same report.

    The watermark is the last successful run of `python.nightly_workflow`. Without
    that key, or without a successful run, the nightly judges the merges of the
    last 24 hours — and says so. `prcov`/`prcov_issues` are imported here, not
    at the top of the module: the other subcommands do not need them.
    """
    from quality import prcov, prcov_issues

    if gh is None:
        gh = prcov_issues.run_gh

    baseline = load(root / BASELINE_NAME, root)
    threshold = baseline.min_diff_pct if min_diff_pct is None else min_diff_pct

    report_path = Path(report)
    if not report_path.is_absolute():
        report_path = root / report_path
    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"ratchet: {report_path} is unreadable: {exc}", file=sys.stderr)
        return 1

    if since is not None:
        watermark = since
    else:
        try:
            watermark = _watermark(prcov_issues, gh)
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
            ValueError,
            KeyError,
            config.ConfigError,
        ) as exc:
            print(f"ratchet: the watermark cannot be determined: {exc}", file=sys.stderr)
            return 1
    try:
        commits = prcov.select_commits(root, watermark)
        attributed = prcov.attribute(root, commits, _dirs("python.source_dirs"))
    except prcov.GitCommandError as exc:
        print(f"ratchet: {exc}", file=sys.stderr)
        return 1

    def read_source(rel: str) -> str:
        return (root / rel).read_text(encoding="utf-8")

    run_url = _run_url()

    step5_failed = False
    for pr, paths in sorted(attributed.items()):
        # Per PR, not all at once via `prcov.per_pr`: that builds all PRs in
        # one dict comprehension, so a single missing report entry
        # (`CoverageReportError`) would crash the whole night — no
        # issues for the other PRs, no floor check, watermark stays
        # stuck.
        try:
            result = diffcov.diff_coverage(paths, data, read_source, baseline.exclude)
        except diffcov.CoverageReportError as exc:
            print(f"ratchet: PR #{pr}: {exc}", file=sys.stderr)
            step5_failed = True
            continue
        print(_pr_report_line(pr, result))
        if result.total == 0 or result.pct >= threshold:
            continue
        try:
            failed = _remediate_pr(
                prcov_issues, gh, pr, result, threshold, run_url, dry_run
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
            ValueError,
            KeyError,
            config.ConfigError,
        ) as exc:
            print(f"ratchet: PR #{pr}: {exc}", file=sys.stderr)
            step5_failed = True
            continue
        if failed:
            step5_failed = True

    total_pct = float(data.get("totals", {}).get("percent_covered", 0.0))
    floor_failures = judge_total(baseline, total_pct)
    for line in floor_failures:
        print(f"ratchet: {line}", file=sys.stderr)

    return 1 if step5_failed or floor_failures else 0


def _fit_width(measured: int, width: int) -> str:
    """The new number gets the width of the old one, so a comment
    after it stays in its column."""
    return f"{measured:<{width}}" if len(str(measured)) < width else str(measured)


def _lower_cognitive_counter(lines: list[str], measured: int) -> list[str]:
    """Lower `cognitive_over_15 = N` in `lines` (in place); never up."""
    for index, line in enumerate(lines):
        match = re.match(
            rf"^(?P<key>{COMPLEXIPY_KEY})(?P<pad>\s*=\s*)(?P<value>\d+)", line
        )
        if match is None:
            continue
        current = int(match.group("value"))
        if measured >= current:
            return []
        start, end = match.span("value")
        replacement = _fit_width(measured, len(match.group("value")))
        lines[index] = line[:start] + replacement + line[end:]
        return [f"{COMPLEXIPY_KEY}: {current} -> {measured}"]
    return []


def rewrite_baseline(
    text: str,
    counts: Mapping[str, int],
    total_pct: float | None = None,
    cognitive_over_15: int | None = None,
) -> tuple[str, list[str]]:
    """(new text, ["RULE: old -> new"]). Only lowers, never up.

    Line by line with a regex and not via a TOML writer, because the comments
    after the numbers are the only memory of the FP sample. tomllib
    can only read, and bringing in a writer would throw those comments away.

    Writing upward is explicitly excluded. A rise should be red
    in the gate; if the lowerer wrote it away, the automation would silently
    commit a bump that nobody decided — exactly the bypass this
    design wants nowhere.
    """
    changes: list[str] = []
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        match = re.match(r"^(?P<rule>[A-Z]+[0-9]+)(?P<pad>\s*=\s*)(?P<value>\d+)", line)
        if match is None:
            continue
        rule = match.group("rule")
        if rule not in COUNTED_RULES or rule not in counts:
            continue
        current = int(match.group("value"))
        measured = counts[rule]
        if measured >= current:
            continue
        replacement = _fit_width(measured, len(match.group("value")))
        start, end = match.span("value")
        lines[index] = line[:start] + replacement + line[end:]
        changes.append(f"{rule}: {current} -> {measured}")

    if cognitive_over_15 is not None:
        changes += _lower_cognitive_counter(lines, cognitive_over_15)

    if total_pct is not None:
        # Truncate downward, do not round: 64.98 stays 64. Without that
        # damping the weekly job opens a PR for every tenth of a percent of noise.
        measured_floor = int(total_pct)
        for index, line in enumerate(lines):
            match = re.match(
                r"^(?P<key>min_total_pct)(?P<pad>\s*=\s*)(?P<value>\d+)", line
            )
            if match is None:
                continue
            current = int(match.group("value"))
            if measured_floor <= current:
                # Never down. A dip may not drag the floor along, otherwise it is
                # no longer a ratchet but a thermometer.
                break
            start, end = match.span("value")
            lines[index] = line[:start] + str(measured_floor) + line[end:]
            changes.append(f"min_total_pct: {current} -> {measured_floor}")
            break

    return "".join(lines), changes


def cmd_tighten(root: Path) -> int:
    """Set the baseline to the measured level: ruff down, the coverage floor up.

    Writes the file; the PR around it is the work of the workflow. That way
    the lowering stays visible and reviewable in a diff — the ratchet never
    adjusts itself silently.
    """
    path = root / BASELINE_NAME
    baseline = load(path, root)
    counts, _, cyclomatic = count_ruff(root)
    cognitive_scores = count_complexipy(root)
    cognitive = len(_over_threshold(cognitive_scores))

    risen = [r for r in COUNTED_RULES if counts.get(r, 0) > baseline.counts[r]]
    if cognitive > baseline.cognitive_over_15:
        risen.append(COMPLEXIPY_KEY)
    zero_hits = [r for r in ZERO_RULES if counts.get(r)]
    hard_hits = len(_over_limit(cyclomatic, CYCLOMATIC_LIMIT)) + len(
        _over_limit(cognitive_scores, COGNITIVE_LIMIT)
    )
    if risen or zero_hits or hard_hits:
        # Fail closed: with a red gate the level is no "new line" but
        # a problem. Touching the file would polish the rise away.
        print(
            "ratchet: the gate is red (risen: "
            f"{risen or '-'}, zero-rule hits: {zero_hits or '-'}, "
            f"above a hard limit: {hard_hits}) — "
            "fix that first, then lower.",
            file=sys.stderr,
        )
        return 1

    # The global coverage comes from the same suite run the coverage arm also
    # uses. If the report is missing, only the ruff axis shifts; a
    # missing measurement may not touch the floor.
    total_pct: float | None = None
    report = root / "coverage.json"
    if not report.exists():
        proc = subprocess.run(coverage_argv(), cwd=root, check=False, env=suite_env())
        if proc.returncode != 0:
            print(
                "ratchet: the suite fails — the coverage floor stays as it is.",
                file=sys.stderr,
            )
            return proc.returncode
    if report.exists():
        try:
            total_pct = float(
                json.loads(report.read_text(encoding="utf-8"))["totals"][
                    "percent_covered"
                ]
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            print(f"ratchet: coverage.json is unusable: {exc}", file=sys.stderr)
            return 1

    text = path.read_text(encoding="utf-8")
    new_text, changes = rewrite_baseline(text, counts, total_pct, cognitive)
    if not changes:
        print("ratchet: nothing to tighten — the baseline is at the measurement.")
        return 0
    path.write_text(new_text, encoding="utf-8")
    print("ratchet: baseline tightened:")
    for change in changes:
        print(f"  {change}")
    return 0


def _report_failures(heading: str, failures: list[str]) -> None:
    if not failures:
        return
    print(file=sys.stderr)
    print(f"ratchet: {heading}", file=sys.stderr)
    for failure in failures:
        print(f"  {failure}", file=sys.stderr)


# A rising counter (`S110: 0 -> 1`, `cognitive_over_15: 107 -> 108`) is the
# only failure a bump can solve; a zero rule or hard limit is not.
BUMPABLE_FAILURE = re.compile(r"^\w+: \d+ -> \d+")
COMPLEXITY_SKILL = (
    ".claude/skills/lighthouse-python-standard/SKILL.md, section "
    "`## Reaching a complexity limit: deepen, don't split`"
)


def _failure_footer(failures: list[str]) -> str:
    """The closing line on red: bump advice only if a rising counter fails.

    A function above a hard limit must come down in the code; the bump advice
    would point the wrong way there.
    """
    parts: list[str] = []
    if any(BUMPABLE_FAILURE.match(failure) for failure in failures):
        parts.append(
            f"Fix it in the code, or adjust {BASELINE_NAME} in the same change "
            "if you accept the rise of a counter on purpose."
        )
    if any(HARD_LIMIT_MARK in failure for failure in failures):
        parts.append(
            "A function above a hard complexity limit must come down in the code; "
            f"there is no bump there. How: {COMPLEXITY_SKILL}."
        )
    if not parts:
        parts.append("Fix it in the code: a zero rule has no bump.")
    parts.append(
        "A `# noqa` does not help here: the ratchet counts with --ignore-noqa and --no-ignore."
    )
    return " ".join(parts)


def cmd_ruff(root: Path) -> int:
    baseline = load(root / BASELINE_NAME, root)
    counts, where, cyclomatic = count_ruff(root)
    cognitive = count_complexipy(root)
    failures, notes = compare(baseline, counts, where)
    complexity_failures, complexity_notes = judge_complexity(
        baseline, cyclomatic, cognitive
    )
    for note in notes + complexity_notes:
        print(f"ratchet: {note}")
    _report_failures("the ruff counters rise.", failures)
    _report_failures(
        "complexity above the limit or the counter rises.", complexity_failures
    )
    if failures or complexity_failures:
        print(file=sys.stderr)
        print(_failure_footer(failures + complexity_failures), file=sys.stderr)
        return 1
    total = sum(counts[rule] for rule in COUNTED_RULES)
    print(
        f"ratchet: ruff ok ({total} hits on the counters, no rise, zero rules clean); "
        f"complexipy ok ({len(_over_threshold(cognitive))} functions cognitive > "
        f"{COGNITIVE_COUNTER_THRESHOLD})"
    )
    return 0


def cmd_show(root: Path) -> int:
    counts, _, _ = count_ruff(root)
    print("[ruff]")
    for rule in COUNTED_RULES:
        print(f"{rule:<6} = {counts[rule]}")
    print()
    print("# zero rules — do NOT belong in the baseline, must be 0:")
    for rule in ZERO_RULES:
        print(f"# {rule:<6} = {counts[rule]}")
    print()
    print("[complexipy]")
    print(f"{COMPLEXIPY_KEY} = {len(_over_threshold(count_complexipy(root)))}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ratchet", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("ruff", help="count the fifteen rules and compare with the baseline")
    sub.add_parser("show", help="print the current counters without judging")
    sub.add_parser(
        "tighten",
        help="set quality-baseline.toml to the measured level (ruff down, coverage floor up)",
    )
    coverage = sub.add_parser("coverage", help="judge diff coverage")
    coverage.add_argument(
        "--base",
        default=None,
        help="ref the merge base is taken against (default: origin/<forge.default_branch>)",
    )
    coverage.add_argument(
        "--report",
        default=None,
        help="read an existing coverage.json instead of running the suite",
    )
    nightly = sub.add_parser(
        "nightly", help="judge diff coverage per PR since the watermark"
    )
    nightly.add_argument(
        "--report", required=True, help="coverage.json of the full suite"
    )
    nightly.add_argument(
        "--since",
        default=None,
        help="watermark SHA; by default the last successful nightly run",
    )
    nightly.add_argument(
        "--min-diff-pct",
        type=int,
        default=None,
        help="threshold override in percent (default: baseline.min_diff_pct)",
    )
    nightly.add_argument(
        "--dry-run",
        action="store_true",
        help="only report what would happen, no gh mutations",
    )
    args = parser.parse_args(argv)

    root = repo_root()
    try:
        if args.command == "ruff":
            return cmd_ruff(root)
        if args.command == "tighten":
            return cmd_tighten(root)
        if args.command == "coverage":
            base = args.base or f"origin/{setting('forge.default_branch')}"
            return cmd_coverage(root, base, args.report)
        if args.command == "nightly":
            return cmd_nightly(
                root, args.report, args.since, args.min_diff_pct, args.dry_run
            )
        return cmd_show(root)
    except BaselineError as exc:
        print(f"ratchet: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

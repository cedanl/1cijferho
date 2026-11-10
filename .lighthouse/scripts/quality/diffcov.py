# lighthouse: python@v0.8.1
"""Shared calculation for diff coverage: one implementation, two callers.

Used by `ratchet.py coverage` (threshold from quality-baseline.toml) and by
the nightly via `prcov.py`. What is shared is the calculation, not the verdict:
only `.py` files, a missing report line is an error instead of
"everything missed", and continuation lines are mapped back to their statement.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

_HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(?P<start>\d+)(?:,\d+)? @@")

# Fields in which a compound statement carries its own sub-statements.
# They determine where the header of that statement ends.
_BODY_FIELDS = ("body", "orelse", "finalbody", "handlers", "cases")

# ExceptHandler and match_case are not an ast.stmt but do have their own
# line number that coverage.py reports as a statement; without them a
# changed `except X:` or `case ...:` line falls back to the enclosing header
# (`try`/`match`) and a never-executed `case` arm counts as covered as soon as
# another arm runs.
_HEADER_NODES = (ast.stmt, ast.ExceptHandler, ast.match_case)


class CoverageReportError(RuntimeError):
    """The report and the diff do not match. Fail closed: red."""


@dataclass(frozen=True)
class DiffCoverage:
    total: int
    uncovered: dict[str, list[int]]

    @property
    def covered(self) -> int:
        return self.total - sum(len(lines) for lines in self.uncovered.values())

    @property
    def pct(self) -> float:
        """100.0 for an empty denominator — no division by zero, and nothing to judge."""
        return 100.0 if self.total == 0 else 100.0 * self.covered / self.total


def added_lines(diff: str) -> dict[str, list[int]]:
    """{path: [line numbers in the new file]} from a `--unified=0` diff."""
    added: dict[str, list[int]] = {}
    path = ""
    lineno = 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            raw = line[4:].strip()
            path = raw.removeprefix("b/")
            continue
        hunk = _HUNK_RE.match(line)
        if hunk:
            lineno = int(hunk.group("start"))
            continue
        if line.startswith("+") and not line.startswith("+++"):
            if path and path != "/dev/null":
                added.setdefault(path, []).append(lineno)
            lineno += 1
        elif line.startswith("-") or line.startswith("\\"):
            continue  # removed line / "\ No newline" — no new line number
        elif line.startswith(" "):
            lineno += 1  # context line (absent with --unified=0, to be safe)
    return added


def _lineno(node: ast.AST) -> int | None:
    """Line number of the header of this node.

    match_case carries no `lineno` itself (no `_attributes` line in the
    grammar); its `pattern` — the `case ...` expression — does.
    """
    if isinstance(node, ast.match_case):
        return node.pattern.lineno
    return getattr(node, "lineno", None)


def _spans(tree: ast.AST) -> list[tuple[int, int]]:
    """Per statement, the lines that really belong to it.

    For a compound statement (if/for/def/try/match/case/…) only
    the header counts: its body consists of its own statements with their own line numbers.
    Without that restriction a blank line in the middle of a function would be mapped back to the
    `def` line and wrongly pull it into the denominator.
    """
    spans: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, _HEADER_NODES):
            continue
        start = _lineno(node)
        if start is None:
            continue
        starts = [
            line
            for field in _BODY_FIELDS
            for child in getattr(node, field, None) or []
            if (line := _lineno(child)) is not None
        ]
        end = (
            min(starts) - 1 if starts else (getattr(node, "end_lineno", None) or start)
        )
        if end >= start:
            spans.append((start, end))
    return spans


def statement_lines(source: str, lines: Iterable[int]) -> set[int]:
    """Map changed line numbers back to the line where their statement begins.

    Coverage.py reports per *statement*, and that is the line where the
    statement begins. A change inside a multiline call otherwise lands on
    a number that is in neither set and would silently drop out of the denominator
    — uncovered code that passes the gate. That is the wrong direction to
    fail in.

    Several changed lines within the same statement yield one number,
    not three. Lines that belong to no statement (comments, blank lines,
    lines between statements) drop out — those you cannot cover.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise CoverageReportError(f"source could not be parsed: {exc}") from exc
    spans = _spans(tree)
    mapped: set[int] = set()
    for number in lines:
        best: tuple[int, int] | None = None
        for start, end in spans:
            if start <= number <= end and (
                best is None or (end - start) < (best[1] - best[0])
            ):
                best = (start, end)
        if best is not None:
            mapped.add(best[0])
    return mapped


def diff_coverage(
    added: Mapping[str, Sequence[int]],
    report: Mapping[str, Any],
    read_source: Callable[[str], str],
    exclude: Sequence[str] = (),
) -> DiffCoverage:
    """Lay the changed lines against `coverage.json` and count.

    `read_source` is injectable, so a caller can read its files from somewhere
    other than disk.

    A changed `.py` file that is not in the report is an **error**:
    `--cov=src` names every Python module under `src/`, even a brand-new one that
    no test imports (verified on src/web/pages/nexus_page.py,
    which is in the report at 0%). If it is missing anyway, report and
    diff do not come from the same run and the gate must not assume "so everything missed".
    """
    files = report.get("files", {})
    total = 0
    uncovered: dict[str, list[int]] = {}
    for path, lines in sorted(added.items()):
        # The gate makes no statement about non-Python: there are 13 tracked
        # non-Python files under src/ (the browser extension, a Jinja template,
        # three CLAUDE.md) and by definition they cannot be in the report.
        if not path.endswith(".py"):
            continue
        if any(path.startswith(prefix) for prefix in exclude):
            continue
        entry = files.get(path)
        if entry is None:
            raise CoverageReportError(
                f"no report entry for {path} — report and diff are not from the "
                "same run; not assuming everything was missed"
            )
        executed = set(entry.get("executed_lines", []))
        missing = set(entry.get("missing_lines", []))
        statements = statement_lines(read_source(path), lines)
        # What is in neither set is not executable (blank line,
        # comment, `# pragma: no cover`) and drops out of the denominator.
        relevant = sorted(s for s in statements if s in executed or s in missing)
        total += len(relevant)
        missed = [s for s in relevant if s in missing]
        if missed:
            uncovered[path] = missed
    return DiffCoverage(total=total, uncovered=uncovered)

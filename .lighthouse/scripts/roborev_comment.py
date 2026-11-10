# lighthouse: core@v0.8.1
"""The PR comment of the roborev review: marker, log block, table and raw output (#119).

    roborev_comment.py render --pr <nr> --bron lokaal|ci --state <path> --raw <path>
        Prints the comment to stdout. Exit 2 when the state file is not a `done`
        state (a skip has nothing to report), or on wrong usage.

Why this shape: `write-pr` (source `lokaal`) and `fix-pr` (source `ci`) post the same
comment, so one renderer keeps them identical. The first line is the marker that tells
`fix-pr` the review is processed. The second line is a machine-readable log of every
finding and its outcome; a later session reads it back instead of parsing the table.

The log sits inside an HTML comment, so its JSON must never contain `-->` or a raw
`<`: those three characters (`<`, `>`, `-`) are written as `\\u003c`, `\\u003e` and
`\\u002d`, which `json.loads` turns back. `json.dumps` already escapes newlines, so the
block stays one line however hostile a reason is.

GitHub refuses a comment over 65536 characters. Only the raw output is cut to fit:
the marker, log and table are what a reader and a later session need.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

LIMIT = 65536
TRUNCATED = "… ingekort tot de limiet van GitHub"
_ESCAPES = (("<", "\\u003c"), (">", "\\u003e"), ("-", "\\u002d"))


def log_block(sha: str, source: str, pr: int, findings: list[dict]) -> str:
    """One line `<!-- roborev-log: <json> -->` that round-trips through json.loads."""
    payload = json.dumps(
        {"v": 1, "sha": sha, "bron": source, "pr": pr, "bevindingen": findings},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    for char, escape in _ESCAPES:
        payload = payload.replace(char, escape)
    return f"<!-- roborev-log: {payload} -->"


def _clean(text: object) -> str:
    """One table cell: whitespace runs folded to a space, `|` escaped, so a row stays one line."""
    return " ".join(str(text).split()).replace("|", "\\|")


def _cell(finding: dict) -> str:
    path = finding.get("path")
    if not path:
        return "—"
    line = finding.get("line")
    return _clean(f"`{path}:{line}`" if line is not None else f"`{path}`")


def _outcome(finding: dict) -> str:
    kind = finding["uitkomst"]
    if kind == "fixed":
        return _clean(f"fixed in {finding.get('commit')}")
    return _clean(f"{kind}: {finding.get('reden') or ''}")


def table(findings: list[dict]) -> str:
    """Markdown table, one row per finding; one explicit row when there are none."""
    rows = ["| Bevinding | Severity | Uitkomst |", "| --- | --- | --- |"]
    if not findings:
        rows.append("| geen bevindingen | — | — |")
    for finding in findings:
        rows.append(f"| {_cell(finding)} | {_clean(finding['severity'])} | {_outcome(finding)} |")
    return "\n".join(rows)


def render(state: dict, raw: str, pr: int, source: str, limit: int = LIMIT) -> str:
    """The whole comment; when too long, only the raw output is cut."""
    sha = state["sha"]
    findings = state["bevindingen"]
    head = (
        f"<!-- roborev-verwerkt: {sha} -->\n"
        f"{log_block(sha, source, pr, findings)}\n\n"
        f"{table(findings)}\n\n"
        "<details><summary>Ruwe roborev-output</summary>\n\n"
    )
    tail = "\n\n</details>"
    if len(head) + len(raw) + len(tail) <= limit:
        return head + raw + tail
    note = "\n" + TRUNCATED
    room = limit - len(head) - len(tail) - len(note)
    if room < 0:
        raise ValueError(
            f"log block and table alone are {len(head) + len(tail) + len(note)} characters, "
            f"over the limit of {limit}"
        )
    return head + raw[:room] + note + tail


def _run(argv: list[str]) -> str:
    parser = argparse.ArgumentParser(prog="roborev_comment.py")
    sub = parser.add_subparsers(dest="command", required=True)
    cmd = sub.add_parser("render")
    cmd.add_argument("--pr", type=int, required=True)
    cmd.add_argument("--bron", choices=("lokaal", "ci"), required=True)
    cmd.add_argument("--state", required=True)
    cmd.add_argument("--raw", required=True)
    args = parser.parse_args(argv)
    state = json.loads(pathlib.Path(args.state).read_text())
    if state.get("uitkomst") != "done":
        raise ValueError("state is not a done review: nothing to render")
    raw = pathlib.Path(args.raw).read_text()
    return render(state, raw, args.pr, args.bron)


def main(argv: list[str] | None = None) -> int:
    try:
        sys.stdout.write(_run(sys.argv[1:] if argv is None else argv) + "\n")
    except (ValueError, OSError) as exc:
        print(f"roborev_comment: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
# lighthouse: core@v0.8.1
"""KPI measurement for the skill lighthouse-fix-pr.

WHAT THIS MEASURES
------------------
The lead-in to a red or conflicting PR. Measured on 2026-08-29, after the fix of the
`git branch`/`git reset`/`git switch`/`git stash`/heredoc classification below:

    tool calls before the first mutating call   n=9    median 4    (min 2, max 8)
    read-only gh/git diagnostic calls before it n=10   median 3.0

`n` grows with the transcript directory: every session since the previous measurement counts, so a later
run of this command reads a higher `n`. The median itself is not fixed -- only the
regex classification above is pinned, the number is a measurement, not a target to tune
towards. The issue estimated 5-7. Target <= 2, which three of the nine turns already met.

WHY "MUTATING" AND NOT "Write/Edit"
-----------------------------------
In 6 of the 8 turns not a single Write/Edit went by: the fix work ran through `git rebase`,
`sed -i` and heredoc Python in Bash. A count on Write/Edit would write off 6 of the 8 cases here as
"no route" -- exactly the blind spot that `worktree-first` in
measure_start_issue_kpi.py names explicitly. That is why `leadin` counts up to the first call that changes
the tree, where a Bash command is called mutating by pattern.

WHAT THIS CANNOT DO
-------------------
The prompt recognition does not see the difference between an issue number and a PR number. "fix 596" counts
whether 596 was an issue or a PR; in the eight measured it was always a PR, but the metric cannot
guarantee that. Making it broader would make it incomparable with the baseline measurement.

The pattern MUTATING_BASH_RE is an approximation. A fix through a tool that is not in it counts
as "no mutation" and drops out of the denominator -- the side that never makes the score more favourable.

`mistrigger` counts one direction: the skill loaded without a fix request in the prompt. The reverse error
-- a fix request where the skill did NOT load -- becomes visible indirectly in `leadin`, because such a
turn keeps the old median.

Usage:
    python3 .lighthouse/scripts/measure_fix_pr_kpi.py            # the whole table
    python3 .lighthouse/scripts/measure_fix_pr_kpi.py leadin --since 2026-08-30

Output keys are fixed English tokens.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# The transcript parsing layer already lives in measure_start_issue_kpi.py and is tested there. Copying it
# here would yield two definitions of "a turn" that can silently drift apart. It
# is loaded as a neighbour in its own directory, not via a repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import measure_start_issue_kpi as tx  # noqa: E402

SKILL_NAME = "lighthouse-fix-pr"

# The five measured shapes: `fix pr 585`, `fix 595`, `627 pr heeft errors`,
# `612 heeft conflicts, fix die`, `kun je conflict 408 goed oplossen?`.
# Deliberately narrow: a prompt that only MENTIONS a PR ("pr 639 is gemerged") is not a fix request.
FIXPR_RE = re.compile(
    r"""
      (?:fix|repareer|debug|herstel)\s+(?:de\s+)?(?:pr\s*)?\#?(?P<a>\d{2,4})\b
    | \#?(?P<b>\d{2,4})\s+(?:pr\s+)?(?:heeft|is)\s+(?:een\s+)?
      (?:conflict|conflicts|conflictje|error|errors|fout|rood)
    | conflict(?:en|je)?\s+(?:van\s+|in\s+|op\s+)?\#?(?P<c>\d{2,4})
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Everything that changes the worktree, the branch or the PR. `gh pr view/checks/diff` is deliberately
# NOT in it; that is the diagnosis this skill precisely wants to summarise.
#
# `git branch` is here only with a delete/rename/copy flag (-d/-D/-m/-M/-c/-C and their long
# form). The bare creation form `git branch <name>` is ambiguous against read forms like
# `git branch --merged main` -- without a flag a word-boundary match cannot tell whether
# "main" is a target branch or a filter argument. That form therefore stays with DIAGNOSTIC_BASH_RE
# (like the bare `git branch`, `-a`, `-r`, `-v`, `--list`, `--show-current`), which makes the
# diagnostic counter slightly too favourable for that one form -- the side the issue allows, see the
# note at the bottom of the module docstring.
#
# The heredoc branch (`<<(?!<) ... `) matches `<<EOF`, `<<-EOF`, `<<'PATCH'` and `<<"SCRIPT"` -- an
# optional `-` indent, an optional quote, then a delimiter that starts with a capital letter or
# underscore. That last requirement is explicitly in a `(?-i:...)` group: the rest of this
# pattern is under `re.IGNORECASE` (for `git`/`gh`/`sed`), but without that local switch
# a heredoc with a lower-case delimiter would match too, and worse, `echo $((x << y))`
# (bit shift on lower-case variables) would then count as a mutation. `(?!<)` also excludes the
# here-string `<<<` (`cat <<< 'tekst'`) -- that changes nothing, and without that lookahead a
# capital-letter quote after it (`<<< 'HELLO'`) would still match.
MUTATING_BASH_RE = re.compile(
    r"git\s+(rebase|merge|cherry-pick|add|commit|push|checkout|restore|worktree\s+add"
    r"|reset|switch)\b"
    r"|git\s+branch\s+(?:\S+\s+)*(?:-[dDmMcC]|--delete|--move|--copy)\b"
    r"|git\s+stash\s+(?:pop|apply)\b"
    r"|sed\s+-i"
    r"|<<(?!<)-?\s*(?-i:['\"]?[A-Z_][A-Z0-9_]*['\"]?)"
    r"|gh\s+(run\s+rerun|pr\s+merge|pr\s+edit)\b",
    re.IGNORECASE,
)

DIAGNOSTIC_BASH_RE = re.compile(
    r"gh\s+pr\s+(view|checks|diff)\b"
    r"|gh\s+run\s+(view|list)\b"
    r"|git\s+stash\s+(?:list|show)\b"
    r"|git\s+(worktree\s+list|status|log|branch|diff|show|fetch|rev-parse|merge-tree)\b",
    re.IGNORECASE,
)

WRITE_TOOLS = frozenset({"Write", "Edit", "NotebookEdit"})

# The two actions that per AC 7 belong AFTER this skill, not in it. Either of them in the same turn
# means the skill ran on past "the PR is green".
SCOPE_SKILLS = frozenset({"lighthouse-close-issue", "caveman:caveman-review"})


def fix_pr_number(text: str) -> int | None:
    match = FIXPR_RE.search(text)
    if not match:
        return None
    return int(match.group("a") or match.group("b") or match.group("c"))


def is_mutating(name: str, payload: dict) -> bool:
    if name in WRITE_TOOLS:
        return True
    if name != "Bash":
        return False
    return bool(MUTATING_BASH_RE.search(str(payload.get("command", ""))))


def is_diagnostic(name: str, payload: dict) -> bool:
    if name != "Bash":
        return False
    command = str(payload.get("command", ""))
    if MUTATING_BASH_RE.search(command):
        return False
    return bool(DIAGNOSTIC_BASH_RE.search(command))


def count_leadin(turn: tx.Turn) -> int | None:
    """Tool calls before the first mutating call, or None if the turn never mutated.

    Exclusive: the mutating call itself is the fix, not the lead-in to it. The ambient
    `Skill{skill: lighthouse-fix-pr}` does not count -- that call IS the loading of the skill, and counting it
    would make the metric succeed by construction.
    """
    seen = 0
    for record in turn.records:
        for name, payload in tx.tool_uses(record):
            if name == "Skill" and payload.get("skill") == SKILL_NAME:
                continue
            if is_mutating(name, payload):
                return seen
            seen += 1
    return None


def count_diagnostics(turn: tx.Turn) -> int:
    """Read-only gh/git calls up to the first mutation (or to the end of the turn)."""
    count = 0
    for record in turn.records:
        for name, payload in tx.tool_uses(record):
            if is_mutating(name, payload):
                return count
            if is_diagnostic(name, payload):
                count += 1
    return count


def overruns_scope(turn: tx.Turn) -> bool:
    """Did the skill load and then an action that per AC 7 falls outside its boundary?"""
    loaded = tx.loaded_skills(turn)
    if SKILL_NAME not in loaded:
        return False
    return any(name in SCOPE_SKILLS for name in loaded)


def iter_fix_turns(directory: Path, since: str | None):
    for path in sorted(directory.glob("*.jsonl")):
        records = [r for r in tx.read_session(path) if tx._after(r, since)]
        for turn in tx.iter_turns(records):
            number = fix_pr_number(turn.prompt)
            if number is not None:
                yield turn, number


def measure_leadin(directory: Path, since: str | None) -> dict[str, object]:
    counts: list[int] = []
    without_mutation = 0
    for turn, _ in iter_fix_turns(directory, since):
        value = count_leadin(turn)
        if value is None:
            without_mutation += 1
        else:
            counts.append(value)
    return {**tx._summary(counts), "without_mutation": without_mutation}


def measure_diagnose(directory: Path, since: str | None) -> dict[str, object]:
    counts = [count_diagnostics(turn) for turn, _ in iter_fix_turns(directory, since)]
    return tx._summary(counts)


def measure_mistrigger(directory: Path, since: str | None) -> dict[str, object]:
    loaded = missed = 0
    examples: list[str] = []
    for path in sorted(directory.glob("*.jsonl")):
        records = [r for r in tx.read_session(path) if tx._after(r, since)]
        for turn in tx.iter_turns(records):
            if SKILL_NAME not in tx.loaded_skills(turn):
                continue
            loaded += 1
            if fix_pr_number(turn.prompt) is None:
                missed += 1
                examples.append(turn.prompt[:120])
    return {
        "loaded": loaded,
        "without_fix_request": missed,
        "examples": examples[:5],
    }


def measure_scope(directory: Path, since: str | None) -> dict[str, object]:
    loaded = outside = 0
    for path in sorted(directory.glob("*.jsonl")):
        records = [r for r in tx.read_session(path) if tx._after(r, since)]
        for turn in tx.iter_turns(records):
            if SKILL_NAME not in tx.loaded_skills(turn):
                continue
            loaded += 1
            if overruns_scope(turn):
                outside += 1
    return {"loaded": loaded, "past_the_boundary": outside}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Review measurement for the skill lighthouse-fix-pr, from the Claude Code transcripts."
    )
    parser.add_argument(
        "metric",
        nargs="?",
        choices=["leadin", "diagnose", "mistrigger", "scope"],
        help="leave out for the whole table",
    )
    parser.add_argument("--dir", type=Path, help="default: this repo's Claude Code projects dir")
    parser.add_argument("--since", help="YYYY-MM-DD; only records from this day on")
    args = parser.parse_args(argv)

    try:
        if args.dir is None:
            args.dir = tx.default_transcript_dir()
    except tx.ConfigError as exc:
        print(f"measure: {exc}", file=sys.stderr)
        return 2
    if not args.dir.is_dir():
        print(f"transcript directory does not exist: {args.dir}", file=sys.stderr)
        return 2

    rows = {
        "leadin": lambda: measure_leadin(args.dir, args.since),
        "diagnose": lambda: measure_diagnose(args.dir, args.since),
        "mistrigger": lambda: measure_mistrigger(args.dir, args.since),
        "scope": lambda: measure_scope(args.dir, args.since),
    }
    selected = [args.metric] if args.metric else list(rows)
    print(
        json.dumps(
            {name: rows[name]() for name in selected}, indent=2, ensure_ascii=False
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

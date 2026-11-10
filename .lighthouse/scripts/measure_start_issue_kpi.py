#!/usr/bin/env python3
# lighthouse: core@v0.8.1
"""KPI measurement for the skill lighthouse-start-issue.

WHAT THIS MEASURES
------------------
The skill must do the routing that now falls to the user. The baseline measurement showed that the
spread is almost entirely explained by whether the user passed the route along by hand:

    prompt itself already names a skill   n=7    median  1 tool call lead-in
    prompt names no skill                 n=11   median 14 tool calls lead-in

Both figures are what this script itself produces (`leadin`, cohorts "routed" and "unrouted"),
not the manual count from the design — that came to n=9 and n=12 / median 11 respectively.

The target <= 2 is therefore not an arbitrary number but what a hand-routed prompt already achieves.

MEASUREMENT SETUP
-----------------
Source: the session jsonl files that are on disk anyway. No synthetic test cases, no
eval harness — that is explicitly a no-go of milestone #42. This script evaluates the skill, it
is not part of what the skill does.

A turn runs from a human prompt to the next human prompt. The lead-in is the number of
tool_use blocks BEFORE the first block from ROUTE_TOOLS; that route call itself does not count.
Exclusive, because the measured baseline has a minimum of 0.

WHAT THIS CANNOT DO
-------------------
`worktree-first` sees only the file_path argument of Write/Edit. A file that comes into existence via a
heredoc or `sed -i` in a Bash call is invisible. Parsing the Bash command to guess a path
from it gives silent false counts; the limit is therefore explicit and is also in
the output. A score of 100% means "of the visible writes", not "of all".

`mistrigger` counts only one direction: the skill loaded while there was no issue number in the prompt.
The reverse error — a pickup where the skill did NOT load — is not measured here;
it becomes visible indirectly in row 1, because such a turn keeps the old median.

`leadin` skips the `Skill` call of lighthouse-start-issue itself (neither lead-in nor route call):
the skill is ambient, so on a pickup `Skill{skill: "lighthouse-start-issue"}` is often the FIRST
tool_use of the turn, before anything at all has been routed. Counting it as a route call would make the
metric succeed by construction (lead-in 0, regardless of whether the routing actually improved).
This does not affect the baseline above: it dates from before the skill existed, so no
baseline turn contains this call. `Skill{skill: "lighthouse-route-issue"}` is skipped
on the same ground: since the split (#1115) start-issue calls it for the gates, and the announcement
comes only after that call.

The same exclusion applies to the mandatory scout call on 4b/4c
(`Agent{subagent_type: issue-impact-scout}`, issue #927): that call is the pre-exploration itself,
not the lead-in to it. Without this exclusion every correct 4b/4c pickup gets +1 on this
row, and that reads as a routing regression while the routing did not get slower.

`gate` counts whether the skill announced the chosen triage gate. It cannot see whether that gate was the RIGHT
one — that is a judgement, not a pattern. What it does deliver is the distribution, and that makes the
reverse question answerable: if gates 1 and 2 stay at zero, the gate sequence is dead wood.

Usage:
    python3 .lighthouse/scripts/measure_start_issue_kpi.py           # the whole table
    python3 .lighthouse/scripts/measure_start_issue_kpi.py leadin --since 2026-08-24

Output keys are fixed English tokens.
"""

from __future__ import annotations

import argparse
import functools
import json
import re
import statistics
import subprocess
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# Run as a file (`python3 .lighthouse/scripts/<name>.py`): put `.lighthouse/lib` first, so
# `lighthouse_lib` imports from this installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib import config, headings  # noqa: E402
from lighthouse_lib.config import ConfigError  # noqa: E402


def default_transcript_dir() -> Path:
    """The Claude Code project directory of this repo: `~/.claude/projects/<root>`, where
    `<root>` is the repo root with every character outside `[A-Za-z0-9]` replaced by `-`."""
    root = str(config.repo_root().resolve())
    return Path.home() / ".claude" / "projects" / re.sub(r"[^A-Za-z0-9]", "-", root)


SKILL_NAME = "lighthouse-start-issue"
# Since #1115 the gates live in a read-only skill of their own that start-issue calls. That call is
# the routing itself, like the ambient start skill: `count_leadin` and `text_before_route` both skip
# it. `measure_gate` and the other denominators keep filtering on SKILL_NAME.
ROUTE_SKILL_NAME = "lighthouse-route-issue"
ROUTING_SKILLS = frozenset({SKILL_NAME, ROUTE_SKILL_NAME})

# Step 3 of the skill puts the worktree before the route; this Skill call is therefore always before the
# gate announcement. Only `text_before_route` skips it (#937) — `count_leadin` does not, because
# row 1 counts by definition up to the first Skill call, and that keeps the baseline comparable.
WORKTREE_SKILL_NAME = "superpowers:using-git-worktrees"

# A session that starts in a worktree lands in a sibling directory of the main directory:
# `<main dir>--claude-worktrees-<name>/`. `session_files` reads that one too (#937).
WORKTREE_DIR_INFIX = "--claude-worktrees-"

# The four actions that count as "a route has been chosen". Exactly these four, because the
# baseline measurement used them; extending them makes the follow-up measurement incomparable with the baseline.
ROUTE_TOOLS = frozenset({"Skill", "Write", "Edit", "AskUserQuestion"})
WRITE_TOOLS = frozenset({"Write", "Edit"})

WORKTREE_MARKER = ".claude/worktrees/"


def correct_doc_dirs() -> tuple[str, ...]:
    """The project's spec and plan directories (`[paths] specs`, `plans`), each with a `/`."""
    cfg = config.load(config.config_path())
    return tuple(
        str(config.lookup(cfg, key)).rstrip("/") + "/" for key in ("paths.specs", "paths.plans")
    )


# Superpowers' default directories, where a spec or plan lands if nobody follows the convention.
_LEGACY_ROOT = "docs/" + "superpowers/"
LEGACY_DOC_DIRS = (_LEGACY_ROOT + "specs/", _LEGACY_ROOT + "plans/")

# `paths` counts added files over the git history; without a lower bound it therefore also counts the
# period in which the convention did not exist yet. There is no fixed lower bound: pass the day on which
# the project introduced the convention with --since.

# Dutch prompts "pak 610 op", "begin aan issue 526", "kijk naar 623", "ga verder met #663"; in English  # lighthouse: allow-language — the Dutch example prompts match the Dutch regex branches below
# "pick up 610", "pick 610 up", "start on issue 526", "work on #663", "look at 623".
# Deliberately narrow: a prompt that only MENTIONS a number ("wat staat er in 610?") is not a pickup.  # lighthouse: allow-language — quotes a Dutch example prompt
PICKUP_RE = re.compile(
    r"""
      (?:pak|pik)\s+(?:issue\s*)?\#?(?P<a>\d{2,4})\s+op
    | (?:begin(?:nen)?|start(?:en)?|ga\s+verder|ga\s+aan\s+de\s+slag|werk)\s+
      (?:aan|met)\s+(?:issue\s*)?\#?(?P<b>\d{2,4})
    | kijk\s+(?:eens\s+)?naar\s+(?:issue\s*)?\#?(?P<c>\d{2,4})
    | pick\s+up\s+(?:issue\s*)?\#?(?P<d>\d{2,4})
    | pick\s+(?:issue\s*)?\#?(?P<e>\d{2,4})\s+up
    | (?:start(?:ing)?|continue|get\s+started|work(?:ing)?)\s+
      (?:on|with)\s+(?:issue\s*)?\#?(?P<f>\d{2,4})
    | look\s+at\s+(?:issue\s*)?\#?(?P<g>\d{2,4})
    """,
    re.IGNORECASE | re.VERBOSE,
)

# A slash command, but not an absolute path: `/usr/local/...` is excluded by the negative
# lookahead on '/'.
SLASH_SKILL_RE = re.compile(r"(?<![\w/])/([a-z][a-z0-9-]*(?::[a-z0-9-]+)?)(?![\w/])")

# Broad: any number of 2-4 digits, with or without '#'. Conservative in the right direction — a broad
# match yields FEWER reported mis-triggers, so this number never exaggerates the problem.
ISSUE_NUMBER_RE = re.compile(r"(?<![\w.])#?\d{2,4}(?![\w.])")


def _flexible(key: str) -> str:
    """Every variant of heading `key` as a regex group; a space matches any whitespace."""
    return "(?:" + "|".join(
        re.escape(v).replace("\\ ", "\\s*") for v in headings.variants(key)
    ) + ")"


# The fixed form in which the skill announces its triage outcome: `Poort 2: systematic-debugging`
# (`heading:gate_line`, any language). Five gates (0 through 4); a higher digit is by definition
# not a gate announcement. Gate 4 carries a mandatory sub-letter (`Poort 4b: writing-plans`); without
# `[abc]?` there is no word boundary between `4` and `b` and exactly the prescribed form counts as
# "not reported".
@functools.cache
def gate_re() -> re.Pattern[str]:
    return re.compile(rf"\b{_flexible('gate_line')}\s*(?P<gate>[0-4])[abc]?\b")


SCOUT_AGENT = "lighthouse-issue-impact-scout"

# The trigger is read structurally from the Agent block (`_scout_calls`), not from this regex:
# the announcement line `Scout: issue-impact-scout` from SKILL.md is measured by nothing in this script.
# `scout_skip_re` below IS used — that contract is `heading:scout_skipped` in
# `.lighthouse/headings.toml`, the same heading the skill writes.
@functools.cache
def scout_skip_re() -> re.Pattern[str]:
    return re.compile(
        rf"^{_flexible('scout_skipped')}\s*—\s*(?P<reden>.+?)\s*$", re.MULTILINE
    )


# A reason of one character (or just a full stop) is no reason. Twenty characters is no deep
# truth; it is the lower bound below which "duidelijk." and "simpel." no longer slip through.
SCOUT_MIN_REASON = 20

# The search budget from the persona (`.claude/agents/lighthouse-issue-impact-scout.md`, norm
# "at most 35 search actions"). It is here because `maxTurns` bounds something else — turns,
# not calls — and a limit nobody counts is decoration. tests/pack/test_issue_impact_scout.py
# pins that this number and the persona say the same.
SCOUT_TOOL_CALL_BUDGET = 35

SCOUT_GATES = frozenset({"4b", "4c"})


@functools.cache
def gate_suffix_re() -> re.Pattern[str]:
    return re.compile(rf"\b{_flexible('gate_line')}\s*(?P<gate>[0-4])\s*(?P<sub>[abc])?\b")


@dataclass(frozen=True)
class Turn:
    """A human prompt and everything that followed it up to the next human prompt."""

    prompt: str
    records: tuple[dict, ...]


# --- record recognition -----------------------------------------------------------------------


def prompt_text(record: dict) -> str:
    """The text blocks of a record, joined. Empty string if there is no text in it."""
    content = record.get("message", {}).get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    return "\n".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    )


def is_user_prompt(record: dict) -> bool:
    """A real human prompt.

    Three exclusions that all matter: a tool_result has the same `type: user`
    (without this check every lead-in is 0), `isSidechain` is subagent traffic, and `isMeta` are
    harness injections such as hook context.
    """
    if record.get("type") != "user":
        return False
    if record.get("isSidechain") or record.get("isMeta"):
        return False
    if record.get("userType") not in (None, "external"):
        return False
    return bool(prompt_text(record).strip())


def tool_uses(record: dict) -> list[tuple[str, dict]]:
    """The (name, input) pairs of every tool_use block in an assistant record."""
    if record.get("type") != "assistant" or record.get("isSidechain"):
        return []
    content = record.get("message", {}).get("content")
    if not isinstance(content, list):
        return []
    return [
        (block.get("name", ""), block.get("input") or {})
        for block in content
        if isinstance(block, dict) and block.get("type") == "tool_use"
    ]


def _content_blocks(record: dict) -> list[dict]:
    content = (record.get("message") or {}).get("content")
    return (
        [b for b in content if isinstance(b, dict)] if isinstance(content, list) else []
    )


def iter_turns(records: Iterable[dict]) -> Iterator[Turn]:
    prompt: str | None = None
    buffer: list[dict] = []
    for record in records:
        if is_user_prompt(record):
            if prompt is not None:
                yield Turn(prompt, tuple(buffer))
            prompt, buffer = prompt_text(record), []
        elif prompt is not None:
            buffer.append(record)
    if prompt is not None:
        yield Turn(prompt, tuple(buffer))


# --- prompt classification --------------------------------------------------------------------


def pickup_issue_number(text: str) -> int | None:
    match = PICKUP_RE.search(text)
    if not match:
        return None
    return int(next(number for number in match.groups() if number))


def names_a_skill(text: str) -> bool:
    return bool(SLASH_SKILL_RE.search(text))


def has_issue_number(text: str) -> bool:
    return bool(ISSUE_NUMBER_RE.search(text))


# --- measurements per turn --------------------------------------------------------------------


def count_leadin(turn: Turn) -> int | None:
    """Tool calls before the first route call, or None if no route was chosen."""
    seen = 0
    for record in turn.records:
        for name, payload in tool_uses(record):
            if name == "Skill" and payload.get("skill") in ROUTING_SKILLS:
                # The skill that loads itself ambiently, and the gate skill it calls, are
                # the routing itself: no lead-in for it and no route call to count.
                continue
            if name == "Agent" and payload.get("subagent_type") == SCOUT_AGENT:
                # Same argument: the mandatory scout call on 4b/4c is the pre-exploration itself,
                # not the lead-in to it (see the module docstring above, issue #927 fix 2).
                continue
            if name in ROUTE_TOOLS:
                return seen
            seen += 1
    return None


def first_write_path(turn: Turn) -> str | None:
    for record in turn.records:
        for name, payload in tool_uses(record):
            if name in WRITE_TOOLS:
                path = payload.get("file_path")
                if isinstance(path, str) and path:
                    return path
    return None


def is_worktree_path(path: str) -> bool:
    return WORKTREE_MARKER in path


def text_before_route(turn: Turn) -> str:
    """Assistant text up to and including the message that contains the first route call.

    The gate announcement belongs before the routing. Text and tool_use can sit in the same
    assistant message — the text blocks come before it — so the boundary lies on the
    tool_use block itself, not on the message. The ambient `Skill{skill: lighthouse-start-issue}` is
    skipped on the same ground as in `count_leadin`: that call IS the loading of the skill, not
    the routing after it. `Skill{skill: lighthouse-route-issue}` likewise: that call IS the routing, and the
    gate announcement only follows it. `Skill{skill: superpowers:using-git-worktrees}` too: step 3 puts
    the worktree before the route, so as a boundary it would swallow every gate announcement (#937). If the skill finds no route (gate 0 stops), there is no boundary and the
    whole turn counts.
    """
    parts: list[str] = []
    for record in turn.records:
        if record.get("type") != "assistant" or record.get("isSidechain"):
            continue
        content = record.get("message", {}).get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                payload = block.get("input") or {}
                if block.get("name") == "Skill" and (
                    payload.get("skill") in ROUTING_SKILLS
                    or payload.get("skill") == WORKTREE_SKILL_NAME
                ):
                    continue
                if block.get("name") in ROUTE_TOOLS:
                    return "\n".join(parts)
    return "\n".join(parts)


def announced_gate(turn: Turn) -> int | None:
    """The gate number the skill announced, or None if it routed silently."""
    match = gate_re().search(text_before_route(turn))
    return int(match.group("gate")) if match else None


def loaded_skills(turn: Turn) -> list[str]:
    return [
        payload["skill"]
        for record in turn.records
        for name, payload in tool_uses(record)
        if name == "Skill" and isinstance(payload.get("skill"), str)
    ]


# --- reading in -------------------------------------------------------------------------------


def read_session(path: Path) -> list[dict]:
    """One session file. Broken lines are skipped, not repaired."""
    records: list[dict] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def session_files(directory: Path) -> list[Path]:
    """The session files of the main directory plus those of every worktree sibling.

    Without the siblings every measurement misses the sessions that began in a worktree — controller runs
    and windows that open in the worktree after a handoff. The scout row therefore saw 0 of well over
    thirty real runs (#937).
    """
    siblings = directory.parent.glob(f"{directory.name}{WORKTREE_DIR_INFIX}*")
    return sorted(
        path
        for folder in (directory, *siblings)
        if folder.is_dir()
        for path in folder.glob("*.jsonl")
    )


def _after(record: dict, since: str | None) -> bool:
    stamp = record.get("timestamp")
    return not since or not isinstance(stamp, str) or stamp[:10] >= since


def iter_pickup_turns(directory: Path, since: str | None) -> Iterator[tuple[Turn, int]]:
    """Every turn that begins with a pickup prompt, with the issue number."""
    for path in session_files(directory):
        records = [r for r in read_session(path) if _after(r, since)]
        for turn in iter_turns(records):
            number = pickup_issue_number(turn.prompt)
            if number is not None:
                yield turn, number


# --- the five rows ----------------------------------------------------------------------------


def measure_leadin(directory: Path, since: str | None) -> dict[str, object]:
    routed: list[int] = []
    unrouted: list[int] = []
    no_route = 0
    for turn, _ in iter_pickup_turns(directory, since):
        count = count_leadin(turn)
        if count is None:
            no_route += 1
            continue
        (routed if names_a_skill(turn.prompt) else unrouted).append(count)
    return {
        "routed": _summary(routed),
        "unrouted": _summary(unrouted),
        "no_route": no_route,
    }


def _summary(values: list[int]) -> dict[str, object]:
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "min": min(values),
        "median": statistics.median(values),
        "max": max(values),
        "mean": round(statistics.fmean(values), 1),
    }


def measure_worktree_first(directory: Path, since: str | None) -> dict[str, object]:
    inside = outside = invisible = 0
    for turn, _ in iter_pickup_turns(directory, since):
        path = first_write_path(turn)
        if path is None:
            invisible += 1
        elif is_worktree_path(path):
            inside += 1
        else:
            outside += 1
    visible = inside + outside
    return {
        "in_worktree": inside,
        "outside_worktree": outside,
        "no_visible_write": invisible,
        "share": round(inside / visible, 3) if visible else None,
        "note": "Write/Edit only; Bash writes are invisible",
    }


def measure_mistrigger(directory: Path, since: str | None) -> dict[str, object]:
    loaded = mistriggered = 0
    examples: list[str] = []
    for path in session_files(directory):
        records = [r for r in read_session(path) if _after(r, since)]
        for turn in iter_turns(records):
            if SKILL_NAME not in loaded_skills(turn):
                continue
            loaded += 1
            if not has_issue_number(turn.prompt):
                mistriggered += 1
                examples.append(turn.prompt[:120])
    return {
        "loaded": loaded,
        "without_issue_number": mistriggered,
        "examples": examples[:5],
    }


def measure_gate(directory: Path, since: str | None) -> dict[str, object]:
    """Did the skill announce which gate it took?

    The denominator is deliberately narrow: only pickups in which the skill actually loaded. A pickup without
    the skill is the error that `leadin` and `mistrigger` measure; counting it here would squeeze two different
    failure modes into one percentage.

    The distribution is a by-catch, not a goal: it shows whether gates 1 and 2 fire at all. If they stay
    at zero while bug and recipe issues did come by, the gate sequence is dead wood.
    """
    reported = not_reported = without_skill = 0
    distribution: dict[str, int] = {}
    for turn, _ in iter_pickup_turns(directory, since):
        if SKILL_NAME not in loaded_skills(turn):
            without_skill += 1
            continue
        gate = announced_gate(turn)
        if gate is None:
            not_reported += 1
            continue
        reported += 1
        key = f"gate_{gate}"
        distribution[key] = distribution.get(key, 0) + 1
    total = reported + not_reported
    return {
        "reported": reported,
        "not_reported": not_reported,
        "share": round(reported / total, 3) if total else None,
        "distribution": dict(sorted(distribution.items())),
        "pickup_without_skill": without_skill,
    }


def _scout_calls(turn: Turn) -> list[tuple[dict, dict | None]]:
    """Every Agent call to the scout, with the matching tool_result or None."""
    calls: list[tuple[str, dict]] = []
    for record in turn.records:
        for block in _content_blocks(record):
            if block.get("type") != "tool_use" or block.get("name") != "Agent":
                continue
            payload = block.get("input") or {}
            if payload.get("subagent_type") == SCOUT_AGENT:
                calls.append((block.get("id") or "", record))
    results = {
        block.get("tool_use_id"): record
        for record in turn.records
        for block in _content_blocks(record)
        if block.get("type") == "tool_result"
    }
    return [(record, results.get(uid)) for uid, record in calls]


def _seconds_between(start: dict, end: dict) -> float | None:
    a, b = start.get("timestamp"), end.get("timestamp")
    if not isinstance(a, str) or not isinstance(b, str):
        return None
    try:
        t0 = datetime.fromisoformat(a)
        t1 = datetime.fromisoformat(b)
    except ValueError:
        return None
    return (t1 - t0).total_seconds()


def _tool_use_count(end: dict) -> int | None:
    """How many tool calls the scout run made, from `toolUseResult.totalToolUseCount`.

    This is the measurable proxy for the budget. Mind the semantics: `maxTurns` in the
    agent frontmatter bounds **turns** (tool-use round trips), and one turn can carry several
    parallel calls. A run with more tool calls than `maxTurns` is therefore not an
    overrun; the number says how widely it searched, not whether a ceiling was hit.
    """
    result = end.get("toolUseResult")
    if not isinstance(result, dict):
        return None
    count = result.get("totalToolUseCount")
    return count if isinstance(count, int) else None


def _finished(end: dict) -> bool:
    """Is this return the end of a run?

    Not for a background start (`status: async_launched`, return after a few seconds while
    the run is still going) and not for a rejected call (`toolUseResult` is then a string). Both
    pulled the runtime median down (#937).
    """
    result = end.get("toolUseResult")
    if isinstance(result, str):
        return False
    return not (isinstance(result, dict) and result.get("status") == "async_launched")


def _run_stats(calls: list[tuple[dict, dict | None]]) -> tuple[list[float], list[int]]:
    """Runtime and tool-call count per finished scout call; a call without a return does not count."""
    seconds: list[float] = []
    counts: list[int] = []
    for start, end in calls:
        if end is None or not _finished(end):
            continue
        duration = _seconds_between(start, end)
        if duration is not None:
            seconds.append(duration)
        count = _tool_use_count(end)
        if count is not None:
            counts.append(count)
    return seconds, counts


def announced_subgate(turn: Turn) -> str | None:
    """`4b` or `4c` if the skill announced the sub-letter, otherwise only the digit as a string."""
    match = gate_suffix_re().search(text_before_route(turn))
    if not match:
        return None
    return match.group("gate") + (match.group("sub") or "")


def measure_scout(directory: Path, since: str | None) -> dict[str, object]:
    """Did the scout fire where it should, and was it silent nowhere?

    The denominator is narrow, as with `gate`: only pickups in which the skill loaded and the announced
    gate was `4b` or `4c`. A pickup without the skill or without an announced gate is the error that
    `leadin` and `gate` already measure.

    `mistrigger` is deliberately outside that denominator: a scout call on gate 0, 1, 2 or 4a is not a
    bad 4b/4c run, it is a different error, and squeezing two failure modes into one percentage makes
    both unreadable.

    `tool_calls` and `over_budget` measure the search budget from the persona
    (`SCOUT_TOOL_CALL_BUDGET`), not `maxTurns`: that ceiling counts turns, and one turn carries
    several parallel calls. A run over the budget is therefore an overrun of the
    prompt norm, not a runtime error.

    An announced gate `4` without a sub-letter is **unknown**, not a mis-trigger: until issue #927
    SKILL.md did not require a sub-letter for gate 4, so such an announcement can be a correct 4b/4c run that
    announced only the digit. Such a turn does not count as a mistrigger and not in the
    denominator (`total`) of this row. Gate 0/1/2/4a stay unambiguous and are therefore left alone.

    The same holds for a scout call without any gate announcement: routing silently is no evidence
    for gate 0/1/2/4a (#937). Those runs count in `gate_unknown`, so they stay visible
    without polluting the denominator or `mistrigger`; the gate error itself is measured by row `gate`.
    """
    triggered = with_reason = silent = mistrigger = unknown = 0
    runtimes: list[float] = []
    tool_calls: list[int] = []
    for turn, _ in iter_pickup_turns(directory, since):
        if SKILL_NAME not in loaded_skills(turn):
            continue
        calls = _scout_calls(turn)
        subgate = announced_subgate(turn)
        if subgate is None or subgate == "4":
            unknown += 1 if calls else 0
            continue
        if subgate not in SCOUT_GATES:
            mistrigger += len(calls)
            continue
        seconds, counts = _run_stats(calls)
        runtimes += seconds
        tool_calls += counts
        if calls:
            triggered += 1
            continue
        skip = scout_skip_re().search(text_before_route(turn))
        reason = skip.group("reden").strip(" .") if skip else ""
        if len(reason) >= SCOUT_MIN_REASON:
            with_reason += 1
        else:
            silent += 1
    total = triggered + with_reason + silent
    return {
        "triggered": triggered,
        "skipped_with_reason": with_reason,
        "silent": silent,
        "mistrigger": mistrigger,
        "gate_unknown": unknown,
        "share": round((triggered + with_reason) / total, 3) if total else None,
        "runtime_seconds": _summary_float(runtimes),
        "tool_calls": _summary_int(tool_calls),
        "over_budget": sum(1 for n in tool_calls if n > SCOUT_TOOL_CALL_BUDGET),
    }


def _summary_int(values: list[int]) -> dict[str, object]:
    if not values:
        return {"n": 0, "median": None, "max": None}
    return {
        "n": len(values),
        "median": round(statistics.median(values)),
        "max": max(values),
    }


def _summary_float(values: list[float]) -> dict[str, object]:
    if not values:
        return {"n": 0, "median": None, "max": None}
    return {
        "n": len(values),
        "median": round(statistics.median(values), 1),
        "max": round(max(values), 1),
    }


def measure_paths(since: str | None) -> dict[str, object]:
    """Reads git, not transcripts: where did new spec and plan files land?

    Without --since the whole history counts, including the period in which the convention did not exist yet;
    a target of 100% is then unreachable by construction. Therefore pass the day on which the
    convention began; `since` in the output always names which window was counted.
    """
    argv = [
        "git",
        "-C",
        str(config.repo_root()),
        "log",
        "--diff-filter=A",
        "--name-only",
        "--pretty=format:",
    ]
    if since:
        argv += ["--since", since]
    output = subprocess.run(argv, capture_output=True, text=True, check=True).stdout
    added = [line for line in output.splitlines() if line.strip()]
    correct = [f for f in added if f.startswith(correct_doc_dirs())]
    legacy = [f for f in added if f.startswith(LEGACY_DOC_DIRS)]
    total = len(correct) + len(legacy)
    return {
        "since": since,
        "right_dir": len(correct),
        "superpowers_default_dir": len(legacy),
        "share": round(len(correct) / total, 3) if total else None,
    }


# --- CLI --------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Review measurement for the skill lighthouse-start-issue, from the Claude Code transcripts."
    )
    parser.add_argument(
        "metric",
        nargs="?",
        choices=["leadin", "worktree-first", "mistrigger", "paths", "gate", "scout"],
        help="leave out for the whole table",
    )
    parser.add_argument("--dir", type=Path, help="default: this repo's Claude Code projects dir")
    parser.add_argument("--since", help="YYYY-MM-DD; only records from this day on")
    args = parser.parse_args(argv)

    try:
        if args.dir is None:
            args.dir = default_transcript_dir()
    except ConfigError as exc:
        print(f"measure: {exc}", file=sys.stderr)
        return 2
    if not args.dir.is_dir():
        print(f"transcript directory does not exist: {args.dir}", file=sys.stderr)
        return 2

    rows = {
        "leadin": lambda: measure_leadin(args.dir, args.since),
        "worktree-first": lambda: measure_worktree_first(args.dir, args.since),
        "mistrigger": lambda: measure_mistrigger(args.dir, args.since),
        "paths": lambda: measure_paths(args.since),
        "gate": lambda: measure_gate(args.dir, args.since),
        "scout": lambda: measure_scout(args.dir, args.since),
    }
    selected = [args.metric] if args.metric else list(rows)
    try:
        table = {name: rows[name]() for name in selected}
    except ConfigError as exc:
        print(f"measure: {exc}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as exc:
        print(f"measure: git log failed: {(exc.stderr or '').strip()}", file=sys.stderr)
        return 2
    print(json.dumps(table, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

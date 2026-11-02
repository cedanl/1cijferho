# lighthouse: core@v0.8.1
"""The comment protocol: the `🤖 <kind>` comments on an issue, read and written.

Every comment of the protocol starts with `🤖 <Kind>`. Below it, up to the first
blank line, stand fields as `key: value`. The footer `<sub>session <uuid> ...</sub>`
ties an outcome to exactly one run; the LAST footer counts, so a session id in free
text cannot override anything.

Writers use the English keys and the heading in `[project] language` (`headings.toml`).
Readers accept every heading variant and the keys and values of the comments that
already stand on issues (`_LEGACY_KEYS`, `_LEGACY_VALUES`, `LEGACY_MARKERS`, decision
15 and 21). The kinds and the keys of a header are protocol tokens, not prose.

Controller policy stays with the caller (decision 25): which commands may start, what
counts as an answer, the check of `lane`, and the prose of a `🤖 Done`.

The syntax stays within Python 3.9, like the rest of the lib.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# Run as a file on its path: put `.lighthouse/lib` first (see `config.py`).
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import headings
from lighthouse_lib.config import config_path, load, lookup

ROBOT = "🤖"
# The marker of an artefact comment (`artefacts.py`). Readers accept the legacy one
# until M2 (decision 15).
MARKER = "<!-- lighthouse:"
LEGACY_MARKERS = ("<!-- paia:",)
# The canonical kinds; the heading of kind `x` is `kind_x` in `headings.toml`.
KINDS = ("handoff", "started", "question", "resumed", "done", "returned", "rebased", "parked")
# The option letters of a question, and the most questions in one comment.
LETTERS = "abcdef"
MAX_QUESTIONS = 4

# The keys of a header as they stand in comments from before the move (decision 21).
_LEGACY_KEYS = {
    "volgende": "next",
    "wanneer": "when",
    "anker": "anchor",
    "basis": "base",
    "sessie": "session",
    "commando": "command",
    "stapel": "stack",
    "blokkade": "blocker",
    "sprong": "skip",
    "terugval": "fallback",
    "voorganger": "predecessor",
    "op": "onto",
    "antwoord": "answer",
    "reden": "reason",
}
_LEGACY_VALUES = {
    ("when", "nu"): "now",
    ("when", "nacht"): "night",
    ("blocker", "verrekend"): "accounted",
}
# The fields of a question as a classification from before the move holds them.
_LEGACY_QUESTION_FIELDS = {
    "fase": "phase",
    "taak": "task",
    "waar_het_staat": "where",
    "wat_er_speelt": "what",
    "commandos": "commands",
    "vragen": "questions",
    "vraag": "question",
    "opties": "options",
}
_LEGACY_OPTION_FIELDS = {"tekst": "text", "aanbevolen": "recommended"}

_FIELD = re.compile(r"^([a-z_]+):\s?(.*)$")
_SESSION = re.compile(
    r"<sub>(?:session|sessie) "
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)
_QUESTION_OPTION = re.compile(r"^- \*\*[1-4]?[a-f]\)\*\*(?: (.*))?$")
FENCE_OPEN = re.compile(r"^(`{3,})bash$")


@dataclass(frozen=True)
class Comment:
    id: int
    body: str
    created_at: str
    rockets: int = 0
    url: str = ""

    @classmethod
    def from_gh(cls, raw: dict) -> Comment:
        """One element of `gh api repos/<repo>/issues/<nr>/comments`."""
        reactions = raw.get("reactions") or {}
        return cls(
            id=int(raw["id"]),
            body=str(raw.get("body") or ""),
            created_at=str(raw.get("created_at") or ""),
            rockets=int(reactions.get("rocket") or 0),
            url=str(raw.get("html_url") or ""),
        )


@dataclass(frozen=True)
class Header:
    kind: str
    fields: dict[str, str] = field(default_factory=dict)
    session: str | None = None


@dataclass(frozen=True)
class Handoff:
    issue: int
    worktree: str
    branch: str
    anchor: str
    next: str
    when: str
    lane: str = ""


class HandoffError(ValueError):
    """A handoff that cannot start; the text is the reason for a `🤖 Returned`."""


# A parser reads the heading table for every comment, and `headings` reads the file
# on every call. Cached per state of the file: path, mtime and size.
_cache: dict = {"stamp": None, "variants": {}}


def _variants(key: str) -> tuple[str, ...]:
    where = headings.path()
    try:
        stat = where.stat()
    except OSError:
        return headings.variants(key)  # raises the ConfigError that names the file
    stamp = (str(where), stat.st_mtime_ns, stat.st_size)
    if _cache["stamp"] != stamp:
        _cache["stamp"], _cache["variants"] = stamp, {}
    known = _cache["variants"]
    if key not in known:
        known[key] = headings.variants(key)
    return known[key]


def _alternatives(key: str) -> str:
    """A regex group that matches every variant of `key` literally."""
    return "(?:" + "|".join(re.escape(v) for v in _variants(key)) + ")"


def _language(language: str | None) -> str:
    """`language`, or `[project] language` of the repo when it is `None`."""
    return language if language is not None else load(config_path()).language


def _heading(key: str, language: str | None) -> str:
    return headings.heading(key, _language(language))


def _kind_of(title: str) -> str | None:
    """The canonical kind of the text after `🤖 `: a variant, followed by nothing,
    whitespace or `·`."""
    pairs = sorted(
        ((variant, kind) for kind in KINDS for variant in _variants("kind_" + kind)),
        key=lambda pair: -len(pair[0]),
    )
    for variant, kind in pairs:
        rest = title[len(variant) :]
        if title.startswith(variant) and (not rest or rest[0].isspace() or rest[0] == "·"):
            return kind
    return None


def parse_header(body: str) -> Header | None:
    lines = body.strip().splitlines()
    if not lines or not lines[0].startswith(ROBOT + " "):
        return None
    kind = _kind_of(lines[0][len(ROBOT) + 1 :].strip())
    if kind is None:
        return None
    fields: dict[str, str] = {}
    for line in lines[1:]:
        if not line.strip():
            break
        match = _FIELD.match(line.strip())
        if match:
            key = _LEGACY_KEYS.get(match.group(1), match.group(1))
            value = match.group(2).strip()
            fields[key] = _LEGACY_VALUES.get((key, value), value)
    sessions = _SESSION.findall(body)
    return Header(kind=kind, fields=fields, session=sessions[-1] if sessions else None)


def stack_base(bodies: Iterable[str]) -> str | None:
    """The field `base` of the last `🤖 Started` that has one, whatever comes after."""
    base = None
    for body in bodies:
        header = parse_header(body)
        if header is not None and header.kind == "started" and header.fields.get("base"):
            base = header.fields["base"]
    return base


def active_stack(bodies: Iterable[str]) -> Header | None:
    """The last `🤖 Started` with `base`, unless a `Rebased` or `Returned` follows it."""
    active = None
    for body in bodies:
        header = parse_header(body)
        if header is None:
            continue
        if header.kind == "started" and header.fields.get("base"):
            active = header
        elif header.kind in ("rebased", "returned"):
            active = None
    return active


def parse_handoff(header: Header) -> Handoff:
    """The fields of a `🤖 Handoff`. `lane` is an ordinary field here; the caller
    checks it against its own policy."""
    if header.kind != "handoff":
        raise HandoffError(f"not a handoff but {header.kind}")
    fields = header.fields
    missing = [
        k for k in ("issue", "worktree", "branch", "next", "when") if not fields.get(k)
    ]
    if missing:
        raise HandoffError("fields missing: " + ", ".join(missing))
    number = re.fullmatch(r"#?([0-9]+)", fields["issue"])
    if number is None:
        raise HandoffError(f"issue unreadable: {fields['issue']}")
    if fields["when"] not in ("now", "night"):
        raise HandoffError(f"when must be now or night, not {fields['when']}")
    return Handoff(
        issue=int(number.group(1)),
        worktree=fields["worktree"].rstrip("/"),
        branch=fields["branch"],
        anchor=fields.get("anchor", ""),
        next=fields["next"],
        when=fields["when"],
        lane=fields.get("lane", ""),
    )


def _clean(text: str) -> str:
    """Free text cannot break open a footer or a code block."""
    return (
        text.replace("```", "'''")
        .replace("~~~", "'''")
        .replace("<sub>", "&lt;sub&gt;")
        .strip()
    )


def _code_block(command: str) -> list[str]:
    """One command literally as a fenced code block; the fence is longer than every
    run of backticks in the command, so the block never closes early."""
    longest = max((len(r) for r in re.findall(r"`+", command)), default=0)
    fence = "`" * max(3, longest + 1)
    return [f"{fence}bash", command, fence]


def footer(session: str, *extra: str) -> str:
    return "<sub>" + " · ".join([f"session {session}", *[e for e in extra if e]]) + "</sub>"


def render_started(
    *,
    session: str,
    worktree: str,
    command: str,
    start: str,
    head_sha: str = "",
    stack: str = "",
    base: str = "",
    branch: str = "",
    blocker: str = "",
    skip: str = "",
    runner: str = "",
    workspace: str = "",
    fallback: str = "",
    language: str | None = None,
) -> str:
    """A stacked start sets `stack` (`#<x> <branch-x>`), `base` and `branch`;
    `active_stack` reads them back. `blocker: accounted` tells the run that the stack
    rule already accounted for an open blocker. `skip` reports that a review from the
    config was skipped. A forge run carries `runner` and, once it has one,
    `workspace`; `fallback` is why a fresh forge start ran headless after all. Each
    field stands there only when it is not empty."""
    lines = [
        f"{ROBOT} {_heading('kind_started', language)}",
        f"session: {session}",
        f"worktree: {worktree}",
        f"command: {command}",
        f"start: {start}",
    ]
    for key, value in (
        ("head", head_sha),
        ("stack", stack),
        ("base", base),
        ("branch", branch),
        ("blocker", blocker),
        ("skip", skip),
        ("runner", runner),
        ("workspace", workspace),
        ("fallback", fallback),
    ):
        if value:
            lines.append(f"{key}: {value}")
    return "\n".join(lines) + "\n\n" + footer(session)


def fill_started(message: str, **fields: str) -> str:
    """Set fields in the header of a `🤖 Started`, or replace them; the rest stays.

    An existing field keeps its place, a new one goes to the end of the header; an
    empty value removes the field, as `render_started` leaves it out. A value is one
    line: whitespace (a newline too) becomes one space, so it cannot break the header
    open. The footer and everything after the first blank line stay literally."""
    lines = message.split("\n")
    end = next((i for i, line in enumerate(lines) if not line.strip()), len(lines))
    header = lines[:end]
    for key, value in fields.items():
        clean = " ".join(str(value).split())
        place = next(
            (i for i, line in enumerate(header) if i > 0 and line.startswith(f"{key}:")),
            None,
        )
        if not clean:
            if place is not None:
                del header[place]
        elif place is not None:
            header[place] = f"{key}: {clean}"
        else:
            header.append(f"{key}: {clean}")
    return "\n".join(header + lines[end:])


def render_rebased(
    *,
    predecessor: int,
    base: str,
    session: str | None = None,
    onto: str | None = None,
    language: str | None = None,
) -> str:
    """After the squash merge of the predecessor: the own commits now stand on
    `onto`, by default `origin/<forge.default_branch>`."""
    if onto is None:
        onto = "origin/" + str(lookup(load(config_path()), "forge.default_branch"))
    text = (
        f"{ROBOT} {_heading('kind_rebased', language)}\n"
        f"predecessor: #{predecessor}\nbase: {base}\nonto: {onto}"
    )
    if session:
        text += "\n\n" + footer(session)
    return text


def render_resumed(*, session: str, answer_id: int, language: str | None = None) -> str:
    return (
        f"{ROBOT} {_heading('kind_resumed', language)}\n"
        f"session: {session}\nanswer: {answer_id}\n\n" + footer(session)
    )


def render_done(
    *, session: str, pr_url: str, note: str = "", language: str | None = None
) -> str:
    """Header, fields and footer; the prose in between (`note`) comes from the caller."""
    text = f"{ROBOT} {_heading('kind_done', language)}\nsession: {session}\npr: {pr_url}"
    if note.strip():
        text += "\n\n" + note.strip()
    return text + "\n\n" + footer(session)


def render_returned(
    *,
    reason: str,
    session: str | None = None,
    detail: str = "",
    language: str | None = None,
) -> str:
    lines = [
        f"{ROBOT} {_heading('kind_returned', language)}",
        "reason: " + " ".join(reason.split()),
    ]
    if session:
        lines.append(f"session: {session}")
    text = "\n".join(lines)
    if detail.strip():
        text += "\n\n```\n" + _clean(detail) + "\n```"
    if session:
        text += "\n\n" + footer(session)
    return text


def _option(option: dict) -> dict:
    return {_LEGACY_OPTION_FIELDS.get(k, k): v for k, v in option.items()}


def _new_fields(fields: dict) -> dict:
    """`fields` with the keys of a classification from before the move renamed."""
    return {_LEGACY_QUESTION_FIELDS.get(k, k): v for k, v in fields.items()}


def questions_of(fields: dict) -> list[dict]:
    """The questions of a `fields` dict: `questions`, or the old form with a single
    `question`/`options`. Keys from before the move are read too."""
    fields = _new_fields(fields)
    if "questions" in fields:
        raw = [_new_fields(q) for q in fields["questions"]]
    else:
        raw = [{"question": fields.get("question", ""), "options": fields.get("options", [])}]
    return [
        {
            "question": q.get("question", ""),
            "options": [_option(o) for o in q.get("options", [])],
        }
        for q in raw
    ]


def _run_yourself(fields: dict, language: str | None) -> list[str]:
    """The run-yourself block: each command its own code block, empty without commands."""
    commands = [str(c) for c in fields.get("commands") or [] if str(c).strip()]
    if not commands:
        return []
    lines = ["", _heading("question_run_yourself", language)]
    for command in commands:
        lines += ["", *_code_block(command)]
    return [*lines, ""]


def render_question(
    *,
    issue: int,
    title: str,
    issue_url: str,
    fields: dict,
    session: str,
    worktree: str,
    handoff_url: str = "",
    language: str | None = None,
) -> str:
    fields = _new_fields(fields)
    language = _language(language)

    def h(key: str) -> str:
        return _heading(key, language)

    top = (
        f"{ROBOT} {h('kind_question')} · #{issue} {title} ([link]({issue_url})) · "
        f"{h('question_phase')} {_clean(str(fields.get('phase') or '?'))}"
    )
    if fields.get("task"):
        top += f" · {h('question_task')} {_clean(str(fields['task']))}"
    questions = questions_of(fields)[:MAX_QUESTIONS] or [{"question": "", "options": []}]
    bundle = len(questions) > 1
    lines = [
        top,
        "",
        f"**{h('question_where')}:** {_clean(fields['where'])}",
        f"**{h('question_what')}:** {_clean(fields['what'])}",
    ]
    # One run-yourself block for the whole comment: with one question below the
    # question line, with a bundle before question 1.
    run_yourself = _run_yourself(fields, language)
    if bundle:
        lines += run_yourself
    for number, question in enumerate(questions, start=1):
        prefix = str(number) if bundle else ""
        if bundle and number > 1:
            # Without a blank line, question n continues the previous option.
            lines.append("")
        lines.append(
            f"**{h('question_label')}{' ' + prefix if bundle else ''}:** "
            f"{_clean(question['question'])}"
        )
        if not bundle:
            lines += run_yourself
        options = list(question["options"])[: len(LETTERS)]
        for letter, option in zip(LETTERS, options):
            line = f"- **{prefix}{letter})** {_clean(option['text'])}"
            if option.get("recommended"):
                line += " " + h("question_recommended")
            lines.append(line)
    lines += ["", h("question_close")]
    if bundle:
        lines.append(h("question_bundle_hint"))
    lines.append(
        footer(
            session,
            f"worktree {worktree}",
            f"handoff {handoff_url}" if handoff_url else "",
        )
    )
    return "\n".join(lines)


def _add_option(questions: list[dict], text: str) -> None:
    """Add an option to the last question; without one, to an empty question."""
    if not questions:
        questions.append({"question": "", "options": []})
    recommended = None
    for variant in _variants("question_recommended"):
        if text == variant or text.endswith(" " + variant):
            recommended = variant
            break
    if recommended is not None:
        text = text[: -len(recommended)].strip()
    questions[-1]["options"].append({"text": text, "recommended": recommended is not None})


def _question_pieces(
    lines: list[str],
) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """The lines between header and close as pieces per field or option, and apart
    the commands: the content of each code block under the run-yourself line,
    literally. Inside a code block no line starts a new piece, not even one that
    looks like an option. Every question field opens a piece `question`."""
    field_line = re.compile(
        r"^\*\*(?:(?P<where>{})|(?P<what>{})|{}(?: [1-4])?):\*\*(?: (?P<text>.*))?$".format(
            _alternatives("question_where"),
            _alternatives("question_what"),
            _alternatives("question_label"),
        )
    )
    run_yourself = set(_variants("question_run_yourself"))
    pieces: list[tuple[str, list[str]]] = []
    commands: list[str] = []
    fence: str | None = None
    block: list[str] = []
    for line in lines:
        if fence is not None:
            if line == fence:
                commands.append("\n".join(block))
                fence, block = None, []
            else:
                block.append(line)
            continue
        open_ = FENCE_OPEN.match(line)
        match = field_line.match(line)
        option = _QUESTION_OPTION.match(line)
        if line in run_yourself:
            pieces.append(("commands", []))
        elif open_ and pieces and pieces[-1][0] == "commands":
            fence = open_.group(1)
        elif match:
            name = "where" if match.group("where") else "what" if match.group("what") else "question"
            pieces.append((name, [match.group("text") or ""]))
        elif option:
            pieces.append(("option", [option.group(1) or ""]))
        elif pieces:
            pieces[-1][1].append(line)
    return pieces, commands


def parse_question(body: str) -> dict | None:
    """The inverse of `render_question`: the `fields` as they went in, cleaned.

    `None` when the comment is no `🤖 Question`. The block runs from the blank line
    after the header to the LAST close line; a blank line in free text belongs to the
    running piece, only the opening lines of a field or option start a new piece."""
    header = parse_header(body)
    if header is None or header.kind != "question":
        return None
    lines = body.strip().splitlines()
    top = re.match(
        r"^.* · {} (.*?)(?: · {} (.*))?$".format(
            _alternatives("question_phase"), _alternatives("question_task")
        ),
        lines[0],
    )
    fields: dict = {
        "phase": top.group(1).strip() if top else "",
        "task": (top.group(2) or "").strip() if top else "",
        "where": "",
        "what": "",
        "commands": [],
        "questions": [],
    }
    close = set(_variants("question_close"))
    end = max((i for i, line in enumerate(lines) if line.strip() in close), default=len(lines))
    pieces, fields["commands"] = _question_pieces(lines[1:end])
    for name, parts in pieces:
        text = "\n".join(parts).strip()
        if name == "commands":
            continue
        if name == "option":
            _add_option(fields["questions"], text)
        elif name == "question":
            fields["questions"].append({"question": text, "options": []})
        else:
            fields[name] = text
    return fields

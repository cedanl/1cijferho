# lighthouse: core@v0.8.1
"""Artefact comments: hidden metadata, a visible header line, the content.

A spec or plan stands as a version comment on its issue. Line 1 is an HTML comment
with JSON metadata (`<!-- lighthouse: {...} -->`), then a visible `<sub>` line, a
blank line and the content. Readers accept the legacy marker and the legacy keys
until M2 (decision 15 and 21); writers use the new ones. Apart from `render`, which
reads the config for the source link and the language, this module does no I/O.

The syntax stays within Python 3.9, like the rest of the lib.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Run as a file on its path: put `.lighthouse/lib` first (see `config.py`).
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import headings
from lighthouse_lib.config import ConfigError, config_path, load
from lighthouse_lib.protocol import LEGACY_MARKERS, MARKER, Comment

# The maximum length of a GitHub comment, in characters.
GITHUB_LIMIT = 65_536

KINDS = ("spec", "plan")

_META_LINE = re.compile(
    r"^(?:" + "|".join(re.escape(m) for m in (MARKER, *LEGACY_MARKERS)) + r") (\{.*\}) -->$"
)
_SHA = re.compile(r"[0-9a-f]{40}")
_HASH = re.compile(r"[0-9a-f]{12}")

_FIELDS = ("kind", "version", "part", "parts", "file", "commit", "hash", "replaces")
# The keys as they stand in comments from before the move.
_LEGACY_FIELDS = {
    "soort": "kind",
    "versie": "version",
    "deel": "part",
    "delen": "parts",
    "bestand": "file",
    "vervangt": "replaces",
}


@dataclass(frozen=True)
class Meta:
    kind: str
    version: int
    part: int
    parts: int
    file: str
    commit: str
    hash: str
    replaces: str | None
    # Unknown fields: the parser keeps them, `render` writes them back.
    extra: dict = field(default_factory=dict)


def _positive(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def parse_meta(body: str) -> Meta | None:
    """The metadata of the first line of `body`, or `None` when absent or invalid."""
    lines = body.strip().splitlines()
    if not lines:
        return None
    match = _META_LINE.match(lines[0])
    if not match:
        return None
    try:
        raw = json.loads(match.group(1))
    except ValueError:
        return None
    if not isinstance(raw, dict):
        return None
    data = {_LEGACY_FIELDS.get(key, key): value for key, value in raw.items()}
    if any(name not in data for name in _FIELDS):
        return None
    valid = (
        data["kind"] in KINDS
        and _positive(data["version"])
        and _positive(data["part"])
        and _positive(data["parts"])
        and data["part"] <= data["parts"]
        and isinstance(data["file"], str)
        and isinstance(data["commit"], str)
        and _SHA.fullmatch(data["commit"]) is not None
        and isinstance(data["hash"], str)
        and _HASH.fullmatch(data["hash"]) is not None
        and (data["replaces"] is None or isinstance(data["replaces"], str))
    )
    if not valid:
        return None
    extra = {key: value for key, value in data.items() if key not in _FIELDS}
    return Meta(**{name: data[name] for name in _FIELDS}, extra=extra)


def content(body: str) -> str:
    """Everything after the metadata line, the `<sub>` line and the blank line."""
    return "".join(body.lstrip().split("\n", 3)[3:])


def render(meta: Meta, content: str, language: str | None = None) -> str:
    """The comment body: metadata line, visible header line, blank line, content.

    The header line links to the file at its commit on `forge.repo`, in `language`
    (default `[project] language`)."""
    data = {name: getattr(meta, name) for name in _FIELDS}
    data.update(meta.extra)
    json_text = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    # `--` can close the HTML comment early.
    if "--" in json_text:
        raise ValueError("metadata holds '--'; that closes the HTML comment")
    cfg = load(config_path())
    if cfg.repo is None:
        raise ConfigError(f"{config_path()}: forge.repo is not set; an artefact links to its source")
    language = cfg.language if language is None else language

    def h(key: str) -> str:
        return headings.heading(key, language)

    path = html.escape(meta.file)
    header = (
        f"<sub>{meta.kind} · {h('artefact_version')} {meta.version} · "
        f"{h('artefact_part')} {meta.part}/{meta.parts} · {h('artefact_source')} "
        f'<a href="https://github.com/{cfg.repo}/blob/{meta.commit}/{path}">{path}</a> '
        f"@ {meta.commit[:7]}</sub>"
    )
    return f"{MARKER} {json_text} -->\n{header}\n\n{content}"


def hash_of(data: bytes) -> str:
    """The first 12 hex characters of the sha256 of `data`."""
    return hashlib.sha256(data).hexdigest()[:12]


_FENCE = re.compile(r" {0,3}(`{3,}|~{3,})(.*)")


def _cut_points(text: str) -> list[tuple[int, int]]:
    """All cut points as `(offset, class)`; class 1 is the strongest.

    Every line start `p > 0` is a cut point of class 4. Class 1 (`## `), 2 (`### `)
    and 3 (after a blank line) hold only outside a code block: the line before `p`
    and the line at `p` both start outside a block.
    """
    points: list[tuple[int, int]] = []
    fence: tuple[str, int] | None = None  # (character, length) of the open block
    previous_outside = False
    previous_blank = False
    offset = 0
    for line in text.splitlines(keepends=True):
        outside = fence is None
        if offset > 0:
            cls = 4
            if previous_outside and outside:
                if line.startswith("## "):
                    cls = 1
                elif line.startswith("### "):
                    cls = 2
                elif previous_blank:
                    cls = 3
            points.append((offset, cls))
        match = _FENCE.match(line)
        if match:
            char, rest = match.group(1), match.group(2)
            if fence is None:
                fence = (char[0], len(char))
            elif char[0] == fence[0] and len(char) >= fence[1] and rest.strip() == "":
                fence = None
        previous_outside = outside
        previous_blank = line.strip() == ""
        offset += len(line)
    return points


def split(text: str, limit: int = 60_000) -> list[str]:
    """Cut `text` into parts of at most `limit` characters, on markdown boundaries.

    Per part: the strongest cut-point class in the window `(start, start + limit]`,
    within it the furthest point; without a cut point the cut falls mid-line. No part
    is empty (except for empty input) and the parts together are exactly `text`.
    """
    points = _cut_points(text)
    parts: list[str] = []
    start = 0
    while len(text) - start > limit:
        candidates = [(c, p) for p, c in points if start < p <= start + limit]
        if candidates:
            strongest = min(c for c, _ in candidates)
            cut = max(p for c, p in candidates if c == strongest)
        else:
            cut = start + limit
        parts.append(text[start:cut])
        start = cut
    parts.append(text[start:])
    return parts


def _versions(
    comments: list[Comment], kind: str, file: str
) -> dict[int, list[tuple[Meta, Comment]]]:
    """The parts of one chain (`kind` + `file`), grouped per version."""
    versions: dict[int, list[tuple[Meta, Comment]]] = {}
    for comment in comments:
        meta = parse_meta(comment.body)
        if meta is not None and meta.kind == kind and meta.file == file:
            versions.setdefault(meta.version, []).append((meta, comment))
    return versions


def _complete(parts: list[tuple[Meta, Comment]]) -> bool:
    """All parts have the same `parts`, and `part` is exactly `1..parts`."""
    total = parts[0][0].parts
    numbers = sorted(meta.part for meta, _ in parts)
    return all(meta.parts == total for meta, _ in parts) and numbers == list(
        range(1, total + 1)
    )


def current(comments: list[Comment], kind: str, file: str) -> list[Comment]:
    """The parts of the highest complete version of the chain, sorted on `part`.

    Incomplete versions (a post that failed halfway) do not count. `[]` when the chain
    has no complete version.
    """
    versions = _versions(comments, kind, file)
    for version in sorted(versions, reverse=True):
        if _complete(versions[version]):
            ordered = sorted(versions[version], key=lambda pair: pair[0].part)
            return [comment for _, comment in ordered]
    return []


def next_version(comments: list[Comment], kind: str, file: str) -> int:
    """`max(versions of the chain, incomplete ones too) + 1`; 1 for an empty chain."""
    return max(_versions(comments, kind, file), default=0) + 1

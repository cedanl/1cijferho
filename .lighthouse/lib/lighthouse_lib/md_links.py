# lighthouse: core@v0.8.1
"""One link extraction for markdown (#886): the norm and the plumbing together.

Norm: a link in a file is relative and points to an existing target;
an absolute link is always a problem (see AGENTS.md, rule "Links:
file relative, chat absolute").

Moved/merged from two earlier, slightly different implementations:
- the `_strip_code_fences`/`iter_relative_links` helpers in
  `tests/unit/test_docs_planning.py` (fence and inline-code logic);
- the Python heredoc in `md-link-check.sh` (link regex, anchor/scheme/
  absolute filter, path resolution against `root`).

Used by `md-link-check.sh` (task 5, through the CLI below) and by the
docs link tests (task 6, through `strip_code`, `relative_links` and
`absolute_links`), so that norm is no longer maintained separately in two
places.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote

# Three backticks/tildes as a runtime string, never as literal text in this
# file: the old link hook blocks an Edit/Write with three backticks on one
# line.
FENCE = "`" * 3
TILDE_FENCE = "~" * 3
_FENCE = re.compile(r"^\s*(" + re.escape(FENCE) + "|" + re.escape(TILDE_FENCE) + ")")
# Inline code span: `...`. A path between backticks is documentation, not a link.
_INLINE_CODE = re.compile(r"`[^`\n]*`")

# A link follows CommonMark (#1014): the destination may sit between angle
# brackets (then with spaces) and the title has three forms: "..", '..' and (..).
LINK = re.compile(
    r"\[[^\]]*\]\("
    r"(?:<(?P<angle>[^<>\n]*)>|(?P<bare>[^)\s<][^)\s]*))"
    r"(?:\s+(?:\"[^\"]*\"|'[^']*'|\([^)]*\)))?"
    r"\s*\)"
)
_SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


def link_target(match: re.Match[str]) -> str:
    """The path of one `LINK` match, without any angle brackets."""
    angle = match.group("angle")
    return angle if angle is not None else match.group("bare")


def strip_code(text: str) -> str:
    """Remove fenced blocks (three backticks or three tildes) and inline code.

    Fences toggle per line: a line that, after whitespace, begins with the
    fence character flips the state and does not count itself. A fence
    character in the MIDDLE of a line (e.g. in a string literal) toggles
    nothing.
    """
    out = []
    inside = False
    for line in text.splitlines():
        if _FENCE.match(line):
            inside = not inside
            continue
        if not inside:
            out.append(_INLINE_CODE.sub("", line))
    return "\n".join(out)


def relative_links(text: str) -> list[str]:
    """Targets of relative markdown links, outside code, deduplicated in order.

    Excludes: a bare anchor (`#kop`), a target that begins with `/` (absolute)
    and a target with a scheme (`https:`, `mailto:`, `obsidian:`, ...). An
    anchor on an otherwise relative target (`b.md#kop`) is cut off to `b.md`.
    """
    seen: set[str] = set()
    out: list[str] = []
    for match in LINK.finditer(strip_code(text)):
        target = link_target(match)
        if target.startswith(("#", "/")):
            continue
        if _SCHEME.match(target):
            continue
        target = target.split("#", 1)[0]
        if not target or target in seen:
            continue
        seen.add(target)
        out.append(target)
    return out


def absolute_links(text: str) -> list[str]:
    """Targets of markdown links that begin with `/`, outside code."""
    return [
        link_target(match)
        for match in LINK.finditer(strip_code(text))
        if link_target(match).startswith("/")
    ]


def find_problems(md_path: Path, root: Path) -> list[tuple[str, str]]:
    """Problems in `md_path`: dead relative links and every absolute link.

    A relative target is, after URL decoding, resolved against
    `md_path.parent` and tested for existence (file or directory). If the
    result falls outside `root`, this function skips it: that is no problem of
    this document. An absolute target is always a problem, whether it exists
    or not.

    An `OSError` while reading `md_path` goes through to the caller.
    """
    text = md_path.read_text(encoding="utf-8")
    problems: list[tuple[str, str]] = []

    for target in relative_links(text):
        resolved = os.path.normpath(os.path.join(md_path.parent, unquote(target)))
        rel = os.path.relpath(resolved, root)
        if rel.startswith(".."):
            continue
        if not os.path.exists(resolved):
            problems.append(("dead", target))

    for target in absolute_links(text):
        problems.append(("absolute", target))

    return problems


def main(argv: list[str] | None = None) -> int:
    """CLI: `md_links.py <md_path> <root>`.

    Prints one tab-separated line per problem found (`<kind>\\t<target>`) on
    stdout and returns 0. On an `OSError` (e.g. an unreadable file) the
    message goes to stderr and the exit code is 1.
    """
    args = sys.argv[1:] if argv is None else argv
    md_path, root = Path(args[0]), Path(args[1])
    try:
        problems = find_problems(md_path, root)
    except OSError as exc:
        print(f"md_links: could not read {md_path}: {exc}", file=sys.stderr)
        return 1
    for kind, target in problems:
        print(f"{kind}\t{target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

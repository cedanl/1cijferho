# lighthouse: core@v0.8.1
"""Find Dutch text and names in code (#26, #37).

Detection only: callers decide where to look (comments, docstrings, names). The
judgement per name or block is layer 1: `code_language_score` compares how often each
word occurs in English and in Dutch. A unit is a finding, a doubt (not a finding; a
later layer may look at it) or clean. Layer 2 (`code_language_llm`, `resolve`) asks a
chain of models about the doubts only; it can add findings, never remove one.
"""

from __future__ import annotations

import ast
import fnmatch
import io
import os
import re
import sys
import tokenize
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import config
from lighthouse_lib import code_language_llm
from lighthouse_lib import code_language_score
from lighthouse_lib.code_language_score import split_name


MARKER = "lighthouse: allow-language"

# Set by bash_writes.run_gate for every gate it runs: layer 2 is skipped there (decision 19).
UNDER_GATES_ENV = "LIGHTHOUSE_UNDER_BASH_WRITE_CHECK"


class Finding(NamedTuple):
    line: int
    kind: str  # "comment", "docstring", "prose", "name" or "directory"
    text: str  # the name, or the first line of the block
    layer: str = "layer 1"
    doubt: bool = False  # true: a doubt, not a finding
    unit: str = ""  # the whole unit for a later layer; empty for a name


class Inspection(NamedTuple):
    findings: list[Finding]
    doubts: list[Finding]
    chain: code_language_llm.Chain | None = None  # `[code_language_llm]`; None when absent


class CannotCheck(Exception):
    """The file could not be read as source; the message is the reason."""


def _comment_findings(source: str) -> list[Finding]:
    blocks: list[list[tuple[int, str]]] = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type != tokenize.COMMENT:
            continue
        line, text = token.start[0], token.string.lstrip("#").strip()
        if blocks and blocks[-1][-1][0] == line - 1:
            blocks[-1].append((line, text))
        else:
            blocks.append([(line, text)])
    return _judge_blocks(blocks)


def _judge_blocks(blocks: list[list[tuple[int, str]]]) -> list[Finding]:
    """A finding or doubt on the first line of each comment block, judged outside marked lines.

    Every comment line is a sentence boundary: comments often end without a full stop.
    """
    found = []
    for block in blocks:
        unit = "\n".join(text for _, text in block if MARKER not in text)
        verdict = code_language_score.judge_text(unit, line_breaks=True)
        if verdict != "clean":
            found.append(
                Finding(block[0][0], "comment", block[0][1], doubt=verdict == "doubt", unit=unit)
            )
    return found


def _name_nodes(tree: ast.AST):
    """Yield `(line, name)` for every place a name comes into being in this file."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            yield node.lineno, node.name
        elif isinstance(node, ast.arg):
            yield node.lineno, node.arg
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            yield node.lineno, node.id
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store):
            yield node.end_lineno or node.lineno, node.attr
        elif isinstance(node, ast.alias) and node.asname:
            yield node.lineno, node.asname
        elif isinstance(node, ast.ExceptHandler) and node.name:
            yield node.lineno, node.name


def scan_python(source: str) -> list[Finding]:
    """Findings and doubts for comments, docstrings and names in Python `source`, by line."""
    try:
        tree = ast.parse(source)
        found = _comment_findings(source)
    except (tokenize.TokenError, SyntaxError, ValueError) as error:
        # IndentationError is a SyntaxError; Python 3.11 raises ValueError on a NUL byte.
        raise CannotCheck(f"cannot parse Python: {error}") from error
    lines = source.splitlines()

    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            continue
        doc = ast.get_docstring(node)
        if doc is None or MARKER in doc:
            continue
        verdict = code_language_score.judge_text(doc)
        if verdict != "clean":
            found.append(
                Finding(
                    node.body[0].lineno,
                    "docstring",
                    doc.splitlines()[0],
                    doubt=verdict == "doubt",
                    unit=doc,
                )
            )

    found += _name_findings(list(_name_nodes(tree)), lines)
    return sorted(found, key=lambda f: f.line)


_IDENT = r"[A-Za-z_]\w*"
_IDENT_LIST = rf"{_IDENT}(?:[ \t]*,[ \t]*{_IDENT})*"


def _name_findings(
    names: list[tuple[int, str]], source_lines: list[str]
) -> list[Finding]:
    """Findings and doubts for names; one per name per line, none on a marked line."""
    found = []
    seen = set()
    for line, name in names:
        if (line, name) in seen:
            continue
        seen.add((line, name))
        marked = 0 < line <= len(source_lines) and MARKER in source_lines[line - 1]
        verdict = "clean" if marked else code_language_score.judge_name(name)
        if verdict != "clean":
            found.append(Finding(line, "name", name, doubt=verdict == "doubt"))
    return found


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


# --- shell ---

_HEREDOC = re.compile(rf"<<(-?)[ \t]*(?:'([^']*)'|\"([^\"]*)\"|({_IDENT}))")
_SHELL_FUNCTION = re.compile(rf"^[ \t]*function[ \t]+({_IDENT})", re.MULTILINE)
_SHELL_FUNCTION_PARENS = re.compile(rf"^[ \t]*({_IDENT})[ \t]*\([ \t]*\)", re.MULTILINE)
_SHELL_ASSIGN = re.compile(rf"(?:^|[;&|(])[ \t]*({_IDENT})\+?=", re.MULTILINE)
_SHELL_DECLARE = re.compile(r"^[ \t]*(?:local|export|readonly|declare|typeset)\b", re.MULTILINE)
_SHELL_DECLARED = re.compile(rf"[ \t]+(?:-\S+|({_IDENT})(?=[ \t=\n]|$))(?:=\S*)?")


def _scan_shell_text(source: str) -> tuple[list[str], list[list[tuple[int, str]]]]:
    """Code lines with strings, comments and heredoc bodies blanked, plus comment blocks."""
    code_lines: list[str] = []
    blocks: list[list[tuple[int, str]]] = []
    last_comment = -1
    quote = ""
    heredoc: tuple[str, bool] | None = None
    pending: list[tuple[str, bool]] = []
    for number, line in enumerate(source.splitlines(), 1):
        if heredoc:
            body = line.rstrip("\r")
            if (body.lstrip("\t") if heredoc[1] else body) == heredoc[0]:
                heredoc = pending.pop(0) if pending else None
            code_lines.append("")
            continue
        out = []
        i = 0
        while i < len(line):
            char = line[i]
            if quote:
                if char == "\\" and quote == '"':
                    i += 1
                elif char == quote:
                    quote = ""
                    out.append(char)
            elif char == "\\":
                i += 1
            elif char in "'\"":
                quote = char
                out.append(char)
            elif char == "#" and (i == 0 or line[i - 1] in " \t;&|("):
                if not (number == 1 and line.startswith("#!")):
                    text = line[i + 1 :].strip()
                    if last_comment == number - 1 and blocks:
                        blocks[-1].append((number, text))
                    else:
                        blocks.append([(number, text)])
                    last_comment = number
                break
            elif line.startswith("<<", i) and "((" not in line[:i]:
                match = _HEREDOC.match(line, i)
                if match and not line.startswith("<<<", i):
                    delimiter = match[2] or match[3] or match[4]
                    pending.append((delimiter, bool(match[1])))
                    i = match.end() - 1
                out.append(char)
            else:
                out.append(char)
            i += 1
        code_lines.append("".join(out))
        if pending and not quote:
            heredoc = pending.pop(0)
    return code_lines, blocks


def scan_shell(source: str) -> list[Finding]:
    """Findings and doubts for comments and names in shell `source`, by line.

    A heuristic reader, not a parser: it skips quoted strings and heredoc bodies and
    looks for function names and assigned variables.
    """
    code_lines, blocks = _scan_shell_text(source)
    code = "\n".join(code_lines)
    names = []
    for pattern in (_SHELL_FUNCTION, _SHELL_FUNCTION_PARENS, _SHELL_ASSIGN):
        names += [(_line_of(code, m.start(1)), m[1]) for m in pattern.finditer(code)]
    for declare in _SHELL_DECLARE.finditer(code):
        position = declare.end()
        while match := _SHELL_DECLARED.match(code, position):
            position = match.end()
            if match[1]:
                names.append((_line_of(code, match.start(1)), match[1]))
    found = _judge_blocks(blocks) + _name_findings(names, source.splitlines())
    return sorted(found, key=lambda f: f.line)


# --- Go ---

_GO_PACKAGE = re.compile(rf"^[ \t]*package[ \t]+({_IDENT})", re.MULTILINE)
_GO_FUNC_NAMED = re.compile(
    rf"^[ \t]*func[ \t]+(?:\(([^)]*)\)[ \t]*)?({_IDENT})[ \t]*(?:\[[^\]]*\][ \t]*)?\(([^)]*)\)",
    re.MULTILINE,
)
_GO_FUNC_PARAMS = re.compile(r"\bfunc[ \t]*\(([^)]*)\)")
_GO_DECLARATION = re.compile(rf"^[ \t]*(?:var|const|type)[ \t]+({_IDENT_LIST})", re.MULTILINE)
_GO_GROUP = re.compile(r"^[ \t]*(?:var|const|type)[ \t]*\(", re.MULTILINE)
_GO_DEFINE = re.compile(rf"(?<![\w.])({_IDENT_LIST})[ \t]*:=")
_GO_STRUCT = re.compile(r"\bstruct[ \t]*\{")
_GO_FIELD = re.compile(rf"^[ \t]*({_IDENT_LIST})[ \t]+\S")


def _scan_go_text(source: str) -> tuple[str, list[list[tuple[int, str]]]]:
    """Source with comments, strings and runes blanked (same offsets), plus comment blocks."""
    out = list(source)
    blocks: list[list[tuple[int, str]]] = []
    last_line_comment = -1  # line of the last `//` comment, when it ended the last block
    line = 1
    i = 0
    size = len(source)

    def blank(start: int, end: int) -> None:
        for k in range(start, end):
            if out[k] != "\n":
                out[k] = " "

    while i < size:
        char = source[i]
        if char == "\n":
            line += 1
        elif source.startswith("//", i):
            end = source.find("\n", i)
            end = size if end < 0 else end
            text = source[i + 2 : end].strip()
            if last_line_comment == line - 1 and blocks:
                blocks[-1].append((line, text))
            else:
                blocks.append([(line, text)])
            last_line_comment = line
            blank(i, end)
            i = end
            continue
        elif source.startswith("/*", i):
            end = source.find("*/", i + 2)
            end = size if end < 0 else end + 2
            body = source[i + 2 : max(i + 2, end - 2)]
            parts = [part.strip().lstrip("*").strip() for part in body.split("\n")]
            blocks.append([(line + n, part) for n, part in enumerate(parts)])
            last_line_comment = -1
            line += source.count("\n", i, end)
            blank(i, end)
            i = end
            continue
        elif char in "\"'`":
            end = i + 1
            while end < size and source[end] != char:
                if source[end] == "\n" and char != "`":
                    break
                end += 2 if source[end] == "\\" and char != "`" else 1
            end = min(end + 1, size)
            blank(i + 1, end - 1)
            line += source.count("\n", i, end)
            i = end
            continue
        i += 1
    return "".join(out), blocks


def _go_param_names(group: str, start: int, code: str) -> list[tuple[int, str]]:
    """Parameter names in the text of one parameter list that begins at offset `start`."""
    pieces = [(m.start(), m[0].split()) for m in re.finditer(r"[^,]+", group)]
    if not any(len(tokens) > 1 for _, tokens in pieces):
        return []  # only types, no names
    return [
        (_line_of(code, start + offset + group[offset:].find(tokens[0])), tokens[0])
        for offset, tokens in pieces
        if tokens and re.fullmatch(_IDENT, tokens[0])
    ]


def _go_names(code: str) -> list[tuple[int, str]]:
    names: list[tuple[int, str]] = []

    def add_list(match: re.Match[str], group: int = 1) -> None:
        for name in re.finditer(_IDENT, match[group]):
            names.append((_line_of(code, match.start(group) + name.start()), name[0]))

    for match in _GO_PACKAGE.finditer(code):
        add_list(match)
    for match in _GO_FUNC_NAMED.finditer(code):
        names.append((_line_of(code, match.start(2)), match[2]))
        if match[1] is not None:
            names += _go_param_names(match[1], match.start(1), code)
        names += _go_param_names(match[3], match.start(3), code)
    for match in _GO_FUNC_PARAMS.finditer(code):
        names += _go_param_names(match[1], match.start(1), code)
    for match in _GO_DECLARATION.finditer(code):
        add_list(match)
    for match in _GO_DEFINE.finditer(code):
        add_list(match)
    for group in _GO_GROUP.finditer(code):
        depth, end = 1, group.end()
        while end < len(code) and depth:
            depth += {"(": 1, ")": -1}.get(code[end], 0)
            end += 1
        for entry in re.finditer(rf"^[ \t]*({_IDENT_LIST})", code[group.end() : end], re.MULTILINE):
            base = group.end() + entry.start(1)
            for name in re.finditer(_IDENT, entry[1]):
                names.append((_line_of(code, base + name.start()), name[0]))
    for struct in _GO_STRUCT.finditer(code):
        depth, end = 1, struct.end()
        while end < len(code) and depth:
            depth += {"{": 1, "}": -1}.get(code[end], 0)
            end += 1
        body = code[struct.end() : end - 1]
        for field in re.finditer(r"[^\n;]+", body):
            match = _GO_FIELD.match(field[0])
            if match:
                base = struct.end() + field.start() + match.start(1)
                for name in re.finditer(_IDENT, match[1]):
                    names.append((_line_of(code, base + name.start()), name[0]))
    return names


def scan_go(source: str) -> list[Finding]:
    """Findings and doubts for comments and names in Go `source`, by line.

    A heuristic reader, not a parser: it blanks comments, strings, raw strings and runes
    first, then looks for the declaration forms of Go.
    """
    code, blocks = _scan_go_text(source)
    found = _judge_blocks(blocks) + _name_findings(_go_names(code), source.splitlines())
    return sorted(found, key=lambda f: f.line)


# --- Markdown ---

_FENCE = re.compile(r"^\s*(```|~~~)")
_HEADING = re.compile(r"^#{1,6}[ \t]")
_LIST_ITEM = re.compile(r"^\s*(?:[-*+]|\d+[.)])[ \t]+(.*)")
_TABLE_SEPARATOR = re.compile(r"^[\s|:-]+$")
_FRONT_KEY = re.compile(r"^([\w-]+):[ \t]*(.*)$")
_FOLDED = {"", ">", "|", ">-", "|-"}
_HTML_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_URL_IN_ANGLES = re.compile(r"<https?://[^>]*>")
_IMAGE = re.compile(r"!\[([^\]]*)\]\([^)]*\)")
_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_HTML_TAG = re.compile(r"</?[A-Za-z][^>]*>")


def _prose_unit(lines: list[tuple[int, str]]) -> Finding | None:
    """A finding or doubt for one unit of `(line, text)`; marked lines do not count."""
    kept = [(number, text) for number, text in lines if MARKER not in text]
    text = " ".join(text.strip() for _, text in kept)
    text = _HTML_COMMENT.sub(" ", text)
    text = _URL_IN_ANGLES.sub(" ", text)
    text = _IMAGE.sub(r"\1", text)
    text = _LINK.sub(r"\1", text)
    text = _HTML_TAG.sub(" ", text).strip()
    if not kept or not text:
        return None
    verdict = code_language_score.judge_text(text)
    if verdict == "clean":
        return None
    return Finding(kept[0][0], "prose", text, doubt=verdict == "doubt", unit=text)


def _frontmatter_units(lines: list[str]) -> tuple[list[list[tuple[int, str]]], int]:
    """Units of the frontmatter and the index of the first body line."""
    if not lines or lines[0].strip() != "---":
        return [], 0
    units: list[list[tuple[int, str]]] = []
    for index in range(1, len(lines)):
        line, number = lines[index], index + 1
        if line.strip() == "---":
            return units, index + 1
        if not line.strip():
            continue
        key = _FRONT_KEY.match(line)
        if line[0] in " \t" and units:
            units[-1].append((number, line))
        elif key:
            value = key[2].strip().strip("\"'")
            units.append([] if key[2].strip() in _FOLDED else [(number, value)])
            if key[2].strip() in _FOLDED:
                units[-1].append((number, ""))
        elif line.lstrip().startswith("- "):
            units.append([(number, line.lstrip()[2:].strip().strip("\"'"))])
        elif units:
            units[-1].append((number, line))
    return units, len(lines)  # never closed: all of it was frontmatter


def scan_markdown(source: str) -> list[Finding]:
    """Findings and doubts for the prose of Markdown `source`, by line.

    A line scanner, not a parser. Code (fenced or indented blocks, inline code), link
    targets, URLs and HTML are not language. Frontmatter values, headings, list items,
    table cells and paragraphs are the units.
    """
    lines = source.splitlines()
    units, start = _frontmatter_units(lines)
    paragraph: list[tuple[int, str]] = []
    fence = ""
    in_list = False
    in_code = False
    previous_blank = True

    def flush() -> None:
        nonlocal paragraph
        if paragraph:
            units.append(paragraph)
        paragraph = []

    for index in range(start, len(lines)):
        line, number = lines[index], index + 1
        blank = not line.strip()
        if fence:
            if line.strip().startswith(fence):
                fence = ""
            continue
        if in_code:
            if blank or line[0] in " \t":
                continue
            in_code = False
        opening = _FENCE.match(line)
        if opening:
            flush()
            fence = opening[1]
            continue
        if blank:
            flush()
            previous_blank = True
            continue
        if previous_blank and not in_list and (line.startswith("    ") or line[0] == "\t"):
            in_code = True
            continue
        previous_blank = False
        item = _LIST_ITEM.match(line)
        if _HEADING.match(line):
            flush()
            units.append([(number, line.lstrip("#"))])
            in_list = False
        elif item:
            flush()
            paragraph = [(number, item[1])]
            in_list = True
        elif line.lstrip().startswith("|"):
            flush()
            if not _TABLE_SEPARATOR.match(line):
                cells = line.strip().strip("|").split("|")
                units += [[(number, cell)] for cell in cells if cell.strip()]
        else:
            if not (line[0] in " \t" and in_list):
                in_list = False
            paragraph.append((number, line))
    flush()
    found = [unit for unit in map(_prose_unit, units) if unit]
    return sorted(found, key=lambda f: f.line)


# --- checking a file ---

EXTENSIONS: dict[str, Callable[[str], list[Finding]]] = {
    ".py": scan_python,
    ".sh": scan_shell,
    ".bash": scan_shell,
    ".go": scan_go,
}

MARKDOWN_EXTENSIONS = (".md", ".markdown")

_MAX_LINES = 20
_TEXT_LIMIT = 60


def _managed_paths(root: Path) -> set[str]:
    import tomllib  # lazily, like config._read_toml: Python < 3.11 must still import this module

    manifest = root / ".lighthouse" / "manifest.toml"
    if not manifest.is_file():
        return set()
    try:
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        return {unit["path"] for unit in data.get("unit", [])}
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        raise CannotCheck(f"cannot read {manifest}: {error}") from error


def _fixed_start(glob: str) -> list[str]:
    """The segments of `glob` before the first wildcard; with none, all but the file name."""
    segments = glob.split("/")
    for index, segment in enumerate(segments):
        if any(char in segment for char in "*?["):
            return segments[:index]
    return segments[:-1]


def scope_dirs(relative: str, globs: list[str]) -> list[str] | None:
    """The directory names of `relative` that a glob in `globs` lets through.

    `None` when no glob covers the path. Otherwise the directories between the fixed
    start of each covering glob and the file, as a union in path order. The fixed start
    is the project's own layout and is not judged.
    """
    parts = relative.split("/")[:-1]
    positions: set[int] = set()
    covered = False
    for glob in globs:
        if fnmatch.fnmatchcase(relative, glob):
            covered = True
            positions.update(range(len(_fixed_start(glob)), len(parts)))
    if not covered:
        return None
    names: list[str] = []
    for position in sorted(positions):
        if parts[position] not in names:
            names.append(parts[position])
    return names


def _directory_findings(names: list[str]) -> list[Finding]:
    found = []
    for name in names:
        verdict = code_language_score.judge_name(name)
        if verdict != "clean":
            found.append(Finding(1, "directory", name, doubt=verdict == "doubt"))
    return found


def _session_chain(session_toml: Path | None) -> code_language_llm.Chain | None:
    """`[code_language_llm]` from the session repo's `lighthouse.toml`; None without one.

    Never from the written file's repo: that repo would choose where the session's key
    goes (`url`, `key_env`, `env_file`).
    """
    if session_toml is None or not session_toml.is_file():
        return None
    try:
        return code_language_llm.chain_from(config.load(session_toml))
    except config.ConfigError as error:
        raise CannotCheck(str(error)) from error


def check_file(path: Path, root: Path, session_toml: Path | None = None) -> Inspection:
    """Findings and doubts for the file at `path` (absolute) in repo `root`.

    Both empty when the file is not checked: no `lighthouse.toml`, no `code_language`,
    an exempt glob, a file `lighthouse init` manages, or a file that is neither code nor
    inside `code_language_paths`. A code file is read as code, a Markdown file inside the
    scope as prose; any other file inside the scope counts by name only. Scope, exemptions
    and the on-switch come from `root`; the layer 2 chain only from `session_toml`, the
    `lighthouse.toml` of the repo the hook runs for.
    """
    nothing = Inspection([], [])
    toml = root / "lighthouse.toml"
    if not toml.is_file():
        return nothing
    try:
        cfg = config.load(toml)
        try:
            config.lookup(cfg, "project.code_language")
        except KeyError:
            return nothing
        exempt, paths = [], []
        try:
            exempt = config.lookup(cfg, "project.code_language_exempt")
        except KeyError:
            pass
        try:
            paths = config.lookup(cfg, "project.code_language_paths")
        except KeyError:
            pass
    except config.ConfigError as error:
        raise CannotCheck(str(error)) from error
    relative = path.relative_to(root).as_posix()
    scan = EXTENSIONS.get(path.suffix)
    dirs = scope_dirs(relative, paths)
    if scan is None and dirs is None:
        return nothing
    if any(fnmatch.fnmatchcase(relative, glob) for glob in exempt):
        return nothing
    if relative in _managed_paths(root):
        return nothing
    # Read for every checked file, doubts or not: a wrong table is COULD NOT CHECK at once,
    # and only then: a file that is not checked never trips over the session's table.
    chain = _session_chain(session_toml)
    if scan is None and path.suffix in MARKDOWN_EXTENSIONS:
        scan = scan_markdown
    found: list[Finding] = []
    try:
        if scan is not None:
            try:
                source = path.read_text(encoding="utf-8-sig")  # a BOM is not content
            except UnicodeDecodeError as error:
                raise CannotCheck(f"{path} is not valid UTF-8: {error}") from error
            found += scan(source)
        found += _directory_findings(dirs or [])
        found += _name_findings([(1, path.stem)], [])
    except code_language_score.DataError as error:
        raise CannotCheck(str(error)) from error
    found.sort(key=lambda f: f.line)
    return Inspection(
        [f for f in found if not f.doubt],
        [f for f in found if f.doubt],
        chain,
    )


def _line(path: Path, finding: Finding) -> str:
    """`<path>:<line>: <kind>: <text>`, a block cut to `_TEXT_LIMIT` characters."""
    text = finding.text
    if finding.kind != "name" and len(text) > _TEXT_LIMIT:
        text = text[:_TEXT_LIMIT]
    return f"{path}:{finding.line}: {finding.kind}: {text}"


def _notice(path: Path, doubts: list[Finding], reasons: str, *, prefix: str = "") -> str:
    """The notice of decision 11: layer 2 gave no verdict, so these doubts stay unchecked."""
    lines = [
        f"code-language-check: {prefix}layer 2 did not answer ({reasons}); "
        f"{len(doubts)} doubtful units not checked:"
    ]
    lines += [_line(path, doubt) for doubt in doubts]
    return "\n".join(lines)


def resolve(
    inspection: Inspection, path: Path, *, under_gates: bool, state_dir: Path
) -> tuple[list[Finding], str | None]:
    """All findings (layer 1, plus each doubt layer 2 calls NOT-EN) and the notice, or None.

    No doubts or no chain: layer 1 alone, silently (decision 12). Under bash-write-check
    layer 2 does not run and the doubts go into a `NOTICE` (decision 19). Layer 2 only
    hears the doubts, never a layer 1 finding (decision 10).
    """
    found = list(inspection.findings)
    doubts = inspection.doubts
    if not doubts or inspection.chain is None:
        return found, None
    if under_gates:
        reason = "layer 2 skipped under bash-write-check"
        return found, _notice(path, doubts, reason, prefix="NOTICE — ")
    reply = code_language_llm.ask(
        inspection.chain, [(d.kind, d.unit or d.text) for d in doubts], state_dir=state_dir
    )
    if reply.not_english is None:
        return found, _notice(path, doubts, "; ".join(reply.failures))
    layer = f"layer 2 ({reply.model})"
    found += [
        d._replace(layer=layer, doubt=False)
        for d, not_english in zip(doubts, reply.not_english)
        if not_english
    ]
    found.sort(key=lambda f: f.line)
    return found, None


def _report(path: Path, found: list[Finding]) -> str:
    lines = [f"code-language-check: {path} is not English"]
    for finding in found[:_MAX_LINES]:
        lines.append(f"{_line(path, finding)} [{finding.layer}]")
    if len(found) > _MAX_LINES:
        lines.append(f"… and {len(found) - _MAX_LINES} more")
    lines.append(
        "The whole file must be English. A line that is deliberately not English "
        f'gets "{MARKER}".'
    )
    if any(f.line == 1 and f.kind == "name" and f.text == path.stem for f in found):
        lines.append(
            f"The module name '{path.stem}' is not English either; the marker cannot clear it: "
            "rename the file or add it to `code_language_exempt`."
        )
    for finding in found:
        if finding.kind == "directory":
            lines.append(
                f"The directory name '{finding.text}' is not English either; the marker cannot "
                "clear it: rename the file or its directory, or add the file to "
                "`code_language_exempt`."
            )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    """`code_language.py <abs-path> <root>`; the chain from `config.config_path()`.

    rc 0 with empty stdout: clean. rc 0 with stdout: only a notice. rc 1: findings on
    stdout, the notice below them when there is one. rc 2: could not check.
    """
    try:
        path, root = Path(argv[0]), Path(argv[1])
        found, notice = resolve(
            check_file(path, root, config.config_path()),
            path,
            under_gates=bool(os.environ.get(UNDER_GATES_ENV)),
            state_dir=code_language_llm.default_state_dir(),
        )
    except Exception as error:  # a crash must never read as findings (rc 1)
        print(f"code-language-check: COULD NOT CHECK — {error}", file=sys.stderr)
        return 2
    if found:
        print(_report(path, found))
        if notice:
            print(f"\n{notice}")
        return 1
    if notice:
        print(notice)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

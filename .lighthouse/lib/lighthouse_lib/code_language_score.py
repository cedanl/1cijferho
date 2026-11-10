# lighthouse: core@v0.8.1
"""Layer 1 of the code-language detector (#37): score a name or a text per word.

Every word gets `(nl - en) / 10` from `code_language_freq.tsv.gz` (word frequencies
from wordfreq, zipf x 10). A unit is a finding when the sum of its words is above
`THRESHOLD`. Prose is also judged per sentence, so one Dutch sentence is not diluted
by English around it. A word in neither language counts as Dutch when it splits into
Dutch words (`dutch_split`) or is the Dutch participle of an English verb
(`dutch_participle`). Standard library only.
"""

from __future__ import annotations

import functools
import gzip
import re
import zlib
from collections.abc import Mapping
from pathlib import Path

FREQ_FILE = "code_language_freq.tsv.gz"
JARGON_FILE = "code_language_jargon.txt"

THRESHOLD = 1.0  # a unit is a finding when its sum is strictly above this
COMPOUND_SCORE = 3.0  # score of an unlisted word that splits into Dutch words
PART_MIN = 3  # letters of a part of a compound
PART_NL_FLOOR = 2.5  # zipf NL a part of a compound needs
DOUBT_LEAN = 2.0  # zipf NL - EN of a word that makes a unit doubtful
DOUBT_NL = 3.0  # zipf NL of a word that makes a unit doubtful
NAME_MIN = 4  # shortest word counted in a name
SPLIT_MAX = 40  # longest word `dutch_split` tries; a longer run of letters is not a compound
TEXT_MIN = 2  # shortest word counted in prose

# An acronym run ("HTTP" in "HTTPServer") or a capitalised or lower-case word.
_NAME_PART = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+")
_WORD = re.compile(r"[^\W\d_]+")
_PROSE_NOISE = re.compile(r"`[^`]*`|https?://\S+")
# A sentence ends at `.`, `!` or `?` before whitespace; not in "e.g." or "i.e.", and a
# period inside a token ("config.py", "1.2") has no whitespace after it.
_SENTENCE_END = r"(?<=[.!?])(?<!\.[^\W\d_]\.)\s+"
_SENTENCE = re.compile(_SENTENCE_END)
_SENTENCE_OR_LINE = re.compile(_SENTENCE_END + r"|\n")


class DataError(Exception):
    """A data file is missing or unreadable; the message is the reason."""


def split_name(name: str) -> list[str]:
    """Split an identifier on `_`, digits and case changes; lower-case, no empty parts."""
    return [part.lower() for part in _NAME_PART.findall(name)]


def words(text: str) -> list[str]:
    """The words of `text`: runs of letters (accents included), lower-case."""
    return _WORD.findall(text.lower())


def _data_path(filename: str) -> Path:
    return Path(__file__).with_name(filename)


@functools.cache
def frequencies() -> Mapping[str, tuple[int, int]]:
    """`word -> (en, nl)`, zipf x 10, loaded once per process."""
    path = _data_path(FREQ_FILE)
    table: dict[str, tuple[int, int]] = {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                word, en, nl = line.rstrip("\n").split("\t")
                table[word] = (int(en), int(nl))
    except (OSError, EOFError, UnicodeDecodeError, ValueError, zlib.error) as error:
        # gzip.BadGzipFile is an OSError; a corrupt deflate stream raises zlib.error.
        raise DataError(f"cannot read {path.name}: {error}") from error
    return table


@functools.cache
def jargon() -> frozenset[str]:
    """English code jargon that looks Dutch, one word per line; `#` starts a comment."""
    path = _data_path(JARGON_FILE)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as error:
        raise DataError(f"cannot read {path.name}: {error}") from error
    entries = (line.strip().lower() for line in lines)
    return frozenset(e for e in entries if e and not e.startswith("#"))


def _is_dutch_part(part: str, table: Mapping[str, tuple[int, int]]) -> bool:
    entry = table.get(part)
    if entry is None or len(part) < PART_MIN:
        return False
    en, nl = entry
    return nl / 10 >= PART_NL_FLOOR and nl > en


def dutch_split(word: str) -> bool:
    """True when `word` splits into two or more Dutch words, even if an English split exists.

    A word longer than `SPLIT_MAX` letters is never split: that bounds the recursion depth
    and the work per word.
    """
    if len(word) > SPLIT_MAX:
        return False
    table = frequencies()
    memo: dict[str, bool] = {}

    def splits(rest: str) -> bool:
        if rest not in memo:
            memo[rest] = any(
                _is_dutch_part(rest[:i], table)
                and (_is_dutch_part(rest[i:], table) or splits(rest[i:]))
                for i in range(PART_MIN, len(rest) - PART_MIN + 1)
            )
        return memo[rest]

    return splits(word)


def dutch_participle(word: str) -> bool:
    """True when `word` is `ge` + an English past form in `-ed` ("geparsed", "gemerged").

    Dutch builds the participle of a loan verb with `ge-`; the rest must be a listed word
    that leans English, so a mere `ge` start ("getout") does not count.
    """
    if not word.startswith("ge"):
        return False
    rest = word[2:]
    entry = frequencies().get(rest)
    return entry is not None and rest.endswith("ed") and entry[0] > entry[1]


def word_score(word: str) -> float:
    """`(nl - en) / 10`; a word in neither list scores `COMPOUND_SCORE` if it is a Dutch
    compound or a Dutch participle of an English verb."""
    entry = frequencies().get(word)
    if entry is None:
        return COMPOUND_SCORE if dutch_split(word) or dutch_participle(word) else 0.0
    en, nl = entry
    return (nl - en) / 10


def _judge(candidates: list[str], minimum: int, sentences: tuple[list[str], ...] = ()) -> str:
    """The words of a unit, and of each of its sentences: one sum above `THRESHOLD` is enough.

    No minimum number of words per sentence: a two-word Dutch line is a finding (PR #72).
    """
    skip = jargon()

    def counted(group: list[str]) -> list[str]:
        return [w for w in group if len(w) >= minimum and w not in skip]

    if any(sum(map(word_score, counted(g))) > THRESHOLD for g in (candidates, *sentences)):
        return "finding"
    counted_words = counted(candidates)
    table = frequencies()
    for w in counted_words:
        if w in table:
            en, nl = table[w]
            if (nl - en) / 10 >= DOUBT_LEAN and nl / 10 >= DOUBT_NL:
                return "doubt"
    return "clean"


def judge_name(name: str) -> str:
    """`"finding"`, `"doubt"` or `"clean"` for an identifier or a file name."""
    return _judge(split_name(name), NAME_MIN)


def judge_text(text: str, *, line_breaks: bool = False) -> str:
    """`"finding"`, `"doubt"` or `"clean"` for prose; inline code and URLs do not count.

    The text as a whole and each of its sentences can make a finding. With `line_breaks`
    every line is a boundary too: the lines of a comment block, not wrapped prose.
    """
    text = _PROSE_NOISE.sub(" ", text)
    parts = (_SENTENCE_OR_LINE if line_breaks else _SENTENCE).split(text)
    sentences = tuple(words(part) for part in parts) if len(parts) > 1 else ()
    return _judge(words(text), TEXT_MIN, sentences)

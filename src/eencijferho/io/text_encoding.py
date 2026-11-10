# -----------------------------------------------------------------------------
# Organization: CEDA
# License: MIT
# -----------------------------------------------------------------------------
"""
Shared reader for DUO file descriptions (``Bestandsbeschrijving*.txt`` and the
``.asc`` files that the extractor scans as text).

Every module that reads a file description goes through
:func:`read_description_text`, so the same bytes always become the same text
(e.g. "vóór"), whichever module read them. This finishes the encoding strategy
of #93, #98 and #99 (related: #54, #208; this module: #203).

Detection order:

1. A UTF-8 byte order mark: decode as ``utf-8-sig`` (the BOM is dropped;
   invalid bytes after it become U+FFFD and are reported, see below).
2. Otherwise strict ``utf-8``: DUO ships ``Bestandsbeschrijving_Dec-bestanden.txt``
   as BOM-less UTF-8. Real latin-1 text with accents is practically never valid
   UTF-8, so this attempt does not misread latin-1 files.
3. Otherwise ``latin-1`` (ISO-8859-1), which decodes any byte sequence.

After decoding, the text must not contain U+FFFD (the replacement character).
It appears when a file was once opened with the wrong encoding and saved again:
the original letters are lost and no decoder can bring them back. Such a file
raises :class:`DescriptionEncodingError` instead of producing broken names.

Out of scope: the encoding of the data files themselves (converter, latin-1)
and ``clean_for_latin1`` (#208).
"""

from __future__ import annotations

import codecs
import os

REPLACEMENT_CHAR = "\ufffd"

# Limits for the error message: keep it readable for a file with many hits.
_MAX_LINES_LISTED = 10
_MAX_EXCERPT_CHARS = 80


class DescriptionEncodingError(ValueError):
    """A file description contains U+FFFD replacement characters after decoding."""

    def __init__(self, path: str, line_numbers: list[int], excerpt: str | None = None):
        self.path = path
        self.line_numbers = line_numbers
        self.excerpt = excerpt
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        shown = ", ".join(str(n) for n in self.line_numbers[:_MAX_LINES_LISTED])
        extra = len(self.line_numbers) - _MAX_LINES_LISTED
        if extra > 0:
            shown += f" (+{extra} meer)"
        regel = "regel" if len(self.line_numbers) == 1 else "regels"
        message = (
            f"Bestandsbeschrijving '{self.path}' bevat vervangingstekens "
            f"(U+FFFD, '{REPLACEMENT_CHAR}') op {regel} {shown}."
        )
        if self.excerpt:
            message += f" Voorbeeld (regel {self.line_numbers[0]}): {self.excerpt!r}."
        message += (
            " Deze tekens ontstaan als het bestand ooit met de verkeerde tekencodering"
            " is geopend en opgeslagen; de oorspronkelijke letters (bijv. 'ó' in 'vóór')"
            " zijn dan verloren. Herstel de tekst op deze regels en sla het bestand op"
            " als UTF-8 of als ISO-8859-1 (latin-1) zonder vervangingstekens, of gebruik"
            " het originele bestand van DUO. Draai daarna 'extract' opnieuw."
        )
        return message


def _replacement_lines(text: str) -> list[int]:
    """Return the 1-based line numbers that contain U+FFFD."""
    return [
        number
        for number, line in enumerate(text.split("\n"), start=1)
        if REPLACEMENT_CHAR in line
    ]


def _excerpt(text: str, line_number: int) -> str:
    line = text.split("\n")[line_number - 1].strip()
    if len(line) > _MAX_EXCERPT_CHARS:
        line = line[: _MAX_EXCERPT_CHARS - 3] + "..."
    return line


def decode_description_bytes(data: bytes, path: str) -> str:
    """Decode the raw bytes of a file description (see the module docstring).

    Args:
        data: Raw file content.
        path: File path, only used in the error message.

    Returns:
        The decoded text. Line endings are left as they are.

    Raises:
        DescriptionEncodingError: When the decoded text contains U+FFFD.
    """
    if data.startswith(codecs.BOM_UTF8):
        # A BOM promises UTF-8: invalid bytes after it become U+FFFD, so the
        # check below reports their line numbers instead of a bare decode error.
        text = data.decode("utf-8-sig", errors="replace")
    else:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("latin-1")

    if REPLACEMENT_CHAR in text:
        line_numbers = _replacement_lines(text)
        # Only quote a line from a .txt description. The .asc files scanned by
        # the extractor can be data files with personal data (BSN), so for those
        # the message gives line numbers only.
        excerpt = None
        if os.path.splitext(path)[1].lower() == ".txt":
            excerpt = _excerpt(text, line_numbers[0])
        raise DescriptionEncodingError(path, line_numbers, excerpt)

    return text


def read_description_text(storage, path: str) -> str:
    """Read a DUO file description through *storage* and decode it.

    This is the single read path for file descriptions; see the module
    docstring for the detection order and the U+FFFD check.

    Args:
        storage: A storage backend (``eencijferho.io.backends``).
        path: Path of the file in that backend.

    Raises:
        DescriptionEncodingError: When the decoded text contains U+FFFD.
        Exception: Whatever the backend raises when the file cannot be read.
    """
    return decode_description_bytes(storage.read_bytes(path), path)

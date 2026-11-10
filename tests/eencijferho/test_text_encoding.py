"""Tests for the shared file-description reader (#203)."""

import codecs

import pytest

from eencijferho.io.backends.disk import DiskBackend
from eencijferho.io.text_encoding import (
    DescriptionEncodingError,
    decode_description_bytes,
    read_description_text,
)

NAME = "Hoogste vooropleiding vóór het HO"
TEXT = f"Dec_vooropl.asc\n===============\n* {NAME}\n"


@pytest.mark.parametrize(
    "raw",
    [
        TEXT.encode("latin-1"),
        TEXT.encode("utf-8"),
        codecs.BOM_UTF8 + TEXT.encode("utf-8"),
    ],
    ids=["latin-1", "utf-8", "utf-8-bom"],
)
def test_same_text_for_every_encoding(raw):
    assert decode_description_bytes(raw, "Bestandsbeschrijving_x.txt") == TEXT


@pytest.mark.parametrize("encoding", ["latin-1", "utf-8", "utf-8-sig"])
def test_read_description_text_via_storage(tmp_path, encoding):
    path = tmp_path / "Bestandsbeschrijving_x.txt"
    path.write_bytes(TEXT.encode(encoding))
    text = read_description_text(DiskBackend(), str(path))
    assert text == TEXT
    assert NAME in text


def test_bom_is_dropped():
    text = decode_description_bytes(codecs.BOM_UTF8 + b"abc", "x.txt")
    assert text == "abc"


def test_line_endings_are_kept():
    raw = "a\r\nvóór\r\n".encode("latin-1")
    assert decode_description_bytes(raw, "x.txt") == "a\r\nvóór\r\n"


def test_plain_ascii_is_unchanged():
    assert decode_description_bytes(b"Startpositie  Aantal posities\n", "x.txt") == (
        "Startpositie  Aantal posities\n"
    )


def test_replacement_char_raises_with_file_and_lines():
    raw = "regel 1\n* Hoogste vooropleiding v\ufffd\ufffdr het HO\nregel 3\nv\ufffdr\n".encode()
    with pytest.raises(DescriptionEncodingError) as exc_info:
        decode_description_bytes(raw, "data/Bestandsbeschrijving_Dec-bestanden.txt")
    err = exc_info.value
    message = str(err)
    assert err.line_numbers == [2, 4]
    assert "Bestandsbeschrijving_Dec-bestanden.txt" in message
    assert "regels 2, 4" in message
    assert "Hoogste vooropleiding v" in message
    assert "UTF-8" in message and "ISO-8859-1" in message


def test_replacement_char_error_is_a_value_error():
    with pytest.raises(ValueError):
        decode_description_bytes("v\ufffdr".encode(), "x.txt")


def test_invalid_bytes_after_bom_are_reported():
    raw = codecs.BOM_UTF8 + b"ok\nv\xf3\xf3r\n"
    with pytest.raises(DescriptionEncodingError) as exc_info:
        decode_description_bytes(raw, "x.txt")
    assert exc_info.value.line_numbers == [2]


def test_no_excerpt_for_asc_files():
    """.asc files can hold personal data: the message gives line numbers only."""
    raw = "123456789 v\ufffdr\n".encode()
    with pytest.raises(DescriptionEncodingError) as exc_info:
        decode_description_bytes(raw, "EV299XX24.asc")
    assert exc_info.value.excerpt is None
    assert "123456789" not in str(exc_info.value)
    assert "regel 1" in str(exc_info.value)


def test_many_lines_are_truncated_in_message():
    raw = ("v\ufffdr\n" * 15).encode("utf-8")
    with pytest.raises(DescriptionEncodingError) as exc_info:
        decode_description_bytes(raw, "x.txt")
    assert len(exc_info.value.line_numbers) == 15
    assert "(+5 meer)" in str(exc_info.value)

# Tests for eencijferho.utils.converter_headers

import pytest

from eencijferho.utils.converter_headers import (
    normalize_name,
    clean_header_name,
    strip_accents,
)


# --- normalize_name ---


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("Persoonsgebonden nummer", "persoonsgebonden_nummer"),
        ("GESLACHT", "geslacht"),
        ("vóór", "voor"),
        ("a  b", "a_b"),
        ("naam/code", "naam_code"),
        ("snake_case", "snake_case"),
        ("var2023", "var2023"),
    ],
)
def test_normalize_name(input_name, expected):
    assert normalize_name(input_name) == expected


def test_normalize_name_strips_leading_trailing_underscores():
    result = normalize_name("_naam_")
    assert not result.startswith("_")
    assert not result.endswith("_")


# --- strip_accents ---


@pytest.mark.parametrize(
    "text,expected",
    [
        # e-varianten
        ("é", "e"), ("ë", "e"), ("è", "e"), ("ê", "e"),
        # o-varianten
        ("ó", "o"), ("ö", "o"), ("ò", "o"), ("ô", "o"),
        # u-varianten
        ("ü", "u"), ("ú", "u"), ("ù", "u"), ("û", "u"),
        # a-varianten
        ("ä", "a"), ("á", "a"), ("à", "a"), ("â", "a"),
        # i-varianten
        ("ï", "i"), ("í", "i"), ("ì", "i"), ("î", "i"),
        # overige diakrieten
        ("ñ", "n"), ("ç", "c"),
        # hoofdletters
        ("Ë", "E"), ("Ü", "U"), ("Ï", "I"),
        # woorden
        ("vóór", "voor"), ("België", "Belgie"), ("Oekraïne", "Oekraine"),
        # ascii ongewijzigd
        ("hello", "hello"),
    ],
)
def test_strip_accents(text, expected):
    assert strip_accents(text) == expected


# --- clean_header_name ---


def test_clean_header_name_removes_diacritics():
    result = clean_header_name("vóór")
    assert "ó" not in result
    assert "voor" in result.lower()


def test_clean_header_name_replaces_fraction_slash():
    assert "\u2044" not in clean_header_name("a\u2044b")
    assert "/" in clean_header_name("a\u2044b")


def test_clean_header_name_strips_whitespace():
    assert clean_header_name("  naam  ") == "naam"


def test_clean_header_name_plain_ascii_unchanged():
    assert clean_header_name("GeslachtCode") == "GeslachtCode"

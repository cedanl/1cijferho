
import pytest

from eencijferho.utils.converter_headers import (
    normalize_name,
    clean_header_name,
    strip_accents,
    convert_csv_headers_to_snake_case,
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


# --- convert_csv_headers_to_snake_case: cell values stay unchanged (#208) ---

# Synthetic values with characters outside latin-1 (en dash, euro sign, oe
# ligature, fraction slash) and inside latin-1 (accented letters).
NON_LATIN1_VALUES = ["Sint–Jan lyceum", "Havo €/school", "school œ", "a⁄b", "vóór"]


def _write_utf8_csv(path, header: list[str], rows: list[list[str]]) -> None:
    lines = [";".join(header)] + [";".join(row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_utf8_csv(path) -> tuple[list[str], list[list[str]]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return lines[0].split(";"), [line.split(";") for line in lines[1:]]


def test_snake_case_step_keeps_non_latin1_cell_values(tmp_path):
    path = tmp_path / "EV2023.csv"
    rows = [[value, str(i)] for i, value in enumerate(NON_LATIN1_VALUES)]
    _write_utf8_csv(path, ["Naam school", "Volgnummer"], rows)

    convert_csv_headers_to_snake_case(input_dir=str(tmp_path))

    header, written = _read_utf8_csv(path)
    assert header == ["naam_school", "volgnummer"]
    assert written == rows
    assert "?" not in path.read_text(encoding="utf-8")


def test_snake_case_step_leaves_snake_case_file_untouched(tmp_path):
    path = tmp_path / "Dec_test.csv"
    _write_utf8_csv(path, ["naam_school"], [[v] for v in NON_LATIN1_VALUES])
    before = path.read_bytes()

    convert_csv_headers_to_snake_case(input_dir=str(tmp_path))

    assert path.read_bytes() == before

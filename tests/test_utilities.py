# Tests for eencijferho.utils: converter_headers, encryptor, compressor

import hashlib
import pytest
import polars as pl
from pathlib import Path

from eencijferho.utils.converter_headers import (
    normalize_name,
    clean_header_name,
    strip_accents,
)
from eencijferho.utils.encryptor import encryptor
from eencijferho.utils.compressor import convert_csv_to_parquet


def _make_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    lines = [";".join(cols)] + [";".join(str(row[c]) for c in cols) for row in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


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
        ("é", "e"),
        ("ê", "e"),
        ("hello", "hello"),
        ("vóór", "voor"),
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


# --- encryptor ---


@pytest.mark.parametrize(
    "column",
    ["Burgerservicenummer", "Onderwijsnummer", "Persoonsgebonden nummer"],
)
def test_encryptor_hashes_sensitive_column(tmp_path, column):
    csv = tmp_path / "EV2023.csv"
    _make_csv(csv, [{column: "123456789"}])
    encryptor(str(tmp_path), str(tmp_path))
    out = tmp_path / "EV2023_encrypted.csv"
    assert out.exists()
    df = pl.read_csv(out, separator=";")
    expected = hashlib.sha256("123456789".encode()).hexdigest()
    assert df[column][0] == expected


def test_encryptor_leaves_non_sensitive_column_unchanged(tmp_path):
    csv = tmp_path / "EV2023.csv"
    _make_csv(csv, [{"Burgerservicenummer": "123", "Naam": "Jan"}])
    encryptor(str(tmp_path), str(tmp_path))
    df = pl.read_csv(tmp_path / "EV2023_encrypted.csv", separator=";")
    assert df["Naam"][0] == "Jan"


def test_encryptor_output_has_encrypted_suffix(tmp_path):
    _make_csv(tmp_path / "EV2023.csv", [{"Burgerservicenummer": "111"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert (tmp_path / "EV2023_encrypted.csv").exists()


@pytest.mark.parametrize("filename", ["Dec_geslacht.csv", "OTHERUNKNOWN.csv"])
def test_encryptor_skips_non_target_files(tmp_path, filename):
    _make_csv(tmp_path / filename, [{"Burgerservicenummer": "111"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert not (tmp_path / filename.replace(".csv", "_encrypted.csv")).exists()


def test_encryptor_processes_vakhavw_files(tmp_path):
    _make_csv(tmp_path / "VAKHAVW2023.csv", [{"Burgerservicenummer": "555"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert (tmp_path / "VAKHAVW2023_encrypted.csv").exists()


def test_encryptor_skips_file_with_no_sensitive_columns(tmp_path):
    _make_csv(tmp_path / "EV2023.csv", [{"OpleCode": "ABC", "Jaar": "2023"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert not (tmp_path / "EV2023_encrypted.csv").exists()


# --- convert_csv_to_parquet ---


def test_compressor_creates_parquet(tmp_path):
    _make_csv(tmp_path / "EV2023.csv", [{"Naam": "Jan"}, {"Naam": "Piet"}])
    convert_csv_to_parquet(str(tmp_path))
    assert (tmp_path / "EV2023.parquet").exists()


@pytest.mark.parametrize("filename", ["Dec_geslacht.csv", "DEC_LANDCODE.csv"])
def test_compressor_skips_dec_files(tmp_path, filename):
    _make_csv(tmp_path / filename, [{"Code": "1"}])
    convert_csv_to_parquet(str(tmp_path))
    assert not (tmp_path / filename.replace(".csv", ".parquet")).exists()


def test_compressor_data_round_trips(tmp_path):
    _make_csv(
        tmp_path / "EV2023.csv",
        [{"Naam": "Jan", "Waarde": "42"}, {"Naam": "Piet", "Waarde": "7"}],
    )
    convert_csv_to_parquet(str(tmp_path))
    df = pl.read_parquet(tmp_path / "EV2023.parquet")
    assert df.shape == (2, 2)
    assert list(df.columns) == ["Naam", "Waarde"]


def test_compressor_empty_directory_no_error(tmp_path):
    convert_csv_to_parquet(str(tmp_path))

"""
Unit tests for eencijferho.utils.dec_validation

Tests:
    - test_parse_dec_mapping_simple: simple mappings zijn correct geparseerd
    - test_parse_dec_mapping_composite: composite key mapping (in combinatie met) geparseerd
    - test_composite_key_plus_skipped: "+"-stijl composite key in simple sectie overgeslagen
    - test_valid_data: alle kolomwaarden komen voor in DEC bestand -> success=True
    - test_invalid_data: kolom bevat waarde die niet in DEC staat -> success=False
    - test_missing_dec_file: DEC CSV niet aanwezig -> kolom stilzwijgend overgeslagen
    - test_composite_key_validation_valid: geldige paren in composite DEC -> success=True
    - test_composite_key_validation_invalid: ongeldig paar in composite DEC -> success=False
"""

import os
import tempfile
import pytest

from eencijferho.utils.dec_validation import (
    parse_dec_mapping,
    validate_with_dec_files,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(content: str, suffix: str, encoding: str = "utf-8") -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding=encoding) as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Sample DEC txt content
# ---------------------------------------------------------------------------

DEC_TXT_SIMPLE = """\
Dec_landcode.asc
================
                                             Startpositie  Aantal posities
Code land                                           1             4
Naam land                                           5            40

Ten behoeve van de decodering van de velden:
* Geboorteland
* Geboorteland ouder 1

Dec_nationaliteitscode.asc
==========================
                                             Startpositie  Aantal posities
Code nationaliteit                                  1             4
Omschrijving                                        5            50

Ten behoeve van de decodering van de velden:
* Nationaliteit 1
* Nationaliteit 2

Dec_instellingscodevest.asc
===========================
Ten behoeve van de decodering van de velden:
* Instellingscode + Vestigingsnummer
"""

DEC_TXT_COMPOSITE = """\
Dec_vest_ho.asc
===============
                                             Startpositie  Aantal posities
Instellingscode                                     1             4
Vestigingsnummer                                    5             2

Ten behoeve van de decodering van instellingscode in combinatie met het veld
* Vestigingsnummer
* Vestigingsnummer diploma

"""

DEC_LANDCODE_CSV = "code_land;naam_land\n0;Onbekend\n5001;Nederland\n5002;Belgie\n"
DEC_NATIONAAL_CSV = "code_nationaliteit;omschrijving\n0001;Nederlandse\n0002;Belgische\n"

# Dec_vest_ho: valid pairs (Instellingscode, Vestigingsnummer)
DEC_VEST_HO_CSV = "instellingscode;vestigingsnummer\n1234;01\n1234;02\n5678;01\n"

MAIN_CSV_VALID = "Geboorteland;Nationaliteit 1\n5001;0001\n5002;0002\n0;0001\n"
MAIN_CSV_INVALID = "Geboorteland;Nationaliteit 1\n5001;0001\n9999;9999\n"

MAIN_CSV_COMPOSITE_VALID = "Instellingscode;Vestigingsnummer\n1234;01\n1234;02\n5678;01\n"
MAIN_CSV_COMPOSITE_INVALID = "Instellingscode;Vestigingsnummer\n1234;01\n9999;99\n"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dec_txt_simple():
    path = _write(DEC_TXT_SIMPLE, ".txt")
    yield path
    os.unlink(path)


@pytest.fixture
def dec_txt_composite():
    path = _write(DEC_TXT_COMPOSITE, ".txt")
    yield path
    os.unlink(path)


@pytest.fixture
def dec_dir(tmp_path):
    (tmp_path / "Dec_landcode.csv").write_text(DEC_LANDCODE_CSV, encoding="utf-8")
    (tmp_path / "Dec_nationaliteitscode.csv").write_text(DEC_NATIONAAL_CSV, encoding="utf-8")
    return tmp_path


@pytest.fixture
def dec_dir_composite(tmp_path):
    (tmp_path / "Dec_vest_ho.csv").write_text(DEC_VEST_HO_CSV, encoding="utf-8")
    return tmp_path


@pytest.fixture
def valid_main_csv():
    path = _write(MAIN_CSV_VALID, ".csv")
    yield path
    os.unlink(path)


@pytest.fixture
def invalid_main_csv():
    path = _write(MAIN_CSV_INVALID, ".csv")
    yield path
    os.unlink(path)


@pytest.fixture
def composite_valid_csv():
    path = _write(MAIN_CSV_COMPOSITE_VALID, ".csv")
    yield path
    os.unlink(path)


@pytest.fixture
def composite_invalid_csv():
    path = _write(MAIN_CSV_COMPOSITE_INVALID, ".csv")
    yield path
    os.unlink(path)


# ---------------------------------------------------------------------------
# Tests: parse_dec_mapping
# ---------------------------------------------------------------------------

def test_parse_dec_mapping_simple(dec_txt_simple):
    mapping = parse_dec_mapping(dec_txt_simple)

    assert "Dec_landcode" in mapping
    entry = mapping["Dec_landcode"]
    assert entry["type"] == "simple"
    assert "Geboorteland" in entry["columns"]
    assert "Geboorteland ouder 1" in entry["columns"]

    assert "Dec_nationaliteitscode" in mapping
    entry2 = mapping["Dec_nationaliteitscode"]
    assert entry2["type"] == "simple"
    assert "Nationaliteit 1" in entry2["columns"]
    assert "Nationaliteit 2" in entry2["columns"]


def test_parse_dec_mapping_composite(dec_txt_composite):
    mapping = parse_dec_mapping(dec_txt_composite)

    assert "Dec_vest_ho" in mapping
    entry = mapping["Dec_vest_ho"]
    assert entry["type"] == "composite"
    assert entry["anchor"] == "instellingscode"
    assert "Vestigingsnummer" in entry["targets"]
    assert "Vestigingsnummer diploma" in entry["targets"]


def test_composite_key_plus_skipped(dec_txt_simple):
    # Dec_instellingscodevest has "* Instellingscode + Vestigingsnummer" in a simple
    # section — the '+' means it's skipped, so the DEC gets no entry at all
    mapping = parse_dec_mapping(dec_txt_simple)
    assert "Dec_instellingscodevest" not in mapping


# ---------------------------------------------------------------------------
# Tests: validate_with_dec_files (simple)
# ---------------------------------------------------------------------------

def test_valid_data(valid_main_csv, dec_dir, dec_txt_simple):
    mapping = parse_dec_mapping(dec_txt_simple)
    success, results = validate_with_dec_files(valid_main_csv, dec_dir, mapping)

    assert success is True
    assert results["total_issues"] == 0
    assert results["columns_checked"] == 2
    for col in results["column_results"]:
        assert col["status"] == "ok"
        assert col["invalid_values"] == []


def test_invalid_data(invalid_main_csv, dec_dir, dec_txt_simple):
    mapping = parse_dec_mapping(dec_txt_simple)
    success, results = validate_with_dec_files(invalid_main_csv, dec_dir, mapping)

    assert success is False
    assert results["total_issues"] > 0
    failed = [r for r in results["column_results"] if r["status"] == "failed"]
    assert len(failed) > 0
    assert any("9999" in r["invalid_values"] for r in failed)


def test_missing_dec_file(valid_main_csv, tmp_path, dec_txt_simple):
    # Empty dec_dir — no DEC CSVs present, columns silently skipped
    mapping = parse_dec_mapping(dec_txt_simple)
    success, results = validate_with_dec_files(valid_main_csv, tmp_path, mapping)

    assert results["columns_checked"] == 0
    assert success is True


# ---------------------------------------------------------------------------
# Tests: validate_with_dec_files (composite)
# ---------------------------------------------------------------------------

def test_composite_key_validation_valid(composite_valid_csv, dec_dir_composite, dec_txt_composite):
    mapping = parse_dec_mapping(dec_txt_composite)
    success, results = validate_with_dec_files(composite_valid_csv, dec_dir_composite, mapping)

    assert success is True
    assert results["columns_checked"] == 1  # only "Vestigingsnummer" found (no diploma col)
    assert results["total_issues"] == 0
    for col in results["column_results"]:
        assert col["status"] == "ok"


def test_composite_key_validation_invalid(composite_invalid_csv, dec_dir_composite, dec_txt_composite):
    mapping = parse_dec_mapping(dec_txt_composite)
    success, results = validate_with_dec_files(composite_invalid_csv, dec_dir_composite, mapping)

    assert success is False
    assert results["total_issues"] > 0
    failed = [r for r in results["column_results"] if r["status"] == "failed"]
    assert len(failed) > 0
    # Invalid pair (9999, 99) should appear in invalid_values
    assert any("9999" in str(r["invalid_values"]) for r in failed)

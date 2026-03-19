"""
Unit tests for eencijferho.utils.dec_validation

Tests:
    - test_parse_dec_mapping: mapping wordt correct geparseerd uit txt
    - test_valid_data: alle kolomwaarden komen voor in DEC bestand -> success=True
    - test_invalid_data: kolom bevat waarde die niet in DEC staat -> success=False
    - test_missing_dec_file: DEC CSV niet aanwezig -> kolom stilzwijgend overgeslagen
    - test_composite_key_skipped: samengestelde keys worden overgeslagen
"""

import os
import tempfile
import pytest

from eencijferho.utils.dec_validation import (
    parse_dec_mapping,
    validate_with_dec_files,
    validate_with_dec_files_folder,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(content: str, suffix: str) -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="latin1") as f:
        f.write(content)
    return path


DEC_TXT = """\
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

DEC_LANDCODE_CSV = "code_land;naam_land\n0;Onbekend\n5001;Nederland\n5002;Belgie\n"
DEC_NATIONAAL_CSV = "code_nationaliteit;omschrijving\n0001;Nederlandse\n0002;Belgische\n"

MAIN_CSV_VALID = "Geboorteland;Nationaliteit 1\n5001;0001\n5002;0002\n0;0001\n"
MAIN_CSV_INVALID = "Geboorteland;Nationaliteit 1\n5001;0001\n9999;9999\n"  # 9999 not in DEC


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dec_txt():
    path = _write(DEC_TXT, ".txt")
    yield path
    os.unlink(path)


@pytest.fixture
def dec_dir(tmp_path):
    (tmp_path / "Dec_landcode.csv").write_text(DEC_LANDCODE_CSV, encoding="utf-8")
    (tmp_path / "Dec_nationaliteitscode.csv").write_text(DEC_NATIONAAL_CSV, encoding="utf-8")
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_parse_dec_mapping(dec_txt):
    mapping = parse_dec_mapping(dec_txt)

    assert "Dec_landcode" in mapping
    assert "Geboorteland" in mapping["Dec_landcode"]
    assert "Geboorteland ouder 1" in mapping["Dec_landcode"]

    assert "Dec_nationaliteitscode" in mapping
    assert "Nationaliteit 1" in mapping["Dec_nationaliteitscode"]
    assert "Nationaliteit 2" in mapping["Dec_nationaliteitscode"]


def test_composite_key_skipped(dec_txt):
    mapping = parse_dec_mapping(dec_txt)
    # Dec_instellingscodevest has a composite key "Instellingscode + Vestigingsnummer"
    # which should be skipped — so no columns mapped
    assert "Dec_instellingscodevest" not in mapping


def test_valid_data(valid_main_csv, dec_dir, dec_txt):
    mapping = parse_dec_mapping(dec_txt)
    success, results = validate_with_dec_files(valid_main_csv, dec_dir, mapping)

    assert success is True
    assert results["total_issues"] == 0
    assert results["columns_checked"] == 2
    for col in results["column_results"]:
        assert col["status"] == "ok"
        assert col["invalid_values"] == []


def test_invalid_data(invalid_main_csv, dec_dir, dec_txt):
    mapping = parse_dec_mapping(dec_txt)
    success, results = validate_with_dec_files(invalid_main_csv, dec_dir, mapping)

    assert success is False
    assert results["total_issues"] > 0

    failed = [r for r in results["column_results"] if r["status"] == "failed"]
    assert len(failed) > 0
    assert any("9999" in r["invalid_values"] for r in failed)


def test_missing_dec_file(valid_main_csv, tmp_path, dec_txt):
    # Empty dec_dir — no DEC CSVs present
    mapping = parse_dec_mapping(dec_txt)
    success, results = validate_with_dec_files(valid_main_csv, tmp_path, mapping)

    # Columns are silently skipped when DEC file is missing
    assert results["columns_checked"] == 0
    assert success is True

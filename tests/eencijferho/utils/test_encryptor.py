# Tests for eencijferho.utils.encryptor

import hashlib
import pytest
import polars as pl

from eencijferho.utils.encryptor import encryptor


@pytest.mark.parametrize(
    "column",
    ["Burgerservicenummer", "Onderwijsnummer", "Persoonsgebonden nummer"],
)
def test_encryptor_hashes_sensitive_column(tmp_path, make_csv, column):
    make_csv(tmp_path / "EV2023.csv", [{column: "123456789"}])
    encryptor(str(tmp_path), str(tmp_path))
    out = tmp_path / "EV2023_encrypted.csv"
    assert out.exists()
    df = pl.read_csv(out, separator=";")
    expected = hashlib.sha256("123456789".encode()).hexdigest()
    assert df[column][0] == expected


def test_encryptor_leaves_non_sensitive_column_unchanged(tmp_path, make_csv):
    make_csv(tmp_path / "EV2023.csv", [{"Burgerservicenummer": "123", "Naam": "Jan"}])
    encryptor(str(tmp_path), str(tmp_path))
    df = pl.read_csv(tmp_path / "EV2023_encrypted.csv", separator=";")
    assert df["Naam"][0] == "Jan"


def test_encryptor_output_has_encrypted_suffix(tmp_path, make_csv):
    make_csv(tmp_path / "EV2023.csv", [{"Burgerservicenummer": "111"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert (tmp_path / "EV2023_encrypted.csv").exists()


@pytest.mark.parametrize("filename", ["Dec_geslacht.csv", "OTHERUNKNOWN.csv"])
def test_encryptor_skips_non_target_files(tmp_path, make_csv, filename):
    make_csv(tmp_path / filename, [{"Burgerservicenummer": "111"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert not (tmp_path / filename.replace(".csv", "_encrypted.csv")).exists()


def test_encryptor_processes_vakhavw_files(tmp_path, make_csv):
    make_csv(tmp_path / "VAKHAVW2023.csv", [{"Burgerservicenummer": "555"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert (tmp_path / "VAKHAVW2023_encrypted.csv").exists()


def test_encryptor_skips_file_with_no_sensitive_columns(tmp_path, make_csv):
    make_csv(tmp_path / "EV2023.csv", [{"OpleCode": "ABC", "Jaar": "2023"}])
    encryptor(str(tmp_path), str(tmp_path))
    assert not (tmp_path / "EV2023_encrypted.csv").exists()

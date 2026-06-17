# Tests for eencijferho.utils.pseudonymizer

import hashlib
import hmac

import polars as pl
import pytest

from eencijferho.config import (
    DUO_BSN_COLUMN,
    DUO_ONDERWIJSNUMMER_COLUMN,
    DUO_PGN_COLUMN,
)
from eencijferho.utils.pseudonymizer import (
    ENCRYPT_KEY_ENV,
    load_key,
    pseudonymize_files,
    pseudonymize_value,
)

_KEY = b"geheime-sleutel"


def _write_csv(path, rows: list[dict], sep: str = ";") -> None:
    cols = list(rows[0].keys())
    lines = [sep.join(cols)] + [sep.join(str(r[c]) for c in cols) for r in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def _read_csv(path) -> pl.DataFrame:
    return pl.read_csv(path, separator=";", infer_schema_length=0)


# ---------------------------------------------------------------------------
# pseudonymize_value — pure helper
# ---------------------------------------------------------------------------

def test_value_is_deterministic():
    assert pseudonymize_value(_KEY, "123456789") == pseudonymize_value(_KEY, "123456789")


def test_value_matches_reference_hmac():
    expected = hmac.new(_KEY, b"123456789", hashlib.sha256).hexdigest()
    assert pseudonymize_value(_KEY, "123456789") == expected


def test_value_is_key_sensitive():
    a = pseudonymize_value(b"key-a", "123456789")
    b = pseudonymize_value(b"key-b", "123456789")
    assert a != b


def test_value_none_and_empty_pass_through():
    assert pseudonymize_value(_KEY, None) is None
    assert pseudonymize_value(_KEY, "") is None


def test_value_coerces_non_string():
    assert pseudonymize_value(_KEY, 123456789) == pseudonymize_value(_KEY, "123456789")


# ---------------------------------------------------------------------------
# Brute-force resistance: without the key, the BSN space cannot be reversed
# ---------------------------------------------------------------------------

def test_brute_force_without_key_fails():
    secret = b"server-side-secret"
    target = pseudonymize_value(secret, "100000042")
    # An attacker enumerates candidate BSNs but does NOT have the secret key.
    # Hashing with the wrong (empty/guessed) key never reproduces the token.
    for candidate in range(100000000, 100000100):
        guess = pseudonymize_value(b"", str(candidate))
        assert guess != target


def test_brute_force_with_key_would_match():
    # Sanity check that the value IS in the searched range — proving the
    # previous test fails because of the missing key, not a range mismatch.
    secret = b"server-side-secret"
    target = pseudonymize_value(secret, "100000042")
    found = any(
        pseudonymize_value(secret, str(c)) == target
        for c in range(100000000, 100000100)
    )
    assert found


# ---------------------------------------------------------------------------
# load_key — precedence and failure
# ---------------------------------------------------------------------------

def test_load_key_explicit():
    assert load_key(key="abc") == b"abc"


def test_load_key_from_file(tmp_path):
    f = tmp_path / "key.txt"
    f.write_text("  filekey\n", encoding="utf-8")
    assert load_key(key_file=str(f)) == b"filekey"


def test_load_key_from_env(monkeypatch):
    monkeypatch.setenv(ENCRYPT_KEY_ENV, "envkey")
    assert load_key() == b"envkey"


def test_load_key_explicit_beats_env(monkeypatch):
    monkeypatch.setenv(ENCRYPT_KEY_ENV, "envkey")
    assert load_key(key="explicit") == b"explicit"


def test_load_key_missing_raises(monkeypatch):
    monkeypatch.delenv(ENCRYPT_KEY_ENV, raising=False)
    with pytest.raises(ValueError, match=ENCRYPT_KEY_ENV):
        load_key()


# ---------------------------------------------------------------------------
# pseudonymize_files — replaces columns in place
# ---------------------------------------------------------------------------

def test_replaces_bsn_in_place(tmp_path):
    _write_csv(
        tmp_path / "EV2023.csv",
        [{DUO_BSN_COLUMN: "111111111", "Naam": "Jan"},
         {DUO_BSN_COLUMN: "222222222", "Naam": "Piet"}],
    )

    pseudonymize_files(output_dir=str(tmp_path), key="geheime-sleutel")

    df = _read_csv(tmp_path / "EV2023.csv")
    # Column still present, but no plaintext BSN survives.
    assert DUO_BSN_COLUMN in df.columns
    values = df[DUO_BSN_COLUMN].to_list()
    assert "111111111" not in values
    assert "222222222" not in values
    # Pseudonyms match the keyed HMAC.
    assert values[0] == pseudonymize_value(_KEY, "111111111")
    # Other columns untouched.
    assert df["Naam"].to_list() == ["Jan", "Piet"]


def test_same_bsn_yields_same_pseudonym(tmp_path):
    _write_csv(
        tmp_path / "EV2023.csv",
        [{DUO_BSN_COLUMN: "111111111", "Jaar": "2022"},
         {DUO_BSN_COLUMN: "111111111", "Jaar": "2023"}],
    )

    pseudonymize_files(output_dir=str(tmp_path), key="geheime-sleutel")

    df = _read_csv(tmp_path / "EV2023.csv")
    pseudos = df[DUO_BSN_COLUMN].to_list()
    assert pseudos[0] == pseudos[1]  # longitudinal linkage preserved


def test_all_three_sensitive_columns_replaced(tmp_path):
    _write_csv(
        tmp_path / "EV2023.csv",
        [{DUO_BSN_COLUMN: "111111111",
          DUO_PGN_COLUMN: "PGN001",
          DUO_ONDERWIJSNUMMER_COLUMN: "ON001",
          "Naam": "Jan"}],
    )

    pseudonymize_files(output_dir=str(tmp_path), key="geheime-sleutel")

    df = _read_csv(tmp_path / "EV2023.csv")
    assert df[DUO_BSN_COLUMN][0] == pseudonymize_value(_KEY, "111111111")
    assert df[DUO_PGN_COLUMN][0] == pseudonymize_value(_KEY, "PGN001")
    assert df[DUO_ONDERWIJSNUMMER_COLUMN][0] == pseudonymize_value(_KEY, "ON001")


def test_processes_vakhavw_file(tmp_path):
    _write_csv(tmp_path / "VAKHAVW2023.csv", [{DUO_BSN_COLUMN: "555555555", "Vak": "NL"}])

    pseudonymize_files(output_dir=str(tmp_path), key="geheime-sleutel")

    df = _read_csv(tmp_path / "VAKHAVW2023.csv")
    assert df[DUO_BSN_COLUMN][0] == pseudonymize_value(_KEY, "555555555")


def test_skips_non_ev_vakhavw_files(tmp_path):
    _write_csv(tmp_path / "Dec_geslacht.csv", [{DUO_BSN_COLUMN: "111111111", "Code": "M"}])

    pseudonymize_files(output_dir=str(tmp_path), key="geheime-sleutel")

    df = _read_csv(tmp_path / "Dec_geslacht.csv")
    assert df[DUO_BSN_COLUMN][0] == "111111111"  # untouched


def test_file_without_sensitive_columns_skipped(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{"Naam": "Jan", "Jaar": "2023"}])

    log = pseudonymize_files(output_dir=str(tmp_path), key="geheime-sleutel")

    df = _read_csv(tmp_path / "EV2023.csv")
    assert df["Naam"][0] == "Jan"
    assert "overgeslagen" in log


def test_key_never_appears_in_log(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{DUO_BSN_COLUMN: "111111111", "Naam": "Jan"}])

    secret = "super-geheime-sleutel-xyz"
    log = pseudonymize_files(output_dir=str(tmp_path), key=secret)

    assert secret not in log


def test_key_never_appears_in_output(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{DUO_BSN_COLUMN: "111111111", "Naam": "Jan"}])

    secret = "super-geheime-sleutel-xyz"
    pseudonymize_files(output_dir=str(tmp_path), key=secret)

    assert secret not in (tmp_path / "EV2023.csv").read_text(encoding="utf-8")


def test_missing_key_raises(tmp_path, monkeypatch):
    monkeypatch.delenv(ENCRYPT_KEY_ENV, raising=False)
    _write_csv(tmp_path / "EV2023.csv", [{DUO_BSN_COLUMN: "111111111", "Naam": "Jan"}])

    with pytest.raises(ValueError, match=ENCRYPT_KEY_ENV):
        pseudonymize_files(output_dir=str(tmp_path))


def test_no_files_returns_log(tmp_path):
    log = pseudonymize_files(output_dir=str(tmp_path), key="geheime-sleutel")
    assert "0 bestand" in log

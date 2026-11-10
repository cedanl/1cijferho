"""Regression gates must verify readable content, not just matching shapes."""

import hashlib
import json

import sys

import polars as pl
import pytest

from eencijferho.utils import snapshot_validator
from eencijferho.utils.snapshot_validator import generate_snapshot, validate_snapshot


@pytest.fixture
def snapshot_case(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    csv = output / "EV-test.csv"
    csv.write_text("code;label\n0001;Een\n", encoding="utf-8")
    snapshot = tmp_path / "snapshot.json"
    generate_snapshot(str(output), str(snapshot))
    return output, csv, snapshot


def test_snapshot_records_hash_and_preserves_string_schema(snapshot_case):
    output, csv, snapshot = snapshot_case
    entry = json.loads(snapshot.read_text())["files"][csv.name]
    assert entry["file_hash"] == hashlib.sha256(csv.read_bytes()).hexdigest()
    assert entry["row_count"] == 1
    assert entry["columns"] == ["code", "label"]
    assert validate_snapshot(str(output), str(snapshot)) == (True, [], [])


def test_changed_values_fail_even_when_shape_matches(snapshot_case):
    output, csv, snapshot = snapshot_case
    csv.write_text("code;label\n9999;Anders\n", encoding="utf-8")
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    # Same row count, columns and dtypes: the hash is the only thing that differs.
    assert errors == [f"{csv.name}: inhoud gewijzigd (SHA256 verschilt)"]


def test_unreadable_csv_never_passes(snapshot_case):
    output, csv, snapshot = snapshot_case
    csv.write_bytes(b"code;label\n\xff;MARKER123\n")
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any(csv.name in error and "onleesbaar" in error for error in errors)
    assert not any("MARKER123" in error for error in errors)


def test_read_error_never_echoes_exception_text(snapshot_case, monkeypatch):
    """Reader errors can quote cell values; only the exception type is reported."""
    output, csv, snapshot = snapshot_case

    def failing_read(*args, **kwargs):
        raise ValueError("could not parse `MARKER-VALUE` as Int64")

    monkeypatch.setattr(pl, "read_csv", failing_read)
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert errors == [f"{csv.name}: bestand onleesbaar: ValueError bij het lezen"]


def test_corrupt_parquet_fails_validate_and_generate(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    parquet = output / "EV-test.parquet"
    pl.DataFrame({"code": ["1"]}).write_parquet(parquet)
    snapshot = tmp_path / "snapshot.json"
    generate_snapshot(str(output), str(snapshot))
    baseline = snapshot.read_bytes()

    parquet.write_bytes(b"not a parquet file")
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any(parquet.name in error and "onleesbaar" in error for error in errors)

    with pytest.raises(ValueError, match="onleesbaar"):
        generate_snapshot(str(output), str(snapshot))
    assert snapshot.read_bytes() == baseline


def test_missing_counts_in_current_file_fail(tmp_path):
    """A file whose scan yields no counts must not pass a snapshot that expects them."""
    output = tmp_path / "output"
    output.mkdir()
    (output / "EV-test.txt").write_text("code;label\n0001;Een\n", encoding="utf-8")
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(
        {"files": {"EV-test.txt": {"row_count": 1, "column_count": 2}}}
    ))
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("rijen None" in error for error in errors)
    assert any("kolommen None" in error for error in errors)


def test_unescaped_quotes_still_read_via_fallback(snapshot_case):
    """The converter does not escape quotes (yet); the fallback keeps such output readable."""
    output, csv, snapshot = snapshot_case
    csv.write_text('code;label\n0001;"de Bontekoe" NTC-VO\n', encoding="utf-8")
    generate_snapshot(str(output), str(snapshot))
    assert validate_snapshot(str(output), str(snapshot)) == (True, [], [])


def test_missing_file_fails(snapshot_case):
    output, csv, snapshot = snapshot_case
    csv.unlink()
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("Bestand ontbreekt" in error for error in errors)


def test_unexpected_file_warns_by_default_and_fails_when_strict(snapshot_case):
    """Lenient while the baselines are v1; strict becomes the default once they are v2."""
    output, _, snapshot = snapshot_case
    (output / "EV-old.csv").write_text("code\n1\n", encoding="utf-8")
    passed, errors, warnings = validate_snapshot(str(output), str(snapshot))
    assert passed and not errors
    assert any("Onverwacht bestand" in warning for warning in warnings)
    passed, errors, _ = validate_snapshot(str(output), str(snapshot), strict_files=True)
    assert not passed
    assert any("Onverwacht bestand" in error for error in errors)


def test_legacy_snapshot_read_error_does_not_become_success(snapshot_case):
    output, csv, snapshot = snapshot_case
    snapshot.write_text(json.dumps({"files": {csv.name: {"read_error": "old failure"}}}))
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("snapshot bevat een leesfout" in error for error in errors)


def test_v2_snapshot_cannot_silently_omit_hash(snapshot_case):
    output, csv, snapshot = snapshot_case
    data = json.loads(snapshot.read_text())
    del data["files"][csv.name]["file_hash"]
    snapshot.write_text(json.dumps(data))
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("SHA256 ontbreekt" in error for error in errors)


def test_legacy_shape_only_snapshot_is_explicitly_warned(snapshot_case):
    output, csv, snapshot = snapshot_case
    data = json.loads(snapshot.read_text())
    data.pop("snapshot_version")
    del data["files"][csv.name]["file_hash"]
    snapshot.write_text(json.dumps(data))
    passed, errors, warnings = validate_snapshot(str(output), str(snapshot))
    assert passed and not errors
    assert any("zonder inhoudhash" in warning for warning in warnings)


def test_bad_output_cannot_overwrite_approved_baseline(snapshot_case):
    output, csv, snapshot = snapshot_case
    baseline = snapshot.read_bytes()
    csv.write_bytes(b"code\n\xff\n")
    with pytest.raises(ValueError, match="onleesbaar"):
        generate_snapshot(str(output), str(snapshot))
    assert snapshot.read_bytes() == baseline


def test_parquet_type_and_content_are_checked(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    parquet = output / "EV-test.parquet"
    pl.DataFrame({"code": ["1"]}).write_parquet(parquet)
    snapshot = tmp_path / "snapshot.json"
    generate_snapshot(str(output), str(snapshot))
    pl.DataFrame({"code": [1]}).write_parquet(parquet)
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("type" in error for error in errors)


def test_metadata_is_not_an_output_product(snapshot_case):
    output, _, snapshot = snapshot_case
    metadata = output / "metadata" / "logs"
    metadata.mkdir(parents=True)
    (metadata / "log.json").write_text("{}")
    assert validate_snapshot(str(output), str(snapshot)) == (True, [], [])


@pytest.mark.parametrize("strict, exit_code", [(False, None), (True, 1)])
def test_cli_passes_strict_files_through(snapshot_case, monkeypatch, strict, exit_code):
    output, _, snapshot = snapshot_case
    (output / "EV-old.csv").write_text("code\n1\n", encoding="utf-8")
    argv = ["snapshot_validator", "--output", str(output), "--snapshot", str(snapshot), "--validate"]
    monkeypatch.setattr(sys, "argv", argv + (["--strict-files"] if strict else []))
    if exit_code is None:
        snapshot_validator.main()
    else:
        with pytest.raises(SystemExit) as exc_info:
            snapshot_validator.main()
        assert exc_info.value.code == exit_code


# ---------------------------------------------------------------------------
# Platform- and library-independent hashes
# ---------------------------------------------------------------------------

@pytest.fixture
def parquet_case(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    parquet = output / "EV-test.parquet"
    frame = pl.DataFrame({
        "code": ["0001", "0002", None, "0004"],
        "label": ["Een", "", "Drie", None],
    })
    frame.write_parquet(parquet, compression="zstd")
    snapshot = tmp_path / "snapshot.json"
    generate_snapshot(str(output), str(snapshot))
    return output, parquet, frame, snapshot


def test_parquet_entry_has_data_hash_and_no_file_hash(parquet_case):
    _, parquet, _, snapshot = parquet_case
    entry = json.loads(snapshot.read_text())["files"][parquet.name]
    assert "file_hash" not in entry
    assert len(entry["data_hash"]) == 64
    assert entry["dtypes"] == {"code": "String", "label": "String"}


@pytest.mark.parametrize("kwargs", [
    {"compression": "snappy"},
    {"compression": "uncompressed"},
    {"compression": "zstd", "row_group_size": 1},
    {"compression": "gzip", "statistics": False},
])
def test_parquet_layout_does_not_change_data_hash(parquet_case, tmp_path, kwargs):
    output, parquet, frame, snapshot = parquet_case
    before = parquet.read_bytes()
    frame.write_parquet(parquet, **kwargs)
    assert parquet.read_bytes() != before
    assert validate_snapshot(str(output), str(snapshot), strict_files=True) == (True, [], [])
    regenerated = tmp_path / "regenerated.json"
    generate_snapshot(str(output), str(regenerated))
    assert (
        json.loads(regenerated.read_text())["files"]
        == json.loads(snapshot.read_text())["files"]
    )


@pytest.mark.parametrize("column, values", [
    ("code", ["0001", "0002", None, "9999"]),   # changed value
    ("code", ["0001", "0002", "", "0004"]),     # null became empty string
    ("label", ["Een", None, "Drie", None]),     # empty string became null
    ("label", ["Ee", "n", "Drie", None]),       # same concatenation, other split
])
def test_one_changed_cell_fails_without_echoing_values(parquet_case, column, values):
    output, parquet, frame, snapshot = parquet_case
    frame.with_columns(pl.Series(column, values)).write_parquet(parquet)
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert errors == [f"{parquet.name}: inhoud gewijzigd (data-hash verschilt)"]
    for value in ("0001", "0002", "0004", "9999", "Een", "Drie"):
        assert not any(value in error for error in errors)


def test_parquet_column_order_is_part_of_data_hash(parquet_case):
    output, parquet, frame, snapshot = parquet_case
    frame.select(["label", "code"]).write_parquet(parquet)
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert errors == [f"{parquet.name}: inhoud gewijzigd (data-hash verschilt)"]


def test_v2_snapshot_cannot_omit_parquet_data_hash(parquet_case):
    output, parquet, _, snapshot = parquet_case
    data = json.loads(snapshot.read_text())
    del data["files"][parquet.name]["data_hash"]
    # A raw-bytes hash does not stand in for the data hash.
    data["files"][parquet.name]["file_hash"] = hashlib.sha256(parquet.read_bytes()).hexdigest()
    snapshot.write_text(json.dumps(data))
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert errors == [f"{parquet.name}: data-hash ontbreekt in de snapshot"]


def test_csv_with_crlf_line_endings_validates(snapshot_case):
    output, csv, snapshot = snapshot_case
    csv.write_bytes(csv.read_bytes().replace(b"\n", b"\r\n"))
    assert validate_snapshot(str(output), str(snapshot), strict_files=True) == (True, [], [])


def test_csv_with_crlf_and_changed_value_still_fails(snapshot_case):
    output, csv, snapshot = snapshot_case
    csv.write_bytes(b"code;label\r\n0002;Een\r\n")
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert errors == [f"{csv.name}: inhoud gewijzigd (SHA256 verschilt)"]

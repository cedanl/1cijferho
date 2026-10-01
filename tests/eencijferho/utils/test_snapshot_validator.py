"""Regression gates must verify readable content, not just matching shapes."""

import hashlib
import json

import polars as pl
import pytest

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
    assert any("SHA256" in error for error in errors)


@pytest.mark.parametrize("content", [b"code;label\n\xff;fout\n", b'code;label\n1;"niet afgesloten\n'])
def test_unreadable_csv_never_passes(snapshot_case, content):
    output, csv, snapshot = snapshot_case
    csv.write_bytes(content)
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("onleesbaar" in error for error in errors)


def test_missing_file_fails(snapshot_case):
    output, csv, snapshot = snapshot_case
    csv.unlink()
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("Bestand ontbreekt" in error for error in errors)


def test_unexpected_file_fails_by_default(snapshot_case):
    output, _, snapshot = snapshot_case
    (output / "EV-old.csv").write_text("code\n1\n", encoding="utf-8")
    passed, errors, _ = validate_snapshot(str(output), str(snapshot))
    assert not passed
    assert any("Onverwacht bestand" in error for error in errors)
    passed, errors, warnings = validate_snapshot(str(output), str(snapshot), strict_files=False)
    assert passed and not errors
    assert any("Onverwacht bestand" in warning for warning in warnings)


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

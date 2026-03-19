"""
Unit tests for eencijferho.utils.value_validation

Tests:
    - test_valid_data: all column values are allowed -> success=True
    - test_invalid_data: column contains disallowed value -> success=False with details
    - test_missing_column: metadata has column not in CSV -> gracefully skipped
    - test_reference_skipped: columns with 'reference' values are not validated
"""

import json
import tempfile
import os
import pytest

from eencijferho.utils.value_validation import validate_column_values


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

METADATA = [
    {
        "name": "Opleidingsvorm",
        "description": "Code voor de studievorm.",
        "values": {"1": "voltijd", "2": "deeltijd", "3": "duaal"},
    },
    {
        "name": "Geslacht",
        "description": "Geslacht van de student.",
        "values": {"M": "Man", "V": "Vrouw", "O": "Onbekend"},
    },
    {
        "name": "Instellingscode",
        "description": "BRIN nummer.",
        "values": {"reference": "Zie bestand instellingscodes.xlsx"},
    },
]

CSV_VALID = "Opleidingsvorm;Geslacht\n1;M\n2;V\n3;O\n1;M\n"
CSV_INVALID = "Opleidingsvorm;Geslacht\n1;M\n9;V\n2;X\n"  # 9 and X are not allowed


def _write_temp(content: str, suffix: str) -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(content)
    return path


@pytest.fixture
def metadata_file():
    path = _write_temp(json.dumps(METADATA, ensure_ascii=False), ".json")
    yield path
    os.unlink(path)


@pytest.fixture
def valid_csv():
    path = _write_temp(CSV_VALID, ".csv")
    yield path
    os.unlink(path)


@pytest.fixture
def invalid_csv():
    path = _write_temp(CSV_INVALID, ".csv")
    yield path
    os.unlink(path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_valid_data(valid_csv, metadata_file):
    """All values are allowed: expect success=True and no failed columns."""
    success, results = validate_column_values(valid_csv, metadata_file)

    assert success is True
    assert results["total_issues"] == 0
    assert results["columns_failed"] == 0
    assert results["columns_checked"] == 2

    for col_res in results["column_results"]:
        assert col_res["status"] == "ok"
        assert col_res["invalid_values"] == []


def test_invalid_data(invalid_csv, metadata_file):
    """Column contains disallowed values: expect success=False with details."""
    success, results = validate_column_values(invalid_csv, metadata_file)

    assert success is False
    assert results["total_issues"] > 0
    assert results["columns_failed"] > 0

    failed = [r for r in results["column_results"] if r["status"] == "failed"]
    assert len(failed) > 0

    failed_col_names = {r["column"] for r in failed}
    # "9" is not valid for Opleidingsvorm, "X" is not valid for Geslacht
    assert "Opleidingsvorm" in failed_col_names or "Geslacht" in failed_col_names

    for r in failed:
        assert len(r["invalid_values"]) > 0


def test_reference_column_skipped(valid_csv, metadata_file):
    """Columns with 'reference' values should not appear in column_results."""
    _, results = validate_column_values(valid_csv, metadata_file)

    checked_cols = {r["column"] for r in results["column_results"]}
    # Instellingscode has a 'reference' value and is not present in the CSV,
    # but even if it were, it should be skipped
    assert "Instellingscode" not in checked_cols


def test_missing_column_gracefully_skipped(metadata_file):
    """CSV without a column defined in metadata: column is skipped silently."""
    csv_content = "Geslacht\nM\nV\n"
    path = _write_temp(csv_content, ".csv")
    try:
        success, results = validate_column_values(path, metadata_file)
        # Only Geslacht is in the CSV, Opleidingsvorm is missing -> checked=1
        assert results["columns_checked"] == 1
        assert success is True
    finally:
        os.unlink(path)


def test_missing_metadata_file(valid_csv):
    """Non-existent metadata file returns success=False with load_error."""
    success, results = validate_column_values(valid_csv, "/nonexistent/path.json")

    assert success is False
    assert "load_error" in results

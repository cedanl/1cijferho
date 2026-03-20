"""
Shared pytest fixtures for the 1CijferHO test suite.

Fixtures provide small, synthetic test data that mirrors real DUO file formats
without using actual sensitive data. All file I/O uses tmp_path so tests are
fully isolated.
"""

import json
from pathlib import Path

import pytest
import pandas as pd

# ---------------------------------------------------------------------------
# Fixed-width ASCII fixture
# ---------------------------------------------------------------------------
FIXED_WIDTH_ROWS = [
    b"Jan       001  ",
    b"Piet      042  ",
    b"Katrijn   007  ",
]
FIXED_WIDTH_WIDTHS = [10, 5]
FIXED_WIDTH_NAMES = ["Naam", "Waarde"]
FIXED_WIDTH_POSITIONS = [(0, 10), (10, 15)]


@pytest.fixture
def fixed_width_file(tmp_path):
    """A tiny latin-1 fixed-width .asc file with two fields."""
    path = tmp_path / "test_data.asc"
    path.write_bytes(b"\n".join(FIXED_WIDTH_ROWS) + b"\n")
    return path


# ---------------------------------------------------------------------------
# Metadata Excel (.xlsx) fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def metadata_xlsx(tmp_path):
    """Matching Excel metadata file for fixed_width_file."""
    path = tmp_path / "Bestandsbeschrijving_test.xlsx"
    df = pd.DataFrame(
        {
            "ID": [1, 2],
            "Naam": FIXED_WIDTH_NAMES,
            "Startpositie": [1, 11],  # 1-based as in real DUO files
            "Aantal posities": FIXED_WIDTH_WIDTHS,
            "Opmerking": ["", ""],
        }
    )
    df.to_excel(path, index=False)
    return path


# ---------------------------------------------------------------------------
# CSV helper fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def make_csv():
    """Factory: Write a semicolon-delimited CSV file from a list of row dicts."""

    def _make(path: Path, rows: list[dict]) -> None:
        if not rows:
            return
        cols = list(rows[0].keys())
        lines = [";".join(cols)] + [";".join(str(row[c]) for c in cols) for row in rows]
        path.write_text("\n".join(lines), encoding="utf-8")

    return _make


# ---------------------------------------------------------------------------
# Parameterized/factory log file fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def make_validation_log(tmp_path):
    """Factory: Write a validation log with custom processed_files list. Returns Path."""

    def _make(entries, filename="(3)_xlsx_validation_log_latest.json"):
        data = {"timestamp": "20240101_000000", "processed_files": entries}
        p = tmp_path / "logs" / filename
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return p

    return _make


@pytest.fixture
def make_conversion_log(tmp_path):
    """Factory: Write a conversion log with custom 'details' list. Returns Path."""

    def _make(details, filename="conversion_log.json"):
        data = {"timestamp": "20240101_000001", "details": details}
        p = tmp_path / filename
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return p

    return _make


@pytest.fixture
def make_match_log(tmp_path):
    """Factory: Write a match log with custom 'processed_files' list. Returns Path."""

    def _make(files, filename="match_log.json"):
        data = {"timestamp": "20240101_000000", "processed_files": files}
        p = tmp_path / filename
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return p

    return _make


# ---------------------------------------------------------------------------
# Generic temp file fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def make_temp_file(tmp_path):
    """Factory: Write arbitrary text content to a unique temp file. Returns Path.

    Each call within the same test gets a uniquely named file inside tmp_path,
    so cleanup is handled automatically by pytest.
    """
    _counter = [0]

    def _make(content: str, suffix: str = ".txt", encoding: str = "utf-8") -> Path:
        _counter[0] += 1
        path = tmp_path / f"temp_{_counter[0]}{suffix}"
        path.write_text(content, encoding=encoding)
        return path

    return _make

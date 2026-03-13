"""
Shared pytest fixtures for the 1CijferHO test suite.

Fixtures provide small, synthetic test data that mirrors real DUO file formats
without using actual sensitive data. All file I/O uses tmp_path so tests are
fully isolated.
"""

import json
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
# Metadata .txt fixture (raw DUO format)
# ---------------------------------------------------------------------------
METADATA_TXT_CONTENT = """\
Bestandsbeschrijving testbestand
=================================

Tabel overzicht
===============

Naam                          Startpositie   Aantal posities
Naam                                    1            10
Waarde                                 11             5

Ten behoeve van de decodering
* Waarde

"""


@pytest.fixture
def metadata_txt(tmp_path):
    """A DUO-style Bestandsbeschrijving .txt file."""
    path = tmp_path / "Bestandsbeschrijving_test.txt"
    path.write_text(METADATA_TXT_CONTENT, encoding="latin-1")
    return path


# ---------------------------------------------------------------------------
# Minimal match-log JSON fixture (static)
# ---------------------------------------------------------------------------
@pytest.fixture
def match_log(tmp_path):
    """A minimal file_matching_log JSON as produced by converter_match."""
    data = {
        "timestamp": "20240101_120000",
        "processed_files": [
            {
                "input_file": "EV2023.asc",
                "row_count": 3,
                "status": "matched",
                "matches": [
                    {
                        "validation_file": "Bestandsbeschrijving_1cyferho_2023.xlsx",
                        "validation_status": "success",
                    }
                ],
            },
            {
                "input_file": "UNKNOWN.asc",
                "row_count": 5,
                "status": "unmatched",
                "matches": [],
            },
        ],
    }
    path = tmp_path / "match_log.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Minimal conversion-log JSON fixture (static)
# ---------------------------------------------------------------------------
@pytest.fixture
def conversion_log(tmp_path):
    """A minimal conversion_log JSON as produced by the converter."""
    data = {
        "timestamp": "20240101_120001",
        "details": [
            {
                "input_file": "EV2023.asc",
                "status": "success",
                "output_file": "EV2023.csv",
                "total_lines": 3,
            }
        ],
    }
    path = tmp_path / "conversion_log.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path


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
        p.write_text(json.dumps(data, indent=2), encoding="latin-1")
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

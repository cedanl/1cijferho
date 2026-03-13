# Tests for eencijferho.core.extractor: txt→JSON→XLSX pipeline

import json
import pytest
import pandas as pd
from pathlib import Path

from eencijferho.core.extractor import (
    extract_decoding_variables,
    extract_tables_from_txt,
    extract_excel_from_json,
)

# Minimal bestandsbeschrijving .txt with one table and one decoding variable
SAMPLE_TXT = """\
Tabel overzicht
===============

Naam                          Startpositie   Aantal posities
Naam                                    1            10
Waarde                                 11             5

Ten behoeve van de decodering
* Waarde

"""

# JSON matching the structure produced by extract_tables_from_txt
SAMPLE_JSON = {
    "filename": "Bestandsbeschrijving_test",
    "tables": [
        {
            "table_number": 1,
            "table_title": "Tabel overzicht",
            "content": [
                "Naam                          Startpositie   Aantal posities",
                "Naam                                    1            10",
                "Waarde                                 11             5",
            ],
            "decoding_variables": ["Waarde"],
        }
    ],
}


@pytest.fixture
def txt_file(tmp_path):
    p = tmp_path / "Bestandsbeschrijving_test.txt"
    p.write_text(SAMPLE_TXT, encoding="latin-1")
    return p


@pytest.fixture
def json_file(tmp_path):
    p = tmp_path / "Bestandsbeschrijving_test.json"
    p.write_text(json.dumps(SAMPLE_JSON, ensure_ascii=False), encoding="latin-1")
    return p


# --- extract_decoding_variables ---


def test_decoding_variables_extracts_bullets():
    lines = (
        "some preamble\nTen behoeve van de decodering\n* Geslacht\n* Nationaliteit\n"
    ).splitlines()
    assert extract_decoding_variables(lines, 0) == ["Geslacht", "Nationaliteit"]


def test_decoding_variables_vertaling_variant():
    lines = "Ten behoeve van de vertaling\n* Opleiding\n".splitlines()
    assert extract_decoding_variables(lines, 0) == ["Opleiding"]


@pytest.mark.parametrize(
    "stop_line",
    ["NB: some note", "Opmerking: zie bijlage", "======"],
)
def test_decoding_variables_stops_at_boundary(stop_line):
    lines = (
        f"Ten behoeve van de decodering\n* VarA\n{stop_line}\n* VarB\n"
    ).splitlines()
    result = extract_decoding_variables(lines, 0)
    assert "VarA" in result
    assert "VarB" not in result


def test_decoding_variables_empty_when_no_section():
    lines = "Just some random text\nNo decoding section here\n".splitlines()
    assert extract_decoding_variables(lines, 0) == []


def test_decoding_variables_skips_blank_lines_between_bullets():
    lines = ("Ten behoeve van de decodering\n* VarA\n\n* VarB\n").splitlines()
    result = extract_decoding_variables(lines, 0)
    assert "VarA" in result
    assert "VarB" in result


def test_decoding_variables_start_index_respected():
    lines = (
        "Ten behoeve van de decodering\n"
        "* EarlyVar\n"
        "---\n"
        "Ten behoeve van de decodering\n"
        "* LateVar\n"
    ).splitlines()
    assert extract_decoding_variables(lines, 3) == ["LateVar"]


# --- extract_tables_from_txt ---


def test_extract_tables_returns_json_path(txt_file, tmp_path):
    result = extract_tables_from_txt(str(txt_file), str(tmp_path / "json"))
    assert result is not None
    assert Path(result).exists()
    assert Path(result).suffix == ".json"


def test_extract_tables_json_has_tables_key(txt_file, tmp_path):
    result = extract_tables_from_txt(str(txt_file), str(tmp_path / "json"))
    data = json.loads(Path(result).read_text(encoding="latin-1"))
    assert "tables" in data
    assert len(data["tables"]) >= 1


def test_extract_tables_title_extracted(txt_file, tmp_path):
    result = extract_tables_from_txt(str(txt_file), str(tmp_path / "json"))
    data = json.loads(Path(result).read_text(encoding="latin-1"))
    titles = [t["table_title"] for t in data["tables"]]
    assert "Tabel overzicht" in titles


def test_extract_tables_content_not_empty(txt_file, tmp_path):
    result = extract_tables_from_txt(str(txt_file), str(tmp_path / "json"))
    data = json.loads(Path(result).read_text(encoding="latin-1"))
    assert all(len(t["content"]) > 0 for t in data["tables"])


def test_extract_tables_decoding_variables_present(txt_file, tmp_path):
    result = extract_tables_from_txt(str(txt_file), str(tmp_path / "json"))
    data = json.loads(Path(result).read_text(encoding="latin-1"))
    all_vars = [v for t in data["tables"] for v in t.get("decoding_variables", [])]
    assert "Waarde" in all_vars


def test_extract_tables_returns_none_for_missing_file(tmp_path):
    assert (
        extract_tables_from_txt(str(tmp_path / "missing.txt"), str(tmp_path / "json"))
        is None
    )


def test_extract_tables_creates_output_dir(txt_file, tmp_path):
    json_dir = tmp_path / "nested" / "json"
    extract_tables_from_txt(str(txt_file), str(json_dir))
    assert json_dir.exists()


# --- extract_excel_from_json ---


def test_extract_excel_returns_tuple(json_file, tmp_path):
    results, files_created, total_tables = extract_excel_from_json(
        str(json_file), str(tmp_path)
    )
    assert isinstance(results, list)
    assert isinstance(files_created, int)
    assert isinstance(total_tables, int)


def test_extract_excel_creates_xlsx(json_file, tmp_path):
    extract_excel_from_json(str(json_file), str(tmp_path))
    assert len(list(tmp_path.glob("*.xlsx"))) >= 1


def test_extract_excel_required_columns(json_file, tmp_path):
    extract_excel_from_json(str(json_file), str(tmp_path))
    xlsx = next(tmp_path.glob("*.xlsx"))
    df = pd.read_excel(xlsx, sheet_name="Table")
    assert {"ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"}.issubset(
        set(df.columns)
    )


def test_extract_excel_row_count(json_file, tmp_path):
    extract_excel_from_json(str(json_file), str(tmp_path))
    xlsx = next(tmp_path.glob("*.xlsx"))
    df = pd.read_excel(xlsx, sheet_name="Table")
    assert len(df) == 2  # Naam + Waarde


def test_extract_excel_decoding_variables_sheet(json_file, tmp_path):
    extract_excel_from_json(str(json_file), str(tmp_path))
    xlsx = next(tmp_path.glob("*.xlsx"))
    assert "DecodingVariables" in pd.ExcelFile(xlsx).sheet_names


def test_extract_excel_decoding_variables_content(json_file, tmp_path):
    extract_excel_from_json(str(json_file), str(tmp_path))
    xlsx = next(tmp_path.glob("*.xlsx"))
    df = pd.read_excel(xlsx, sheet_name="DecodingVariables")
    assert "Waarde" in df["DecodingVariables"].values


def test_extract_excel_total_tables_count(json_file, tmp_path):
    _, _, total_tables = extract_excel_from_json(str(json_file), str(tmp_path))
    assert total_tables == 1


def test_extract_excel_corrupt_json_returns_empty(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not json", encoding="latin-1")
    _, files_created, total_tables = extract_excel_from_json(str(bad), str(tmp_path))
    assert files_created == 0
    assert total_tables == 0

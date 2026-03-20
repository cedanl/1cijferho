# Tests for eencijferho.core.extractor — encoding correctheid

import json
from pathlib import Path

import pytest

from eencijferho.core.extractor import (
    extract_tables_from_txt,
    process_txt_folder,
    _find_title_above,
    _parse_tables,
    _parse_data_line,
    _build_table_rows,
    _write_table_excel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bestandsbeschrijving(path: Path, title: str = "TestTabel") -> Path:
    """Minimale DUO-achtige bestandsbeschrijving in latin-1."""
    content = (
        f"{title}\n"
        "================\n"
        "Startpositie  Naam\n"
        "1             VeldA\n"
        "\n"
    )
    path.write_text(content, encoding="latin-1")
    return path


# ---------------------------------------------------------------------------
# extract_tables_from_txt
# ---------------------------------------------------------------------------


def test_extract_returns_json_path(tmp_path):
    txt = _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_test.txt")
    result = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    assert result is not None
    assert result.endswith(".json")


def test_extract_json_is_valid_utf8(tmp_path):
    """Het geschreven JSON-bestand moet als utf-8 leesbaar zijn."""
    txt = _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_test.txt")
    json_path = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    assert "tables" in data


def test_extract_preserves_accented_title(tmp_path):
    """Een tabelnaam met accenten (uit latin-1 bron) moet intact in de utf-8 JSON staan."""
    txt = _make_bestandsbeschrijving(
        tmp_path / "Bestandsbeschrijving_test.txt",
        title="Vóór het HO",
    )
    json_path = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    titles = [t["table_title"] for t in data["tables"]]
    assert any("V" in t and "r het HO" in t for t in titles), (
        f"Verwachtte 'Vóór het HO' in tabelnamen, maar kreeg: {titles}"
    )


def test_extract_returns_none_for_missing_file(tmp_path):
    result = extract_tables_from_txt(str(tmp_path / "bestaat_niet.txt"), str(tmp_path))
    assert result is None


def test_extract_returns_none_when_no_tables(tmp_path):
    txt = tmp_path / "Bestandsbeschrijving_leeg.txt"
    txt.write_text("Geen tabellen hier\n", encoding="latin-1")
    result = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    assert result is None


# ---------------------------------------------------------------------------
# process_txt_folder
# ---------------------------------------------------------------------------


def test_process_txt_folder_returns_list(tmp_path):
    _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_test.txt")
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    assert isinstance(result, list)


def test_process_txt_folder_returns_extracted_paths(tmp_path):
    _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_A.txt")
    _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_B.txt")
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    assert len(result) == 2
    assert all(p.endswith(".json") for p in result)


def test_process_txt_folder_json_files_are_utf8(tmp_path):
    """Alle door process_txt_folder aangemaakte JSON-bestanden moeten utf-8 zijn."""
    _make_bestandsbeschrijving(
        tmp_path / "Bestandsbeschrijving_test.txt",
        title="Tabel met é en ë",
    )
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    for json_path in result:
        with open(json_path, encoding="utf-8") as f:
            json.load(f)  # mag geen UnicodeDecodeError geven


def test_process_txt_folder_empty_dir_returns_empty_list(tmp_path):
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    assert result == []


# ---------------------------------------------------------------------------
# _find_title_above
# ---------------------------------------------------------------------------


def test_find_title_above_returns_title():
    lines = ["TabelNaam", "================", "Startpositie  Aantal posities"]
    assert _find_title_above(lines, 2) == "TabelNaam"


def test_find_title_above_returns_empty_when_no_divider():
    lines = ["TabelNaam", "Startpositie  Aantal posities"]
    assert _find_title_above(lines, 1) == ""


def test_find_title_above_returns_empty_when_title_is_blank():
    lines = ["", "================", "Startpositie  Aantal posities"]
    assert _find_title_above(lines, 2) == ""


# ---------------------------------------------------------------------------
# _parse_tables
# ---------------------------------------------------------------------------


def _make_lines(title: str = "Tabel") -> list[str]:
    """Minimal bestandsbeschrijving-style line list."""
    return [
        title,
        "================",
        "Naam          Startpositie  Aantal posities",
        "VeldA         1             3",
        "",
    ]


def test_parse_tables_returns_one_table():
    tables = _parse_tables(_make_lines())
    assert len(tables) == 1


def test_parse_tables_title_extracted():
    tables = _parse_tables(_make_lines("MijnTabel"))
    assert tables[0]["table_title"] == "MijnTabel"


def test_parse_tables_content_has_header_and_data():
    tables = _parse_tables(_make_lines())
    content = tables[0]["content"]
    assert any("startpositie" in line.lower() for line in content)
    assert any("VeldA" in line for line in content)


def test_parse_tables_empty_lines_returns_empty():
    assert _parse_tables([]) == []
    assert _parse_tables(["Geen tabellen hier", ""]) == []


def test_parse_tables_table_without_trailing_blank():
    """Table at EOF with no trailing blank line must still be captured."""
    lines = [
        "Tabel",
        "================",
        "Naam          Startpositie  Aantal posities",
        "VeldA         1             3",
        # no trailing blank
    ]
    tables = _parse_tables(lines)
    assert len(tables) == 1


def test_parse_tables_two_tables():
    lines = (
        _make_lines("Tabel1")
        + ["Tabel2", "================"]
        + ["Naam          Startpositie  Aantal posities", "VeldB         4             2", ""]
    )
    tables = _parse_tables(lines)
    assert len(tables) == 2
    assert tables[1]["table_title"] == "Tabel2"


# ---------------------------------------------------------------------------
# _parse_data_line
# ---------------------------------------------------------------------------

# A realistic header line: "Naam          Startpositie  Aantal posities  Opmerking"
_HEADER = "Naam          Startpositie  Aantal posities  Opmerking"
_SP = _HEADER.find("Startpositie")
_AP = _HEADER.find("Aantal posities")


def test_parse_data_line_normal():
    line = "Persoonsid    1             4"
    result = _parse_data_line(line, _SP, _AP)
    assert result is not None
    field, start, aantal, comment = result
    assert field.strip() == "Persoonsid"
    assert start == 1
    assert aantal == 4


def test_parse_data_line_with_comment():
    line = "Geslacht      5             1                Zie Dec_geslacht"
    result = _parse_data_line(line, _SP, _AP)
    assert result is not None
    _, _, _, comment = result
    assert "Dec_geslacht" in comment


def test_parse_data_line_repeated_header_keywords_skipped():
    # Lines containing both header keywords are treated as repeated headers and skipped.
    line = "Persoonsid    Startpositie 1  Aantal posities 4"
    result = _parse_data_line(line, _SP, _AP)
    assert result is None


def test_parse_data_line_too_short_returns_none():
    result = _parse_data_line("ab", 20, 35)
    assert result is None


def test_parse_data_line_no_digits_returns_none():
    result = _parse_data_line("Naam          geen  geen", _SP, _AP)
    assert result is None


# ---------------------------------------------------------------------------
# _build_table_rows
# ---------------------------------------------------------------------------


def test_build_table_rows_returns_header_plus_data():
    content = [
        _HEADER,
        "Persoonsid    1             4",
        "Geslacht      5             1",
    ]
    rows, valid = _build_table_rows(content, _SP, _AP)
    assert rows[0] == ["ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"]
    assert valid == 2
    assert len(rows) == 3  # header + 2 data rows


def test_build_table_rows_skips_blank_lines():
    content = [_HEADER, "", "Persoonsid    1             4", ""]
    rows, valid = _build_table_rows(content, _SP, _AP)
    assert valid == 1


def test_build_table_rows_row_ids_are_sequential():
    content = [_HEADER, "VeldA         1             3", "VeldB         4             2"]
    rows, _ = _build_table_rows(content, _SP, _AP)
    assert rows[1][0] == 1
    assert rows[2][0] == 2


# ---------------------------------------------------------------------------
# _write_table_excel
# ---------------------------------------------------------------------------


def test_write_table_excel_creates_file(tmp_path):
    rows = [
        ["ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"],
        [1, "VeldA", 1, 3, ""],
    ]
    out = str(tmp_path / "test.xlsx")
    n = _write_table_excel(rows, [], out)
    assert n == 1
    assert Path(out).exists()


def test_write_table_excel_with_decoding_variables(tmp_path):
    rows = [
        ["ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"],
        [1, "VeldA", 1, 3, ""],
    ]
    out = str(tmp_path / "test_dec.xlsx")
    n = _write_table_excel(rows, ["VeldA", "VeldB"], out)
    assert n == 1
    import pandas as pd
    sheets = pd.ExcelFile(out).sheet_names
    assert "DecodingVariables" in sheets

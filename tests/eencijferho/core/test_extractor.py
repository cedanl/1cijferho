# Tests for eencijferho.core.extractor — encoding correctheid

import json
import sys
from pathlib import Path

import openpyxl
import pytest

from eencijferho.core.extractor import (
    extract_tables_from_txt,
    process_txt_folder,
    get_fwf_params,
    list_fwf_tables,
    _find_title_above,
    _parse_tables,
    _parse_data_line,
    _build_table_rows,
    _write_table_excel,
    write_variable_metadata,
)
from eencijferho.cli import main as cli_main
from eencijferho.io.text_encoding import DescriptionEncodingError


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


def test_process_txt_folder_counts_no_tables_found(tmp_path, capsys):
    """Bestanden zonder tabellen worden geteld, niet als losse log-entry opgenomen (#210)."""
    _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_A.txt")
    (tmp_path / "Dec_test.asc").write_text("01Een\n02Twee\n", encoding="latin-1")
    (tmp_path / "EV_DEMO.asc").write_text("0000000012023\n", encoding="latin-1")

    process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "meta" / "json"))

    log_path = tmp_path / "meta" / "logs" / "(1)_json_processing_log_latest.json"
    log = json.loads(log_path.read_text(encoding="utf-8"))
    assert log["no_tables_found_count"] == 2
    assert sorted(log["no_tables_found_files"]) == ["Dec_test.asc", "EV_DEMO.asc"]
    assert [f["file"] for f in log["processed_files"]] == ["Bestandsbeschrijving_A.txt"]
    assert all(f["status"] != "no_tables_found" for f in log["processed_files"])
    assert log["total_files_processed"] == 3
    assert log["total_files_extracted"] == 1

    out = capsys.readouterr().out
    assert "No tables found in 2 files" in out
    assert "Dec_test.asc" not in out


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
    sheets = openpyxl.load_workbook(out).sheetnames
    assert "DecodingVariables" in sheets


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_fwf_txt(path: Path, fields: list[tuple[str, int, int]]) -> Path:
    """Write a minimal DUO bestandsbeschrijving with the given (name, start, length) fields."""
    header = "Naam                  Startpositie  Aantal posities"
    sp = header.find("Startpositie")
    ap = header.find("Aantal posities")
    lines = [
        "TestTabel",
        "================",
        header,
    ]
    for name, start, length in fields:
        # Pad name to startpositie column, then right-align numbers
        padded = name.ljust(sp)
        start_str = str(start).ljust(ap - sp)
        lines.append(f"{padded}{start_str}{length}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="latin-1")
    return path


# ---------------------------------------------------------------------------
# get_fwf_params
# ---------------------------------------------------------------------------


def test_get_fwf_params_returns_names_and_colspecs(tmp_path):
    txt = _make_fwf_txt(
        tmp_path / "Bestandsbeschrijving_test.txt",
        [("VeldA", 1, 3), ("VeldB", 4, 2)],
    )
    params = get_fwf_params(str(txt))
    assert params["names"] == ["VeldA", "VeldB"]
    assert params["colspecs"] == [(0, 3), (3, 5)]


def test_get_fwf_params_colspecs_zero_based(tmp_path):
    """Startpositie 1 → colspec start 0 (DUO is 1-based, pandas is 0-based)."""
    txt = _make_fwf_txt(tmp_path / "Bestandsbeschrijving_test.txt", [("Veld", 1, 5)])
    params = get_fwf_params(str(txt))
    assert params["colspecs"][0] == (0, 5)


def test_get_fwf_params_single_field(tmp_path):
    txt = _make_fwf_txt(tmp_path / "Bestandsbeschrijving_test.txt", [("Enkel", 3, 4)])
    params = get_fwf_params(str(txt))
    assert params["names"] == ["Enkel"]
    assert params["colspecs"] == [(2, 6)]


def test_get_fwf_params_raises_for_missing_file(tmp_path):
    with pytest.raises(ValueError, match="niet lezen"):
        get_fwf_params(str(tmp_path / "bestaat_niet.txt"))


def test_get_fwf_params_raises_for_no_tables(tmp_path):
    txt = tmp_path / "Bestandsbeschrijving_leeg.txt"
    txt.write_text("Geen tabellen\n", encoding="latin-1")
    with pytest.raises(ValueError, match="Geen tabellen"):
        get_fwf_params(str(txt))


def test_get_fwf_params_raises_for_invalid_table_index(tmp_path):
    txt = _make_fwf_txt(tmp_path / "Bestandsbeschrijving_test.txt", [("VeldA", 1, 3)])
    with pytest.raises(ValueError, match="buiten bereik"):
        get_fwf_params(str(txt), table_index=99)


def test_get_fwf_params_keys_are_names_and_colspecs(tmp_path):
    txt = _make_fwf_txt(tmp_path / "Bestandsbeschrijving_test.txt", [("VeldA", 1, 3)])
    params = get_fwf_params(str(txt))
    assert set(params.keys()) == {"names", "colspecs"}


def test_get_fwf_params_names_and_colspecs_same_length(tmp_path):
    txt = _make_fwf_txt(
        tmp_path / "Bestandsbeschrijving_test.txt",
        [("A", 1, 2), ("B", 3, 4), ("C", 7, 1)],
    )
    params = get_fwf_params(str(txt))
    assert len(params["names"]) == len(params["colspecs"]) == 3


# ---------------------------------------------------------------------------
# list_fwf_tables
# ---------------------------------------------------------------------------


def test_list_fwf_tables_single_table(tmp_path):
    txt = _make_fwf_txt(tmp_path / "Bestandsbeschrijving_test.txt", [("VeldA", 1, 3)])
    names = list_fwf_tables(str(txt))
    assert names == ["TestTabel"]


def test_list_fwf_tables_two_tables(tmp_path):
    header = "Naam                  Startpositie  Aantal posities"
    sp = header.find("Startpositie")
    content = "\n".join([
        "TabelEen",
        "================",
        header,
        "VeldA".ljust(sp) + "1             3",
        "",
        "TabelTwee",
        "================",
        header,
        "VeldB".ljust(sp) + "1             5",
        "",
    ])
    txt = tmp_path / "Bestandsbeschrijving_multi.txt"
    txt.write_text(content, encoding="latin-1")
    names = list_fwf_tables(str(txt))
    assert names == ["TabelEen", "TabelTwee"]


def test_list_fwf_tables_returns_empty_for_missing_file(tmp_path):
    result = list_fwf_tables(str(tmp_path / "bestaat_niet.txt"))
    assert result == []


def test_list_fwf_tables_returns_empty_for_no_tables(tmp_path):
    txt = tmp_path / "Bestandsbeschrijving_leeg.txt"
    txt.write_text("Geen tabellen\n", encoding="latin-1")
    result = list_fwf_tables(str(txt))
    assert result == []


# ---------------------------------------------------------------------------
# Tekencodering van bestandsbeschrijvingen (#203)
# ---------------------------------------------------------------------------

DEMO_DIR = Path(__file__).resolve().parents[3] / "data" / "01-input" / "DEMO"
VOOROPLEIDING = "Hoogste vooropleiding vóór het HO"


def _dec_description(name: str) -> str:
    """Minimale Dec-bestandsbeschrijving met één decodeervariabele."""
    return (
        "Dec_vooropl.asc\n"
        "===============\n"
        "Code vooropleiding    Startpositie  Aantal posities\n"
        "Code                  1             5\n"
        "\n"
        "Ten behoeve van de decodering van de velden:\n"
        f"* {name}\n"
        "\n"
    )


def _decoding_variables(json_path: str) -> list[str]:
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    return [v for t in data["tables"] for v in t["decoding_variables"]]


def test_mixed_encodings_give_same_name(tmp_path):
    """Latin-1, UTF-8 en UTF-8 met BOM leveren dezelfde naam in de JSON."""
    inp = tmp_path / "in"
    inp.mkdir()
    content = _dec_description(VOOROPLEIDING)
    (inp / "Bestandsbeschrijving_A.txt").write_bytes(content.encode("latin-1"))
    (inp / "Bestandsbeschrijving_B.txt").write_bytes(content.encode("utf-8"))
    (inp / "Bestandsbeschrijving_C.txt").write_bytes(content.encode("utf-8-sig"))
    result = process_txt_folder(str(inp), json_output_folder=str(tmp_path / "json"))
    assert len(result) == 3
    for json_path in result:
        assert _decoding_variables(json_path) == [VOOROPLEIDING]


def test_replacement_char_stops_process_txt_folder(tmp_path):
    """U+FFFD in een bestandsbeschrijving geeft een fout, geen 'no tables found'."""
    inp = tmp_path / "in"
    inp.mkdir()
    bad = inp / "Bestandsbeschrijving_Dec-bestanden.txt"
    bad.write_bytes(_dec_description("Hoogste vooropleiding v\ufffd\ufffdr het HO").encode("utf-8"))
    with pytest.raises(DescriptionEncodingError, match="Bestandsbeschrijving_Dec-bestanden.txt") as exc_info:
        process_txt_folder(str(inp), json_output_folder=str(tmp_path / "json"))
    assert exc_info.value.line_numbers == [7]


def test_replacement_char_stops_write_variable_metadata(tmp_path):
    """De fout wordt niet ingeslikt door de except in write_variable_metadata."""
    inp = tmp_path / "in"
    inp.mkdir()
    (inp / "Bestandsbeschrijving_x.txt").write_bytes("Naam\n----\nv\ufffdr\n".encode("utf-8"))
    with pytest.raises(DescriptionEncodingError):
        write_variable_metadata(input_dir=str(inp), json_folder=str(tmp_path / "json"))


def test_replacement_char_in_get_fwf_params_and_list_fwf_tables(tmp_path):
    txt = tmp_path / "Bestandsbeschrijving_x.txt"
    txt.write_bytes(_dec_description("v\ufffdr").encode("utf-8"))
    with pytest.raises(DescriptionEncodingError):
        get_fwf_params(str(txt))
    with pytest.raises(DescriptionEncodingError):
        list_fwf_tables(str(txt))


def test_cli_extract_exits_nonzero_on_replacement_char(tmp_path, monkeypatch, capsys):
    """'eencijferho extract' stopt met exitcode 1 en noemt het bestand."""
    inp = tmp_path / "in"
    inp.mkdir()
    (inp / "Bestandsbeschrijving_Dec-bestanden.txt").write_bytes(
        _dec_description("v\ufffd\ufffdr").encode("utf-8")
    )
    monkeypatch.chdir(tmp_path)  # CLI accepteert alleen paden binnen de werkmap
    monkeypatch.setattr(sys, "argv", ["eencijferho", "extract", "--input", "in", "--output", "out"])
    with pytest.raises(SystemExit) as exc_info:
        cli_main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Bestandsbeschrijving_Dec-bestanden.txt" in out
    assert "U+FFFD" in out


def test_demo_descriptions_keep_vooropleiding_name(tmp_path):
    """Regressie: de naam blijft gelijk uit de latin-1 1cyferho-DEMO en de UTF-8 Dec-bestanden-DEMO."""
    inp = tmp_path / "in"
    inp.mkdir()
    latin1_name = "Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt"
    utf8_name = "Bestandsbeschrijving_Dec-bestanden.txt"
    for name in (latin1_name, utf8_name):
        (inp / name).write_bytes((DEMO_DIR / name).read_bytes())
    json_dir = tmp_path / "json"
    result = process_txt_folder(str(inp), json_output_folder=str(json_dir))
    assert len(result) == 2

    latin1_json = (json_dir / latin1_name.replace(".txt", ".json")).read_text(encoding="utf-8")
    assert VOOROPLEIDING in latin1_json
    dec_json = json_dir / utf8_name.replace(".txt", ".json")
    assert VOOROPLEIDING in _decoding_variables(str(dec_json))
    for path in json_dir.glob("*.json"):
        text = path.read_text(encoding="utf-8")
        assert "\ufffd" not in text
        assert "ï¿½" not in text

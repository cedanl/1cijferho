# Tests for eencijferho.core.converter: process_chunk, converter, and private helpers

import json
import pytest
from pathlib import Path

from eencijferho.core.converter import (
    process_chunk,
    converter,
    _resolve_output_path,
    _load_metadata,
    _read_lines,
    _write_header,
    _run_serial,
    _load_match_log,
    _convert_one,
)


@pytest.mark.parametrize(
    "positions,chunk,expected",
    [
        ([(0, 5), (5, 10)], ["helloworld"], ["hello;world"]),
        ([(0, 10), (10, 15)], ["Jan         001  "], ["Jan;001"]),
        ([(0, 5), (5, 10)], [b"abc  def  "], ["abc;def"]),
        ([(0, 5)], ["hello"], ["hello"]),
    ],
)
def test_process_chunk_slicing(positions, chunk, expected):
    assert process_chunk((positions, chunk)) == expected


def test_process_chunk_skips_empty_lines():
    positions = [(0, 5), (5, 10)]
    result = process_chunk((positions, ["helloworld", "     ", "", "foo  bar  "]))
    assert result == ["hello;world", "foo;bar"]


def test_process_chunk_semicolon_count():
    result = process_chunk(([(0, 3), (3, 6), (6, 9)], ["abcdefghi"]))
    assert result[0].count(";") == 2


def test_process_chunk_empty_chunk():
    assert process_chunk(([(0, 5), (5, 10)], [])) == []


def test_process_chunk_all_whitespace_lines():
    assert process_chunk(([(0, 5), (5, 10)], ["", "   ", "\n"])) == []


def test_converter_creates_output_file(fixed_width_file, metadata_xlsx, tmp_path):
    out_path, _ = converter(str(fixed_width_file), str(metadata_xlsx), str(tmp_path))
    assert out_path is not None
    assert Path(out_path).exists()


def test_converter_header_is_first_line(fixed_width_file, metadata_xlsx, tmp_path):
    out_path, _ = converter(str(fixed_width_file), str(metadata_xlsx), str(tmp_path))
    lines = Path(out_path).read_text(encoding="utf-8").splitlines()
    assert lines[0] == "Naam;Waarde"


def test_converter_data_row_count(fixed_width_file, metadata_xlsx, tmp_path):
    out_path, _ = converter(str(fixed_width_file), str(metadata_xlsx), str(tmp_path))
    all_lines = Path(out_path).read_text(encoding="utf-8").splitlines()
    data_lines = [line for line in all_lines if line.strip()][1:]  # skip header
    assert len(data_lines) == 3


def test_converter_output_is_utf8(fixed_width_file, metadata_xlsx, tmp_path):
    out_path, _ = converter(str(fixed_width_file), str(metadata_xlsx), str(tmp_path))
    assert len(Path(out_path).read_text(encoding="utf-8")) > 0


def test_converter_semicolon_delimiter(fixed_width_file, metadata_xlsx, tmp_path):
    out_path, _ = converter(str(fixed_width_file), str(metadata_xlsx), str(tmp_path))
    lines = [
        line
        for line in Path(out_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert all(";" in line for line in lines)


def test_converter_output_filename(fixed_width_file, metadata_xlsx, tmp_path):
    out_path, _ = converter(str(fixed_width_file), str(metadata_xlsx), str(tmp_path))
    assert Path(out_path).stem == fixed_width_file.stem
    assert Path(out_path).suffix == ".csv"


def test_converter_total_lines_returned(fixed_width_file, metadata_xlsx, tmp_path):
    _, total_lines = converter(str(fixed_width_file), str(metadata_xlsx), str(tmp_path))
    assert total_lines == 3


def test_converter_creates_missing_output_dir(
    fixed_width_file, metadata_xlsx, tmp_path
):
    nested = tmp_path / "a" / "b" / "c"
    out_path, _ = converter(str(fixed_width_file), str(metadata_xlsx), str(nested))
    assert Path(out_path).exists()


# ---------------------------------------------------------------------------
# _resolve_output_path
# ---------------------------------------------------------------------------


def test_resolve_output_path_csv_extension():
    result = _resolve_output_path("/input/data/MyFile.asc", "/output")
    assert result == "/output/MyFile.csv"


def test_resolve_output_path_strips_directory():
    result = _resolve_output_path("/deep/path/to/Dec_landcode.asc", "/out")
    assert Path(result).name == "Dec_landcode.csv"


# ---------------------------------------------------------------------------
# _load_metadata
# ---------------------------------------------------------------------------


def test_load_metadata_returns_columns_and_positions(metadata_xlsx):
    columns, positions = _load_metadata(str(metadata_xlsx))
    assert columns == ["Naam", "Waarde"]
    assert positions == [(0, 10), (10, 15)]


def test_load_metadata_positions_are_cumulative(metadata_xlsx):
    _, positions = _load_metadata(str(metadata_xlsx))
    # Second field starts where first ends
    assert positions[1][0] == positions[0][1]


# ---------------------------------------------------------------------------
# _read_lines
# ---------------------------------------------------------------------------


def test_read_lines_returns_all_lines(fixed_width_file):
    lines = _read_lines(str(fixed_width_file))
    assert len(lines) == 3


def test_read_lines_empty_file(tmp_path):
    f = tmp_path / "empty.asc"
    f.write_bytes(b"")
    assert _read_lines(str(f)) == []


def test_read_lines_latin1_encoding(tmp_path):
    f = tmp_path / "latin1.asc"
    f.write_bytes("café".encode("latin-1"))
    lines = _read_lines(str(f))
    assert "caf" in lines[0]


# ---------------------------------------------------------------------------
# _write_header
# ---------------------------------------------------------------------------


def test_write_header_creates_file(tmp_path):
    out = tmp_path / "out.csv"
    _write_header(str(out), ["A", "B", "C"])
    assert out.exists()
    assert out.read_text(encoding="utf-8").strip() == "A;B;C"


def test_write_header_overwrites_existing(tmp_path):
    out = tmp_path / "out.csv"
    out.write_text("old content", encoding="utf-8")
    _write_header(str(out), ["X"])
    assert out.read_text(encoding="utf-8").strip() == "X"


# ---------------------------------------------------------------------------
# _run_serial
# ---------------------------------------------------------------------------


def test_run_serial_appends_to_file(tmp_path):
    out = tmp_path / "out.csv"
    _write_header(str(out), ["Naam", "Waarde"])
    lines = ["Jan       001  ", "Piet      042  "]
    _run_serial(lines, [(0, 10), (10, 15)], str(out))
    rows = [l for l in out.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert rows[0] == "Naam;Waarde"
    assert rows[1] == "Jan;001"
    assert rows[2] == "Piet;042"


# ---------------------------------------------------------------------------
# _load_match_log
# ---------------------------------------------------------------------------


def test_load_match_log_returns_processed_files(tmp_path):
    log = tmp_path / "match.json"
    data = {"processed_files": [{"input_file": "a.asc", "status": "matched", "matches": []}]}
    log.write_text(json.dumps(data), encoding="utf-8")
    result = _load_match_log(str(log))
    assert result is not None
    assert result[0]["input_file"] == "a.asc"


def test_load_match_log_missing_file_returns_none(tmp_path):
    assert _load_match_log(str(tmp_path / "geen.json")) is None


def test_load_match_log_invalid_json_returns_none(tmp_path):
    log = tmp_path / "bad.json"
    log.write_text("geen json {{{", encoding="utf-8")
    assert _load_match_log(str(log)) is None


# ---------------------------------------------------------------------------
# _convert_one
# ---------------------------------------------------------------------------


def test_convert_one_skips_unmatched_file():
    file_info = {"input_file": "test.asc", "status": "unmatched", "matches": []}
    result = _convert_one(file_info, "/in", "/meta", "/out")
    assert result["status"] == "skipped"


def test_convert_one_skips_no_valid_matches():
    file_info = {
        "input_file": "test.asc",
        "status": "matched",
        "matches": [{"validation_status": "failed", "validation_file": "meta.xlsx"}],
    }
    result = _convert_one(file_info, "/in", "/meta", "/out")
    assert result["status"] == "skipped"


def test_convert_one_fails_missing_input(tmp_path, metadata_xlsx):
    file_info = {
        "input_file": "bestaat_niet.asc",
        "status": "matched",
        "matches": [{"validation_status": "success", "validation_file": metadata_xlsx.name}],
    }
    result = _convert_one(file_info, str(tmp_path), str(metadata_xlsx.parent), str(tmp_path))
    assert result["status"] == "failed"


def test_convert_one_succeeds(fixed_width_file, metadata_xlsx, tmp_path):
    file_info = {
        "input_file": fixed_width_file.name,
        "status": "matched",
        "matches": [{"validation_status": "success", "validation_file": metadata_xlsx.name}],
    }
    result = _convert_one(
        file_info,
        str(fixed_width_file.parent),
        str(metadata_xlsx.parent),
        str(tmp_path),
    )
    assert result["status"] == "success"
    assert Path(result["output_file"]).exists()

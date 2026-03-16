# Tests for eencijferho.core.converter: process_chunk and converter()

import pytest
from pathlib import Path

from eencijferho.core.converter import process_chunk, converter


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

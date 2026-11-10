# Tests for eencijferho.core.converter: process_chunk, converter, and private helpers

import json
import pytest
import polars as pl
from pathlib import Path

from eencijferho.core.converter import (
    process_chunk,
    converter,
    _resolve_output_path,
    _load_metadata,
    _read_lines,
    _run_serial,
    _load_match_log,
    _convert_one,
    _unsupported_reason,
    run_conversions_from_matches,
)
from eencijferho.core.dec_layouts import INFERRED_LAYOUT_NOTE, SUPPLEMENTARY_LAYOUT_FILENAME


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
    assert Path(result) == Path("/output/MyFile.csv")


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
# _run_serial
# ---------------------------------------------------------------------------


def test_run_serial_returns_csv_lines():
    lines = ["Jan       001  ", "Piet      042  "]
    result = _run_serial(lines, [(0, 10), (10, 15)])
    assert result == ["Jan;001", "Piet;042"]


def test_run_serial_empty_input():
    assert _run_serial([], [(0, 5)]) == []


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


def test_convert_one_fails_no_valid_matches():
    file_info = {
        "input_file": "test.asc",
        "status": "matched",
        "matches": [{"validation_status": "failed", "validation_file": "meta.xlsx"}],
    }
    result = _convert_one(file_info, "/in", "/meta", "/out")
    assert result["status"] == "failed"
    assert result["candidates"] == ["meta.xlsx"]


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


# ---------------------------------------------------------------------------
# _load_metadata — Startpositie is the authority, a gap is an error
# ---------------------------------------------------------------------------


def _write_layout(path, names, widths, starts=None):
    """Write a Lay-out Excel. Without `starts` the column is left out entirely."""
    data = {"ID": list(range(1, len(names) + 1)), "Naam": names, "Aantal posities": widths}
    if starts is not None:
        data["Startpositie"] = starts
    pl.DataFrame(data).write_excel(path)
    return path


def test_load_metadata_uses_startposities(tmp_path):
    """Positions follow Startpositie, not the running sum of widths."""
    path = _write_layout(
        tmp_path / "layout.xlsx",
        ["A", "B", "C"],
        [3, 2, 4],
        starts=[1, 4, 6],
    )
    _, positions = _load_metadata(str(path))
    assert positions == [(0, 3), (3, 5), (5, 9)]


def test_load_metadata_rejects_a_gap_between_fields(tmp_path):
    """A gap would silently shift every following field, so it has to fail.

    Without this the converter would read 'B' from the wrong three characters
    and report status: success.
    """
    path = _write_layout(
        tmp_path / "layout.xlsx",
        ["A", "B", "C"],
        [3, 2, 4],
        starts=[1, 4, 9],  # C should start at 6
    )
    with pytest.raises(ValueError, match="C"):
        _load_metadata(str(path))


def test_load_metadata_gap_error_names_both_positions(tmp_path):
    """The message has to say which field and what was expected."""
    path = _write_layout(tmp_path / "layout.xlsx", ["A", "B"], [3, 2], starts=[1, 9])
    with pytest.raises(ValueError) as err:
        _load_metadata(str(path))
    text = str(err.value)
    assert "B" in text
    assert "4" in text          # where the sum says it starts
    assert "9" in text          # where the layout says it starts


def test_load_metadata_rejects_a_leading_gap(tmp_path):
    """The first field starting after position 1 is a gap too."""
    path = _write_layout(tmp_path / "layout.xlsx", ["A", "B"], [3, 2], starts=[4, 7])
    with pytest.raises(ValueError, match="A"):
        _load_metadata(str(path))


def test_load_metadata_without_startpositie_falls_back_to_widths(tmp_path, capsys):
    """A future DUO version without the column still converts, but says so."""
    path = _write_layout(tmp_path / "layout.xlsx", ["A", "B"], [3, 2])
    columns, positions = _load_metadata(str(path))
    assert columns == ["A", "B"]
    assert positions == [(0, 3), (3, 5)]
    assert "Startpositie" in capsys.readouterr().out


def test_load_metadata_all_zero_width_field_is_not_a_gap(tmp_path):
    """A field with width 0 keeps its own start, so it must not trip the check."""
    path = _write_layout(tmp_path / "layout.xlsx", ["A", "B", "C"], [3, 0, 2], starts=[1, 4, 4])
    _, positions = _load_metadata(str(path))
    assert positions == [(0, 3), (3, 3), (3, 5)]


# ---------------------------------------------------------------------------
# run_conversions_from_matches: files without a layout are reported (#204)
# ---------------------------------------------------------------------------


def _match_log(tmp_path, processed_files):
    logs = tmp_path / "meta" / "logs"
    logs.mkdir(parents=True)
    path = logs / "(4)_file_matching_log_latest.json"
    path.write_text(json.dumps({"processed_files": processed_files}), encoding="utf-8")
    return path


def _unmatched(name):
    return {"input_file": name, "status": "unmatched", "matches": []}


def test_unsupported_reason_says_niet_ondersteund():
    reason = _unsupported_reason("Croho.asc", [])
    assert "niet ondersteund" in reason
    assert "geen bestandsbeschrijving" in reason


def test_unsupported_reason_names_the_described_years():
    layouts = [
        "Bestandsbeschrijving_Dec-bestanden_12_Dec_postcodecijfers_2024.asc.xlsx",
        "Bestandsbeschrijving_Dec-bestanden_13_Dec_postcodecijfers_2025.asc.xlsx",
        "Bestandsbeschrijving_Dec-bestanden_1_Dec_landcode.asc.xlsx",
    ]
    reason = _unsupported_reason("Dec_postcodecijfers_2021.asc", layouts)
    assert "niet ondersteund" in reason
    assert "2024, 2025" in reason
    assert "Dec_postcodecijfers" in reason


def test_run_conversions_reports_unmatched_files_as_unsupported(tmp_path, capsys):
    meta = tmp_path / "meta"
    log = _match_log(tmp_path, [
        _unmatched("Croho.asc"),
        _unmatched("Dec_postcodecijfers_2021.asc"),
    ])
    (meta / "Bestandsbeschrijving_Dec-bestanden_12_Dec_postcodecijfers_2024.asc.xlsx").write_bytes(b"")

    results = run_conversions_from_matches(
        str(tmp_path / "in"), metadata_folder=str(meta),
        match_log_file=str(log), output_folder=str(tmp_path / "out"),
    )

    unsupported = {u["input_file"]: u["reason"] for u in results["unsupported_files"]}
    assert set(unsupported) == {"Croho.asc", "Dec_postcodecijfers_2021.asc"}
    assert all("niet ondersteund" in r for r in unsupported.values())
    assert "2024" in unsupported["Dec_postcodecijfers_2021.asc"]
    # Not counted as converted, failed or skipped.
    assert results["successful_conversions"] == 0
    assert results["failed_conversions"] == 0
    assert results["skipped_files"] == 0
    assert results["unsupported_count"] == 2

    written = json.loads((log.parent / "(5)_conversion_log_latest.json").read_text(encoding="utf-8"))
    statuses = {d["input_file"]: d["status"] for d in written["details"]}
    assert statuses == {"Croho.asc": "unsupported", "Dec_postcodecijfers_2021.asc": "unsupported"}
    assert written["unsupported_files"] == results["unsupported_files"]
    out = capsys.readouterr().out
    assert "Niet ondersteund" in out
    assert "Croho.asc" in out


def test_run_conversions_marks_an_inferred_layout(tmp_path, fixed_width_file):
    meta = tmp_path / "meta"
    log = _match_log(tmp_path, [])
    layout = meta / f"{SUPPLEMENTARY_LAYOUT_FILENAME}_1_{fixed_width_file.name}.xlsx"
    pl.DataFrame({
        "ID": [1, 2], "Naam": ["Naam", "Waarde"],
        "Startpositie": [1, 11], "Aantal posities": [10, 5], "Opmerking": ["", ""],
    }).write_excel(layout)
    log.write_text(json.dumps({"processed_files": [{
        "input_file": fixed_width_file.name, "status": "matched",
        "matches": [{"validation_file": layout.name, "validation_status": "success"}],
    }]}), encoding="utf-8")

    results = run_conversions_from_matches(
        str(fixed_width_file.parent), metadata_folder=str(meta),
        match_log_file=str(log), output_folder=str(tmp_path / "out"),
    )
    (detail,) = results["details"]
    assert detail["status"] == "success"
    assert detail["layout_note"] == INFERRED_LAYOUT_NOTE

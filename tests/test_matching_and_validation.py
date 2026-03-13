# Tests for eencijferho.utils.converter_match and converter_validation

import json
import pytest
import polars as pl
from pathlib import Path

from eencijferho.utils.converter_match import load_input_files, match_files
from eencijferho.utils.converter_validation import converter_validation


def _make_validation_log(tmp_path: Path, entries: list[dict]) -> Path:
    data = {"timestamp": "20240101_000000", "processed_files": entries}
    p = tmp_path / "logs" / "(3)_xlsx_validation_log_latest.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2), encoding="latin-1")
    return p


def _make_asc(folder: Path, name: str, rows: int = 3) -> Path:
    p = folder / name
    p.write_bytes(b"\n".join([b"data"] * rows) + b"\n")
    return p


def _make_conversion_log(tmp_path: Path, details: list[dict]) -> Path:
    data = {"timestamp": "20240101_000001", "details": details}
    p = tmp_path / "conversion_log.json"
    p.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return p


def _make_asc(folder: Path, name: str, rows: int = 3) -> Path:
    p = folder / name
    p.write_bytes(b"\n".join([b"data"] * rows) + b"\n")
    return p


# --- load_input_files ---


def test_load_input_files_columns(tmp_path):
    _make_asc(tmp_path, "EV2023.asc")
    df = load_input_files(str(tmp_path))
    assert "input_file" in df.columns
    assert "row_count" in df.columns


def test_load_input_files_counts_lines(tmp_path):
    _make_asc(tmp_path, "EV2023.asc", rows=5)
    df = load_input_files(str(tmp_path))
    assert df.filter(pl.col("input_file") == "EV2023.asc")["row_count"][0] == 5


@pytest.mark.parametrize("filename", ["meta.txt", "meta.xlsx", "archive.zip"])
def test_load_input_files_excludes_extension(tmp_path, filename):
    _make_asc(tmp_path, "data.asc")
    (tmp_path / filename).write_bytes(b"x")
    df = load_input_files(str(tmp_path))
    assert filename not in df["input_file"].to_list()


def test_load_input_files_empty_folder(tmp_path):
    assert len(load_input_files(str(tmp_path))) == 0


def test_load_input_files_missing_folder(tmp_path):
    assert len(load_input_files(str(tmp_path / "nonexistent"))) == 0


def test_load_input_files_no_recursion(tmp_path):
    sub = tmp_path / "sub"
    sub.mkdir()
    _make_asc(sub, "hidden.asc")
    _make_asc(tmp_path, "visible.asc")
    names = load_input_files(str(tmp_path))["input_file"].to_list()
    assert "visible.asc" in names
    assert "hidden.asc" not in names


# --- match_files ---


@pytest.mark.parametrize(
    "input_name,validation_name",
    [
        ("EV2023.asc", "Bestandsbeschrijving_1cyferho_2023.xlsx"),
        ("VAKHAVW2023.asc", "Bestandsbeschrijving_Vakgegevens_2023.xlsx"),
        ("CUSTOM2023.asc", "CUSTOM2023.asc"),  # default substring match
    ],
)
def test_match_files_matched(
    tmp_path, input_name, validation_name, make_validation_log
):
    _make_asc(tmp_path, input_name)
    log = make_validation_log([{"file": validation_name, "status": "success"}])
    result = match_files(str(tmp_path), str(log))
    row = result["input_matches"].filter(pl.col("input_file") == input_name)
    assert row["matched"][0] is True


def test_match_files_unmatched_flagged(tmp_path, make_validation_log):
    _make_asc(tmp_path, "UNKNOWN_XYZ.asc")
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_1cyferho.xlsx", "status": "success"}]
    )
    result = match_files(str(tmp_path), str(log))
    row = result["input_matches"].filter(pl.col("input_file") == "UNKNOWN_XYZ.asc")
    assert row["matched"][0] is False


def test_match_files_returns_expected_keys(tmp_path, make_validation_log):
    _make_asc(tmp_path, "EV2023.asc")
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_1cyferho.xlsx", "status": "success"}]
    )
    result = match_files(str(tmp_path), str(log))
    assert "input_matches" in result
    assert "unmatched_validation" in result


def test_match_files_row_count_propagated(tmp_path, make_validation_log):
    _make_asc(tmp_path, "EV2023.asc", rows=7)
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_1cyferho.xlsx", "status": "success"}]
    )
    result = match_files(str(tmp_path), str(log))
    row = result["input_matches"].filter(pl.col("input_file") == "EV2023.asc")
    assert row["row_count"][0] == 7


def test_match_files_writes_log(tmp_path, make_validation_log):
    _make_asc(tmp_path, "EV2023.asc")
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_1cyferho.xlsx", "status": "success"}]
    )
    match_files(str(tmp_path), str(log))
    assert (tmp_path / "logs" / "(4)_file_matching_log_latest.json").exists()


# --- converter_validation ---


def test_converter_validation_passes_on_match(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log(
        [{"input_file": "EV2023.asc", "status": "success", "total_lines": 100}]
    )
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 100,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["successful_conversions"] == 1
    assert result["failed_conversions"] == 0


def test_converter_validation_fails_on_mismatch(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log(
        [{"input_file": "EV2023.asc", "status": "success", "total_lines": 99}]
    )
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 100,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["failed_conversions"] == 1
    assert result["successful_conversions"] == 0


def test_converter_validation_error_message_contains_counts(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log(
        [{"input_file": "EV2023.asc", "status": "success", "total_lines": 50}]
    )
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 100,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    error = result["file_details"][0]["error"]
    assert "50" in error or "100" in error


def test_converter_validation_skips_missing_from_conversion_log(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log([])
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 5,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["total_files"] == 0


def test_converter_validation_writes_output_log(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log([])
    match = make_match_log([])
    out = tmp_path / "sub" / "validation.json"
    converter_validation(str(conv), str(match), str(out))
    assert out.exists()


def test_converter_validation_status_completed(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log([])
    match = make_match_log([])
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["status"] == "completed"

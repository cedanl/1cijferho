"""The converter uses exactly one layout per input file (#198).

With layouts for two years in one folder, each EV file must be read with the
layout of its own year; a match log that holds no single valid layout must not
lead to a conversion with a layout picked at random. Synthetic data only.
"""

import json
from pathlib import Path

import polars as pl
import pytest

from eencijferho.core.converter import (
    MatchLogNotCompletedError,
    _convert_one,
    run_conversions_from_matches,
)
from eencijferho.core.pipeline import run_turbo_convert_pipeline
from eencijferho.utils.converter_match import LayoutMatchError, match_files

LAYOUT_2019 = "Bestandsbeschrijving_1cyferho_2019_1_Lay-out bestand.xlsx"
LAYOUT_2024 = "Bestandsbeschrijving_1cyferho_2024_1_Lay-out bestand.xlsx"


def _write_layout(path: Path, names: list[str], widths: list[int]) -> Path:
    starts = [1 + sum(widths[:i]) for i in range(len(widths))]
    pl.DataFrame(
        {
            "ID": list(range(1, len(names) + 1)),
            "Naam": names,
            "Startpositie": starts,
            "Aantal posities": widths,
            "Opmerking": [""] * len(names),
        }
    ).write_excel(path)
    return path


def _write_validation_log(logs: Path, files: list[str]) -> Path:
    logs.mkdir(parents=True, exist_ok=True)
    path = logs / "(3)_xlsx_validation_log_latest.json"
    data = {"processed_files": [{"file": f, "status": "success"} for f in files]}
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


@pytest.fixture
def two_years(tmp_path):
    """Two EV files and two layouts; field 2 is 4 wide in 2019 and 6 wide in 2024."""
    inp = tmp_path / "in"
    meta = tmp_path / "meta"
    out = tmp_path / "out"
    for folder in (inp, meta, out):
        folder.mkdir()
    (inp / "EV299XX19_X.asc").write_bytes(b"001ABCD\n002EFGH\n")
    (inp / "EV299XX24_X.asc").write_bytes(b"003ABCDEF\n004GHIJKL\n")
    _write_layout(meta / LAYOUT_2019, ["Code", "Waarde"], [3, 4])
    _write_layout(meta / LAYOUT_2024, ["Code", "Waarde"], [3, 6])
    log = _write_validation_log(meta / "logs", [LAYOUT_2019, LAYOUT_2024])
    return inp, meta, out, log


def test_each_year_is_read_with_its_own_layout(two_years):
    inp, meta, out, log = two_years

    match_files(str(inp), str(log))
    summary = run_conversions_from_matches(
        str(inp),
        metadata_folder=str(meta),
        match_log_file=str(meta / "logs" / "(4)_file_matching_log_latest.json"),
        output_folder=str(out),
    )

    assert summary["successful_conversions"] == 2
    assert (out / "EV299XX19_X.csv").read_text(encoding="utf-8").splitlines() == [
        "Code;Waarde",
        "001;ABCD",
        "002;EFGH",
    ]
    assert (out / "EV299XX24_X.csv").read_text(encoding="utf-8").splitlines() == [
        "Code;Waarde",
        "003;ABCDEF",
        "004;GHIJKL",
    ]


def test_layout_error_leaves_a_log_the_converter_refuses(two_years):
    inp, meta, out, log = two_years
    (inp / "EV299XX22_X.asc").write_bytes(b"005ABCD\n")

    with pytest.raises(LayoutMatchError):
        match_files(str(inp), str(log))
    summary = run_conversions_from_matches(
        str(inp),
        metadata_folder=str(meta),
        match_log_file=str(meta / "logs" / "(4)_file_matching_log_latest.json"),
        output_folder=str(out),
    )

    assert summary["status"] == "failed"
    assert summary["match_log_status"] == "failed"
    assert list(out.iterdir()) == []


@pytest.mark.parametrize("status", [None, "started", "failed"])
def test_run_conversions_refuses_log_that_is_not_completed(tmp_path, status):
    log = tmp_path / "match.json"
    data = {
        "processed_files": [
            {
                "input_file": "EV2023.asc",
                "status": "matched",
                "matches": [{"validation_file": "x.xlsx", "validation_status": "success"}],
            }
        ]
    }
    if status is not None:
        data["status"] = status
    log.write_text(json.dumps(data), encoding="utf-8")

    summary = run_conversions_from_matches(
        str(tmp_path), metadata_folder=str(tmp_path), match_log_file=str(log),
        output_folder=str(tmp_path / "out"),
    )

    assert summary["status"] == "failed"
    assert "completed" in summary["reason"]
    assert not (tmp_path / "out").exists()


def test_convert_one_fails_on_two_different_layouts(two_years):
    inp, meta, out, _ = two_years
    file_info = {
        "input_file": "EV299XX24_X.asc",
        "status": "matched",
        "matches": [
            {"validation_file": LAYOUT_2019, "validation_status": "success"},
            {"validation_file": LAYOUT_2024, "validation_status": "success"},
        ],
    }

    result = _convert_one(file_info, str(inp), str(meta), str(out))

    assert result["status"] == "failed"
    assert result["candidates"] == [LAYOUT_2019, LAYOUT_2024]
    assert LAYOUT_2019 in result["reason"] and LAYOUT_2024 in result["reason"]
    assert list(out.iterdir()) == []


def test_convert_one_accepts_two_identical_layouts(two_years):
    """Dec_vakcode.asc is described twice by DUO, with the same layout."""
    inp, meta, out, _ = two_years
    copy = "Bestandsbeschrijving_Vakkenbestanden_1_Lay-out 2024.xlsx"
    _write_layout(meta / copy, ["Code", "Waarde"], [3, 6])
    file_info = {
        "input_file": "EV299XX24_X.asc",
        "status": "matched",
        "matches": [
            {"validation_file": LAYOUT_2024, "validation_status": "success"},
            {"validation_file": copy, "validation_status": "success"},
        ],
    }

    result = _convert_one(file_info, str(inp), str(meta), str(out))

    assert result["status"] == "success"
    assert (out / "EV299XX24_X.csv").read_text(encoding="utf-8").splitlines()[1] == "003;ABCDEF"


def test_pipeline_stops_on_a_failed_match_log(two_years):
    inp, meta, out, log = two_years
    (inp / "EV299XX22_X.asc").write_bytes(b"005ABCD\n")
    with pytest.raises(LayoutMatchError):
        match_files(str(inp), str(log))

    with pytest.raises(MatchLogNotCompletedError, match="failed"):
        run_turbo_convert_pipeline(
            input_dir=str(inp), output_dir=str(out), metadata_dir=str(meta),
        )
    assert list(out.iterdir()) == []

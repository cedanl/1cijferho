
import json

import pytest
import polars as pl
from pathlib import Path

from eencijferho.utils.converter_match import (
    LayoutMatchError,
    _year_from_input_name,
    _year_from_layout_name,
    load_input_files,
    match_files,
    select_layout,
)


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


# --- layout choice by year (#198) ---

LAYOUT_2019 = "Bestandsbeschrijving_1cyferho_2019_v1.0_1_Lay-out bestand.xlsx"
LAYOUT_2023 = "Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO_1_Lay-out bestand.xlsx"
LAYOUT_2024 = "Bestandsbeschrijving_1cyferho_2024_v1.1_DEMO_1_Lay-out bestand.xlsx"
VAK_NO_YEAR = "Bestandsbeschrijving_Vakkenbestanden_2_Vakgegevens.xlsx"


def _ok(*names):
    return [{"file": name, "status": "success"} for name in names]


def _read_match_log(folder: Path) -> dict:
    return json.loads(
        (folder / "logs" / "(4)_file_matching_log_latest.json").read_text(encoding="utf-8")
    )


@pytest.mark.parametrize(
    "name,expected",
    [
        ("EV299XX24_DEMO.asc", 2024),
        ("EV299XX19_X.asc", 2019),
        ("EV2023.asc", 2023),
        ("VAKHAVW2023.asc", 2023),
        ("VAKHAVW_99XX_DEMO.asc", None),
        ("EV12345678.asc", None),
    ],
)
def test_year_from_input_name(name, expected):
    assert _year_from_input_name(name) == expected


@pytest.mark.parametrize(
    "name,keyword,expected",
    [
        (LAYOUT_2024, "1cyferho", 2024),
        ("Bestandsbeschrijving_1cyferho.xlsx", "1cyferho", None),
        ("Bestandsbeschrijving_Vakgegevens_2023.xlsx", "Vakgegevens", 2023),
        (VAK_NO_YEAR, "Vakgegevens", None),
    ],
)
def test_year_from_layout_name(name, keyword, expected):
    assert _year_from_layout_name(name, keyword) == expected


def test_select_layout_picks_year_from_input_name():
    choice = select_layout(
        "EV299XX24_DEMO.asc",
        [(LAYOUT_2023, "success"), (LAYOUT_2024, "success")],
        "1cyferho",
    )
    assert choice.chosen == LAYOUT_2024
    assert choice.kind == "ok"


def test_select_layout_rejects_chosen_layout_that_failed_validation():
    choice = select_layout(
        "EV299XX24_DEMO.asc",
        [(LAYOUT_2023, "success"), (LAYOUT_2024, "failed")],
        "1cyferho",
    )
    assert choice.chosen is None
    assert choice.kind == "not_success"
    assert LAYOUT_2024 in choice.reason


def test_match_files_picks_layout_of_input_year(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "EV299XX24_DEMO.asc")
    log = make_validation_log(_ok(LAYOUT_2023, LAYOUT_2024))

    result = match_files(str(inp), str(log))

    rows = result["input_matches"].filter(pl.col("input_file") == "EV299XX24_DEMO.asc")
    assert rows["validation_file"].to_list() == [LAYOUT_2024]
    data = _read_match_log(tmp_path)
    assert data["status"] == "completed"
    (entry,) = data["processed_files"]
    assert [m["validation_file"] for m in entry["matches"]] == [LAYOUT_2024]
    assert data["matched_validation_files"] == 1
    assert [u["validation_file"] for u in data["unmatched_validation"]] == [LAYOUT_2023]


def test_match_files_two_years_two_files(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "EV299XX19_X.asc")
    _make_asc(inp, "EV299XX24_X.asc")
    log = make_validation_log(_ok(LAYOUT_2019, LAYOUT_2024))

    result = match_files(str(inp), str(log))

    pairs = dict(result["input_matches"].select("input_file", "validation_file").rows())
    assert pairs == {"EV299XX19_X.asc": LAYOUT_2019, "EV299XX24_X.asc": LAYOUT_2024}


@pytest.mark.parametrize(
    "input_name,layouts,fragment",
    [
        # no layout at all for an EV file
        ("EV299XX24_X.asc", [], "Geen bestandsbeschrijving"),
        # two layouts for the same year
        (
            "EV299XX24_X.asc",
            [LAYOUT_2024, "Bestandsbeschrijving_1cyferho_2024_v2.0_1_Lay-out bestand.xlsx"],
            "voor jaar 2024",
        ),
        # no year in the input name and more than one layout
        ("EV_ZONDER_JAAR.asc", [LAYOUT_2023, LAYOUT_2024], "Geen jaar in de bestandsnaam"),
        # two Vakgegevens layouts without a year
        (
            "VAKHAVW_99XX_DEMO.asc",
            [VAK_NO_YEAR, "Bestandsbeschrijving_Vakkenbestanden_3_Vakgegevens.xlsx"],
            "Geen jaar in de bestandsnaam",
        ),
    ],
)
def test_match_files_ambiguous_or_missing_layout_raises(
    tmp_path, make_validation_log, input_name, layouts, fragment
):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, input_name)
    _make_asc(inp, "CUSTOM2023.asc")
    log = make_validation_log(_ok(*layouts, "CUSTOM2023.asc"))

    with pytest.raises(LayoutMatchError) as exc:
        match_files(str(inp), str(log))

    assert fragment in str(exc.value)
    for layout in layouts:
        assert layout in str(exc.value)
    data = _read_match_log(tmp_path)
    assert data["status"] == "failed"
    entries = {f["input_file"]: f for f in data["processed_files"]}
    assert entries[input_name]["status"] == "failed"
    assert entries[input_name]["candidates"] == layouts
    assert fragment in entries[input_name]["error"]
    assert entries[input_name]["matches"] == []
    # other file types keep the substring match
    assert entries["CUSTOM2023.asc"]["status"] == "matched"


def test_match_files_year_mismatch_names_both_years(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "EV299XX24_X.asc")
    log = make_validation_log(_ok(LAYOUT_2019, LAYOUT_2023))

    with pytest.raises(LayoutMatchError) as exc:
        match_files(str(inp), str(log))

    message = str(exc.value)
    assert "2024" in message and "2019, 2023" in message
    assert "--layout-year" in message
    assert exc.value.errors[0]["input_file"] == "EV299XX24_X.asc"
    assert _read_match_log(tmp_path)["status"] == "failed"


def test_match_files_single_layout_of_other_year_is_a_mismatch(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "EV299XX24_X.asc")
    log = make_validation_log(_ok(LAYOUT_2023))

    with pytest.raises(LayoutMatchError, match="komt niet overeen"):
        match_files(str(inp), str(log))


def test_match_files_layout_year_forces_layout(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "EV299XX24_X.asc")
    _make_asc(inp, "VAKHAVW_99XX_DEMO.asc")
    log = make_validation_log(_ok(LAYOUT_2019, LAYOUT_2023, VAK_NO_YEAR))

    result = match_files(str(inp), str(log), layout_year=2023)

    pairs = dict(result["input_matches"].select("input_file", "validation_file").rows())
    # EV is forced onto 2023; the single Vakgegevens layout without a year still applies
    assert pairs == {"EV299XX24_X.asc": LAYOUT_2023, "VAKHAVW_99XX_DEMO.asc": VAK_NO_YEAR}
    data = _read_match_log(tmp_path)
    assert data["status"] == "completed"
    assert data["layout_year"] == 2023
    entries = {f["input_file"]: f for f in data["processed_files"]}
    assert entries["EV299XX24_X.asc"]["forced_layout_year"] == 2023
    assert "forced_layout_year" not in entries["VAKHAVW_99XX_DEMO.asc"]
    assert len(data["warnings"]) == 1
    assert "geforceerd" in data["warnings"][0]


def test_match_files_layout_year_without_layout_raises(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "EV299XX24_X.asc")
    log = make_validation_log(_ok(LAYOUT_2019, LAYOUT_2024))

    with pytest.raises(LayoutMatchError, match="--layout-year 2022"):
        match_files(str(inp), str(log), layout_year=2022)


def test_match_files_vakhavw_picks_year(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "VAKHAVW_99XX24.asc")
    vak_2023 = "Bestandsbeschrijving_Vakgegevens_2023.xlsx"
    vak_2024 = "Bestandsbeschrijving_Vakgegevens_2024.xlsx"
    log = make_validation_log(_ok(vak_2023, vak_2024))

    result = match_files(str(inp), str(log))

    assert result["input_matches"]["validation_file"].to_list() == [vak_2024]


def test_match_files_failed_layout_validation_is_an_error(tmp_path, make_validation_log):
    inp = tmp_path / "in"
    inp.mkdir()
    _make_asc(inp, "EV299XX24_X.asc")
    log = make_validation_log(
        [{"file": LAYOUT_2023, "status": "success"}, {"file": LAYOUT_2024, "status": "failed"}]
    )

    with pytest.raises(LayoutMatchError, match="validatiestatus 'failed'"):
        match_files(str(inp), str(log))


import json

import pytest
import polars as pl
from pathlib import Path

from eencijferho.utils.converter_match import load_input_files, match_files


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


# --- jaargang-matching (issue #198) ---
#
# Een EV-bestand draagt zijn jaar als `XX<yy>` in de naam (`EV299XX24` = 2024).
# De BB draagt het jaar als volledig jaar in de bestandsnaam. We vergelijken `yy`
# met de laatste twee cijfers van het BB-jaar, zodat er niets geraden hoeft te
# worden naar de eeuw.

MATCH_LOG = "(4)_file_matching_log_latest.json"


def _file_log(tmp_path, input_name):
    """Het `processed_files`-element voor één invoerbestand uit het matchlog.

    `match_files` schrijft de matchstatus in het logbestand; dat is het contract
    waar `converter.py` op leest. De teruggegeven dataframe bevat alleen de
    rijen van daadwerkelijk gematchte validatiebestanden.
    """
    log = json.loads((tmp_path / "logs" / MATCH_LOG).read_text(encoding="utf-8"))
    return next(e for e in log["processed_files"] if e["input_file"] == input_name)


def test_match_files_ev_kiest_bb_van_hetzelfde_jaar(tmp_path, make_validation_log):
    _make_asc(tmp_path, "EV299XX24_DEMO.asc")
    log = make_validation_log([
        {"file": "Bestandsbeschrijving_1cyferho_2023_v1.1.xlsx", "status": "success"},
        {"file": "Bestandsbeschrijving_1cyferho_2024_v1.1.xlsx", "status": "success"},
    ])
    result = match_files(str(tmp_path), str(log))
    matched = result["input_matches"].filter(pl.col("input_file") == "EV299XX24_DEMO.asc")
    assert matched["validation_file"].to_list() == [
        "Bestandsbeschrijving_1cyferho_2024_v1.1.xlsx"
    ]


def test_match_files_ev_jaarmismatch_is_expliciet(tmp_path, make_validation_log):
    _make_asc(tmp_path, "EV299XX24_DEMO.asc")
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_1cyferho_2023_v1.1.xlsx", "status": "success"}]
    )
    match_files(str(tmp_path), str(log))
    entry = _file_log(tmp_path, "EV299XX24_DEMO.asc")
    assert entry["status"] == "year_mismatch"
    assert entry["year"] == 2024


def test_match_files_ev_jaarmismatch_noemt_beschikbaar_jaar(tmp_path, make_validation_log):
    _make_asc(tmp_path, "EV299XX24_DEMO.asc")
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_1cyferho_2023_v1.1.xlsx", "status": "success"}]
    )
    match_files(str(tmp_path), str(log))
    entry = _file_log(tmp_path, "EV299XX24_DEMO.asc")
    assert [m["validation_file"] for m in entry["matches"]] == [
        "Bestandsbeschrijving_1cyferho_2023_v1.1.xlsx"
    ]
    assert [m["validation_year"] for m in entry["matches"]] == [2023]


def test_match_files_ev_zonder_jaar_in_naam_valt_terug(tmp_path, make_validation_log):
    _make_asc(tmp_path, "EV_zonder_jaar.asc")
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_1cyferho_2023_v1.1.xlsx", "status": "success"}]
    )
    match_files(str(tmp_path), str(log))
    assert _file_log(tmp_path, "EV_zonder_jaar.asc")["status"] == "matched"


def test_match_files_vakhavw_heeft_geen_jaar_in_bb(tmp_path, make_validation_log):
    _make_asc(tmp_path, "VAKHAVW_99XX_DEMO.asc")
    log = make_validation_log(
        [{"file": "Bestandsbeschrijving_Vakgegevens.xlsx", "status": "success"}]
    )
    match_files(str(tmp_path), str(log))
    assert _file_log(tmp_path, "VAKHAVW_99XX_DEMO.asc")["status"] == "matched"

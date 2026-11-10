"""The decode steps write a machine-readable decode log (#210).

Synthetic data only: every code and identifier below is made up.
"""

import json
from unittest.mock import patch

import polars as pl
import pytest

from eencijferho.config import OutputConfig
from eencijferho.core.decode_report import DecodeReport, is_personal_id_column
from eencijferho.core.decoder import (
    _apply_single_dec_join,
    _apply_single_mapping,
    _apply_vakken_layout,
    _apply_vakken_simple,
    decode_fields,
    decode_fields_dec_only,
    load_dec_tables_from_metadata,
    load_variable_mappings,
)
from eencijferho.core.pipeline import run_turbo_convert_pipeline

# Seven distinct unknown country codes: more than the sample cap of five.
UNKNOWN_LAND = ["9001", "9002", "9003", "9004", "9005", "9006", "9007"]

MAIN_ROWS = {
    "Burgerservicenummer": ["000000012"] * 3 + ["000000024"] * 7,
    "Geboorteland": ["6030", "6030", "5010"] + UNKNOWN_LAND,
    "Nationaliteit": ["1", "1", "1", "1", "1", "1", "1", "1", "1", "1"],
    "Soort opleiding": ["A"] * 10,
    "Geslacht": ["M", "V", "M", "V", "M", "V", "M", "V", "M", "X"],
}


def _dec_meta() -> dict:
    return {
        "tables": [
            {
                "table_title": "Dec_landcode.asc",
                "decoding_variables": ["Geboorteland"],
                "content": ["Startpositie  Naam", "Code land     1     4", "Omschrijving"],
            },
            {
                "table_title": "Dec_nationaliteit.asc",
                "decoding_variables": ["Nationaliteit"],
                "content": ["Startpositie  Naam", "Code nat      1     1", "Omschrijving"],
            },
            {
                "table_title": "Dec_opleiding.asc",
                "decoding_variables": ["Soort opleiding"],
                "content": ["Startpositie  Naam", "Code opl      1     1", "Omschrijving"],
            },
        ]
    }


@pytest.fixture
def decode_setup(tmp_path):
    """Dec JSON, Dec CSVs (one missing from disk) and variable_metadata."""
    dec_dir = tmp_path / "out"
    dec_dir.mkdir()
    meta_path = tmp_path / "Bestandsbeschrijving_Dec-bestanden.json"
    meta_path.write_text(json.dumps(_dec_meta()), encoding="utf-8")
    (dec_dir / "Dec_landcode.csv").write_text(
        "Code land;Omschrijving\n6030;Nederland\n5010;Belgie\n", encoding="utf-8"
    )
    (dec_dir / "Dec_opleiding.csv").write_text(
        "Code opl;Omschrijving\nA;Bachelor\n", encoding="utf-8"
    )
    # Dec_nationaliteit.csv is deliberately absent.
    var_meta = tmp_path / "variable_metadata.json"
    var_meta.write_text(
        json.dumps([{"name": "Geslacht", "values": {"M": "Man", "V": "Vrouw"}}]),
        encoding="utf-8",
    )
    return {"meta": str(meta_path), "dec_dir": str(dec_dir), "var_meta": str(var_meta)}


def _decode_both_passes(setup, report, decode_columns=None):
    df = pl.DataFrame(MAIN_ROWS)
    dec_tables = load_dec_tables_from_metadata(setup["meta"], setup["dec_dir"], report=report)
    report.begin_file("EV_TEST.csv", df.columns)
    dec_only = decode_fields_dec_only(
        df, setup["meta"], dec_tables, decode_columns=decode_columns, report=report,
    )
    enriched = decode_fields(
        df, setup["meta"], dec_tables, variable_metadata_path=setup["var_meta"],
        decode_columns=decode_columns, report=report,
    )
    return dec_only, enriched


def _detail(report, name="EV_TEST.csv"):
    return next(d for d in report.to_dict()["details"] if d["file"] == name)


# ---------------------------------------------------------------------------
# Dec tables
# ---------------------------------------------------------------------------


def test_missing_dec_csv_is_listed_as_not_loaded(decode_setup):
    report = DecodeReport()
    tables = load_dec_tables_from_metadata(decode_setup["meta"], decode_setup["dec_dir"], report=report)

    assert set(tables) == {"Dec_landcode.asc", "Dec_opleiding.asc"}
    data = report.to_dict()
    assert data["dec_tables_total"] == 3
    assert data["dec_tables_loaded"] == 2
    assert data["dec_tables_not_loaded"] == [
        {
            "table_title": "Dec_nationaliteit.asc",
            "file": "Dec_nationaliteit.csv",
            "reason": "file_not_found",
        }
    ]


def test_vakken_dec_table_not_loaded_is_recorded():
    vakken_meta = {
        "tables": [
            {
                "table_title": "Vakken",
                "content": [
                    "Naam   Startpositie   Aantal posities   Opmerking",
                    "Vak    1              2                 te decoderen met Dec_vak.asc",
                ],
            }
        ]
    }
    report = DecodeReport()
    report.begin_file("VAKHAVW_TEST.csv", ["Vak"])
    report.start_pass({"vak": "Vak"}, ["Vak"])

    _apply_vakken_layout(pl.DataFrame({"vak": ["1"]}), vakken_meta, {}, {}, report=report)

    data = report.to_dict()
    assert data["dec_tables_not_loaded"][0]["table_title"] == "Dec_vak.asc"
    assert _detail(report, "VAKHAVW_TEST.csv")["columns_not_decoded"] == [
        {"column": "Vak", "reason": "dec_table_not_loaded"}
    ]


# ---------------------------------------------------------------------------
# Per-file content
# ---------------------------------------------------------------------------


def test_columns_decoded_and_not_decoded_cover_all_input_columns(decode_setup):
    report = DecodeReport()
    _decode_both_passes(decode_setup, report)
    detail = _detail(report)

    assert detail["columns_total"] == len(MAIN_ROWS)
    covered = detail["columns_decoded"] + [c["column"] for c in detail["columns_not_decoded"]]
    assert sorted(covered) == sorted(MAIN_ROWS)
    assert detail["columns_decoded"] == ["Geboorteland", "Soort opleiding", "Geslacht"]
    assert detail["decoded_via"]["Geslacht"] == ["variable_metadata"]
    assert detail["decoded_via"]["Geboorteland"] == ["dec"]
    assert {c["column"]: c["reason"] for c in detail["columns_not_decoded"]} == {
        "Burgerservicenummer": "no_dec_table",
        "Nationaliteit": "dec_table_not_loaded",
    }


def test_decode_columns_filter_gives_not_in_decode_scope(decode_setup):
    report = DecodeReport()
    _decode_both_passes(decode_setup, report, decode_columns=["Geboorteland"])
    reasons = {c["column"]: c["reason"] for c in _detail(report)["columns_not_decoded"]}
    assert reasons["Soort opleiding"] == "not_in_decode_scope"
    assert reasons["Nationaliteit"] == "not_in_decode_scope"


def test_unmatched_codes_are_aggregated_with_capped_sample(decode_setup):
    report = DecodeReport()
    _decode_both_passes(decode_setup, report)
    detail = _detail(report)

    assert detail["unmatched_codes"] == [
        {
            "column": "Geboorteland",
            "dec_table": "Dec_landcode.asc",
            "rows_unmatched": 7,
            "distinct_codes": 7,
            "sample": UNKNOWN_LAND[:5],
        }
    ]
    # Recorded by both passes, counted once.
    assert report.summary()["total_unmatched_distinct_codes"] == 7
    assert report.summary()["total_unmatched_rows"] == 7


def test_dec_variable_with_different_name_is_not_found_not_guessed(decode_setup):
    meta = _dec_meta()
    meta["tables"][0]["decoding_variables"] = ["Geboortelanden"]
    with open(decode_setup["meta"], "w", encoding="utf-8") as f:
        json.dump(meta, f)
    report = DecodeReport()
    _decode_both_passes(decode_setup, report)
    detail = _detail(report)
    assert "Geboorteland" not in detail["columns_decoded"]
    assert "Geboortelanden" in detail["variables_not_found"]["Dec_landcode.asc"]
    assert "fuzzy_matches" not in detail


def test_variable_mapping_with_unmapped_value_is_logged(decode_setup):
    report = DecodeReport()
    _decode_both_passes(decode_setup, report)
    (mapping,) = _detail(report)["variable_mappings"]
    assert mapping["column"] == "Geslacht"
    assert mapping["mapped"] == 9
    assert mapping["total_non_null"] == 10
    assert mapping["unmapped_distinct"] == 1
    assert mapping["sample"] == ["X"]
    assert report.summary()["total_unmapped_distinct_values"] == 1


def test_report_none_leaves_results_identical(decode_setup):
    report = DecodeReport()
    with_report = _decode_both_passes(decode_setup, report)

    df = pl.DataFrame(MAIN_ROWS)
    tables = load_dec_tables_from_metadata(decode_setup["meta"], decode_setup["dec_dir"])
    plain_dec_only = decode_fields_dec_only(df, decode_setup["meta"], tables)
    plain_enriched = decode_fields(
        df, decode_setup["meta"], tables, variable_metadata_path=decode_setup["var_meta"],
    )
    assert with_report[0].equals(plain_dec_only)
    assert with_report[1].equals(plain_enriched)


def test_load_variable_mappings_records_status(tmp_path, decode_setup):
    report = DecodeReport()
    load_variable_mappings(decode_setup["var_meta"], report=report)
    assert report.to_dict()["variable_metadata"] == {
        "file": "variable_metadata.json", "status": "loaded", "mappings_loaded": 1,
    }


# ---------------------------------------------------------------------------
# Join helpers
# ---------------------------------------------------------------------------


def test_int_fallback_is_counted():
    report = DecodeReport()
    df = pl.DataFrame({"code": ["00411", "411", "7"]})
    _apply_single_mapping(df, "code", {"411": "Label", 411: "Label"}, "code", "Code", report=report)
    (entry,) = report.to_dict()["details"][0]["variable_mappings"]
    assert entry["int_fallback_rows"] == 1
    assert entry["int_fallback_distinct"] == 1
    assert entry["int_fallback_sample"] == ["00411"]
    assert report.summary()["total_int_fallback_rows"] == 1


def test_composite_dec_join_logs_key_pairs():
    report = DecodeReport()
    df = pl.DataFrame({"instcode": ["A1", "A1", "B2"], "vakcode": ["10", "99", "10"]})
    dec = pl.DataFrame(
        {"instcode": ["A1", "B2"], "vakcode": ["10", "10"], "omschrijving": ["X", "Y"]}
    )
    _apply_single_dec_join(
        df, dec, "vakcode", "instcode", True, "vakcode",
        report=report, dec_table_title="Dec_vak.asc",
    )
    (entry,) = report.to_dict()["details"][0]["unmatched_codes"]
    assert entry["column"] == "vakcode"
    assert entry["key_columns"] == ["instcode", "vakcode"]
    assert entry["distinct_codes"] == 1
    assert entry["sample"] == ["A1|99"]


def test_join_error_fails_strict():
    report = DecodeReport()
    report.begin_file("EV_TEST.csv", ["vakcode"])
    df = pl.DataFrame({"instcode": ["A1"], "vakcode": ["10"]})
    dec = pl.DataFrame(
        {"instcode": ["A1", "A1"], "vakcode": ["10", "10"], "omschrijving": ["X", "Y"]}
    )
    _apply_single_dec_join(
        df, dec, "vakcode", "instcode", True, "vakcode",
        report=report, dec_table_title="Dec_vak.asc",
    )
    summary = report.summary()
    assert summary["errors"] == 1
    assert summary["total_unmatched_distinct_codes"] == 0
    assert summary["strict_violations"] == ["1 fout(en) bij decoderen"]


def test_vakken_simple_unmatched_is_logged():
    report = DecodeReport()
    df = pl.DataFrame({"vak": ["1", "2", "2"]})
    dec = pl.DataFrame({"code": ["1"], "omschrijving": ["Wiskunde"]})
    _apply_vakken_simple(df, dec, "vak", "code", "Vak", "Dec_vak.asc", report=report)
    (entry,) = report.to_dict()["details"][0]["unmatched_codes"]
    assert entry["dec_table"] == "Dec_vak.asc"
    assert entry["rows_unmatched"] == 2
    assert entry["distinct_codes"] == 1


def test_empty_code_is_not_an_unmatched_finding():
    report = DecodeReport()
    report.begin_file("EV_TEST.csv", ["code"])
    df = pl.DataFrame({"code": ["1", " ", ""]})
    dec = pl.DataFrame({"code": ["1"], "omschrijving": ["Een"]})
    _apply_single_dec_join(df, dec, "code", "code", False, None, report=report, dec_table_title="Dec_x.asc")
    assert report.to_dict()["details"][0]["unmatched_codes"] == []


# ---------------------------------------------------------------------------
# Privacy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["Burgerservicenummer", "burgerservicenummer", "Persoonsgebonden nummer",
     "persoonsgebonden_nummer", "Onderwijsnummer", "BSN student"],
)
def test_personal_id_columns_are_detected(name):
    assert is_personal_id_column(name)


@pytest.mark.parametrize("name", ["Geboorteland", "Vestigingsnummer", "Soort opleiding"])
def test_other_columns_are_not_personal_ids(name):
    assert not is_personal_id_column(name)


def test_no_sample_from_personal_id_columns():
    report = DecodeReport()
    df = pl.DataFrame({"onderwijsnummer": ["000000036", "000000063"]})
    dec = pl.DataFrame({"onderwijsnummer": ["000000099"], "omschrijving": ["x"]})
    _apply_single_dec_join(
        df, dec, "onderwijsnummer", "onderwijsnummer", False, None,
        report=report, dec_table_title="Dec_x.asc",
    )
    _apply_single_mapping(
        pl.DataFrame({"burgerservicenummer": ["000000012"]}),
        "burgerservicenummer", {"1": "x"}, "burgerservicenummer", "Burgerservicenummer",
        report=report,
    )
    detail = report.to_dict()["details"][0]
    (unmatched,) = detail["unmatched_codes"]
    assert unmatched["sample"] == []
    assert unmatched["distinct_codes"] == 2
    (mapping,) = detail["variable_mappings"]
    assert mapping["sample"] == []
    assert mapping["unmapped_distinct"] == 1
    text = json.dumps(report.to_dict())
    assert "000000036" not in text and "000000012" not in text


# ---------------------------------------------------------------------------
# Pipeline: the (7) log
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline_dirs(tmp_path, decode_setup, make_csv):
    """Metadata and output dirs for run_turbo_convert_pipeline, conversion stubbed."""
    out = tmp_path / "out"
    metadata = tmp_path / "metadata"
    (metadata / "json").mkdir(parents=True)
    (metadata / "json" / "Bestandsbeschrijving_Dec-bestanden.json").write_text(
        json.dumps(_dec_meta()), encoding="utf-8"
    )
    (metadata / "json" / "variable_metadata.json").write_text(
        (tmp_path / "variable_metadata.json").read_text(encoding="utf-8"), encoding="utf-8"
    )
    rows = [dict(zip(MAIN_ROWS, values)) for values in zip(*MAIN_ROWS.values())]
    make_csv(out / "EV_TEST.csv", rows)
    make_csv(out / "VAKHAVW_TEST.csv", [{"Vak": "1"}])
    with patch("eencijferho.core.pipeline.converter"), patch("eencijferho.core.pipeline.cv"):
        yield {"out": out, "metadata": metadata}


def _run_pipeline(dirs, **config):
    report = DecodeReport()
    run_turbo_convert_pipeline(
        input_dir="unused",
        output_dir=str(dirs["out"]),
        metadata_dir=str(dirs["metadata"]),
        output_config=OutputConfig(formats=[], column_casing="none", **config),
        decode_report=report,
    )
    return report


def _read_logs(dirs):
    logs = dirs["metadata"] / "logs"
    latest = logs / "(7)_decoding_log_latest.json"
    stamped = list(logs.glob("decoding_log_*.json"))
    assert len(stamped) == 1
    assert latest.read_bytes() == stamped[0].read_bytes()
    return json.loads(latest.read_text(encoding="utf-8"))


def test_pipeline_writes_decoding_log(pipeline_dirs):
    report = _run_pipeline(pipeline_dirs)
    data = _read_logs(pipeline_dirs)

    assert report.log_path.endswith("(7)_decoding_log_latest.json")
    assert data["status"] == "completed"
    assert {"timestamp", "details", "summary", "dec_tables_total", "dec_tables_loaded"} <= set(data)
    files = {d["file"]: d for d in data["details"]}
    assert set(files) == {"EV_TEST.csv", "VAKHAVW_TEST.csv"}
    assert files["EV_TEST.csv"]["decoded"] == "written"
    assert files["EV_TEST.csv"]["enriched"] == "written"
    assert files["VAKHAVW_TEST.csv"]["enriched"] == "skipped_no_mappings"
    assert data["summary"]["total_unmatched_distinct_codes"] == 7
    assert data["summary"]["strict_violations"]


def test_pipeline_without_decode_writes_empty_log(pipeline_dirs):
    report = _run_pipeline(pipeline_dirs, variants=[])
    data = _read_logs(pipeline_dirs)

    assert data["details"] == []
    assert data["decode_enabled"] is False
    assert data["dec_tables_not_loaded"] == []
    assert report.summary()["strict_violations"] == []


def test_findings_without_begin_file_go_to_a_dataframe_entry():
    """Without begin_file, findings land in one "<dataframe>" entry."""
    report = DecodeReport()
    report.set_file_status(decoded="written")
    report.record_skip("geslacht", "no_dec_table")

    details = report.to_dict()["details"]
    assert [d["file"] for d in details] == ["<dataframe>"]
    assert details[0]["decoded"] == "written"


def test_composite_join_without_second_code_column_logs_a_type_error():
    """A composite join without its second code column keeps the frame and logs a TypeError."""
    main = pl.DataFrame({"instcode": ["10"], "vakcode": ["01"]})
    lookup = pl.DataFrame({"instcode": ["10"], "vakcode": ["1"], "vaknaam": ["Wiskunde"]})
    report = DecodeReport()

    result = _apply_single_dec_join(
        main, lookup, "vakcode", "instcode",
        is_composite=True, code_col2_norm=None, report=report,
    )

    assert result.equals(main)
    errors = report.to_dict()["details"][0]["errors"]
    assert errors == [{"column": "vakcode", "step": "dec_join", "error_type": "TypeError"}]

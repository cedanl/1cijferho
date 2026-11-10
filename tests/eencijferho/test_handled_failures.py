"""Forced failures land in the run log with file, column and error type (#211)."""

import json
import logging
import sys

import polars as pl
import pytest

from eencijferho import cli
from eencijferho.core import converter, decoder
from eencijferho.core.decode_report import DecodeReport
from eencijferho.core.decoder_info import (
    get_available_decode_columns,
    get_available_enrich_variables,
    get_decode_column_info,
    get_enrich_variable_info,
)
from eencijferho.core.pipeline import _collect_output_files
from eencijferho.utils.dec_validation import validate_with_dec_files
from eencijferho.utils.run_log import RUN_LOG_FILENAME, setup_logging, teardown_logging

FAKE_BSN = "123456782"


@pytest.fixture
def run_log_text(tmp_path):
    """Non-debug run log in tmp_path/logs; call the returned function to read it."""
    setup_logging(str(tmp_path / "logs"), debug=False)
    path = tmp_path / "logs" / RUN_LOG_FILENAME

    def read() -> str:
        teardown_logging()
        return path.read_text(encoding="utf-8")

    yield read
    teardown_logging()


def _broken_json(tmp_path, name="variable_metadata.json"):
    path = tmp_path / name
    path.write_text("{not json", encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# decoder
# ---------------------------------------------------------------------------


def test_unreadable_variable_metadata_is_logged_with_file_and_type(tmp_path, run_log_text):
    report = DecodeReport()
    maps = decoder.load_variable_mappings(str(_broken_json(tmp_path)), report=report)

    assert maps == {}
    assert report.variable_metadata["status"] == "unreadable"
    text = run_log_text()
    assert "outcome=failed step=variable_metadata_load file=variable_metadata.json" in text
    assert "error=JSONDecodeError" in text


def test_non_numeric_mapping_keys_stay_string_keys(tmp_path):
    path = tmp_path / "variable_metadata.json"
    path.write_text(
        json.dumps([{"name": "Soort", "values": {"A": "Alpha", "²": "kwadraat", "01": "een"}}]),
        encoding="utf-8",
    )
    mapping = decoder.load_variable_mappings(str(path))["soort"]["mapping"]
    assert mapping["A"] == "Alpha"
    assert mapping["²"] == "kwadraat"
    assert mapping[1] == "een"


def test_malformed_dec_csv_is_logged_and_recorded(tmp_path, run_log_text):
    dec_dir = tmp_path / "out"
    dec_dir.mkdir()
    # More fields than the header: unreadable with and without quote parsing.
    (dec_dir / "Dec_test.csv").write_text("code;naam\n1;een;extra\n", encoding="utf-8")
    meta = tmp_path / "Bestandsbeschrijving_Dec-bestanden.json"
    meta.write_text(json.dumps({"tables": [{"table_title": "Dec_test.asc"}]}), encoding="utf-8")
    report = DecodeReport()

    tables = decoder.load_dec_tables_from_metadata(str(meta), str(dec_dir), report=report)

    assert tables == {}
    assert report.to_dict()["dec_tables_not_loaded"] == [
        {"table_title": "Dec_test.asc", "file": "Dec_test.csv", "reason": "read_error"}
    ]
    text = run_log_text()
    assert "outcome=skipped step=dec_load file=Dec_test.csv reason=retry_without_quote_parsing" in text
    assert "outcome=failed step=dec_load file=Dec_test.csv error=ComputeError" in text


def test_dec_join_failure_names_file_and_column_but_no_value(monkeypatch, run_log_text):
    def explode(*args, **kwargs):
        raise pl.exceptions.ComputeError(f"could not parse `{FAKE_BSN}` as Int64")

    monkeypatch.setattr(decoder, "_keyed_lookup", explode)
    report = DecodeReport()
    report.begin_file("EV23DEMO.csv", ["code"])
    data = pl.DataFrame({"code": [FAKE_BSN]})
    dec = pl.DataFrame({"code": ["1"], "naam": ["x"]})

    decoder._apply_single_dec_join(
        data, dec, "code", "code", is_composite=False, code_col2_norm=None,
        report=report, dec_table_title="Dec_x.asc",
    )

    assert report.to_dict()["details"][0]["errors"] == [
        {"column": "code", "step": "dec_join", "error_type": "ComputeError"}
    ]
    text = run_log_text()
    assert "outcome=failed step=dec_join file=EV23DEMO.csv column=code error=ComputeError" in text
    assert FAKE_BSN not in text


def test_column_mapping_failure_is_logged_with_column(run_log_text):
    report = DecodeReport()
    report.begin_file("EV23DEMO.csv", ["geslacht"])
    df = pl.DataFrame({"geslacht": ["M"]})

    result = decoder._apply_single_mapping(df, "geslacht", None, "geslacht", "Geslacht", report=report)

    assert result.equals(df)
    text = run_log_text()
    assert (
        "outcome=failed step=variable_mapping file=EV23DEMO.csv column=geslacht error=AttributeError"
        in text
    )


def test_mixed_type_mapping_still_records_the_error(caplog):
    """pl.Series of str and int raises TypeError in both attempts: recorded, not raised."""
    report = DecodeReport()
    report.begin_file("EV.csv", ["c"])
    df = pl.DataFrame({"c": ["1", "x"]})
    decoder._apply_single_mapping(df, "c", {"1": 7}, "c", "C", report=report)
    assert report.to_dict()["details"][0]["errors"] == [
        {"column": "c", "step": "variable_mapping", "error_type": "TypeError"}
    ]


# ---------------------------------------------------------------------------
# decoder_info, dec_validation, pipeline, converter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "func, empty",
    [
        (get_available_decode_columns, []),
        (get_decode_column_info, {}),
        (get_enrich_variable_info, {}),
        (get_available_enrich_variables, []),
    ],
)
def test_decoder_info_logs_why_it_returns_nothing(tmp_path, caplog, func, empty):
    assert func(str(_broken_json(tmp_path, "meta.json"))) == empty
    record = caplog.records[-1]
    assert record.levelno == logging.ERROR
    assert "outcome=failed" in record.run_log_fields
    assert "file=meta.json error=JSONDecodeError" in record.run_log_fields


def test_dec_validation_logs_an_unreadable_dec_csv(tmp_path, caplog):
    main_csv = tmp_path / "EV.csv"
    main_csv.write_text("Geboorteland\n0001\n", encoding="utf-8")
    (tmp_path / "Dec_landcode.csv").write_text("a;b\n1;2;3\n", encoding="utf-8")
    mapping = {"Dec_landcode": {"type": "simple", "columns": ["Geboorteland"]}}

    _, results = validate_with_dec_files(main_csv, tmp_path, mapping)

    assert results["columns_checked"] == 0
    fields = caplog.records[-1].run_log_fields
    assert fields.startswith("outcome=failed step=dec_validation file=Dec_landcode.csv")
    assert "error=ComputeError" in fields


def test_dec_validation_logs_an_unreadable_main_csv(tmp_path, caplog):
    _, results = validate_with_dec_files(tmp_path / "missing.csv", tmp_path, {})
    assert "load_error" in results
    assert "file=missing.csv error=FileNotFoundError" in caplog.records[-1].run_log_fields


def test_output_size_failure_is_logged(caplog):
    class Storage:
        def list_files(self, pattern):
            return ["out/EV.csv"]

        def read_bytes(self, path):
            raise OSError("gone")

    assert _collect_output_files(Storage(), "out")[0]["size"] == 0
    assert "outcome=failed step=output_size file=EV.csv error=OSError" in caplog.records[-1].run_log_fields


def test_conversion_failure_keeps_reason_and_is_logged(tmp_path, monkeypatch, caplog):
    (tmp_path / "EV.asc").write_text("x\n", encoding="latin-1")
    (tmp_path / "layout.xlsx").write_bytes(b"")

    def fail(*args, **kwargs):
        raise ValueError("Layout-inconsistent in layout.xlsx")

    monkeypatch.setattr(converter, "converter", fail)
    file_info = {
        "input_file": "EV.asc",
        "status": "matched",
        "matches": [{"validation_file": "layout.xlsx", "validation_status": "success"}],
    }

    result = converter._convert_one(file_info, str(tmp_path), str(tmp_path), str(tmp_path))

    assert result["status"] == "failed"
    assert result["reason"] == "Fout tijdens omzetting: Layout-inconsistent in layout.xlsx"
    fields = caplog.records[-1].run_log_fields
    assert fields.startswith("outcome=failed step=convert file=EV.asc error=ValueError")
    assert "message=" in fields  # file-level: the message is safe and useful


# ---------------------------------------------------------------------------
# CLI: run log file and --debug
# ---------------------------------------------------------------------------


@pytest.fixture
def enrich_case(tmp_path, monkeypatch):
    """An output folder whose Dec CSV is missing and whose variable_metadata is broken."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "in").mkdir()
    json_dir = tmp_path / "out" / "metadata" / "json"
    json_dir.mkdir(parents=True)
    (json_dir / "Bestandsbeschrijving_Dec-bestanden_DEMO.json").write_text(
        json.dumps({"tables": [{"table_title": "Dec_land.asc"}]}), encoding="utf-8"
    )
    _broken_json(json_dir)
    return tmp_path / "out" / "metadata" / "logs" / RUN_LOG_FILENAME


def _run_cli(monkeypatch, *extra):
    monkeypatch.setattr(sys, "argv", ["eencijferho", "enrich", "--input", "in", "--output", "out", *extra])
    cli.main()


def test_cli_writes_run_log_and_one_line_per_problem(enrich_case, monkeypatch, capsys):
    _run_cli(monkeypatch)

    captured = capsys.readouterr()
    assert "Traceback" not in captured.out + captured.err
    assert "[decoder] Kon Dec_land.csv niet laden (FileNotFoundError)" in captured.err
    assert "kon variable metadata niet laden" in captured.err

    text = enrich_case.read_text(encoding="utf-8")
    assert "eencijferho enrich gestart" in text
    assert "outcome=failed step=dec_load file=Dec_land.csv error=FileNotFoundError" in text
    assert "outcome=failed step=variable_metadata_load file=variable_metadata.json error=JSONDecodeError" in text
    assert "Traceback" not in text


def test_cli_debug_shows_full_traceback(enrich_case, monkeypatch, capsys):
    _run_cli(monkeypatch, "--debug")

    err = capsys.readouterr().err
    assert "Traceback (most recent call last)" in err
    assert "JSONDecodeError" in err
    assert "Traceback" in enrich_case.read_text(encoding="utf-8")


@pytest.mark.parametrize("command", ["extract", "validate", "decode", "enrich", "convert", "pipeline", "validate-output"])
def test_debug_flag_on_every_command(monkeypatch, capsys, command):
    monkeypatch.setattr(sys, "argv", ["eencijferho", command, "--help"])
    with pytest.raises(SystemExit):
        cli.main()
    assert "--debug" in capsys.readouterr().out


def test_cli_without_local_disk_skips_the_file_handler(monkeypatch):
    monkeypatch.setattr(cli, "get_backend", lambda: object())
    assert cli._run_log_dir("out") is None

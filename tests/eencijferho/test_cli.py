"""Tests voor _build_output_config in eencijferho.cli."""

import argparse
import sys
from unittest.mock import patch

import pytest
from eencijferho import cli
from eencijferho.cli import _build_output_config
from eencijferho.core.decode_report import DecodeReport


def _args(**kwargs) -> argparse.Namespace:
    """Bouw een Namespace met alle vlaggen op hun standaardwaarde (False/None)."""
    defaults = dict(
        skip_decode=False,
        skip_enrich=False,
        skip_parquet=False,
        skip_snake_case=False,
        skip_ev=False,
        skip_vakhavw=False,
        decode_columns=None,
        enrich_variables=None,
        bsn_mapping_file=None,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# Standaard (geen vlaggen)
# ---------------------------------------------------------------------------

def test_defaults_produce_full_config():
    cfg = _build_output_config(_args())
    assert cfg.variants == ["decoded", "enriched"]
    assert cfg.formats == ["parquet"]
    assert cfg.column_casing == "snake_case"
    assert cfg.convert_ev is True
    assert cfg.convert_vakhavw is True


# ---------------------------------------------------------------------------
# skip-decode / skip-enrich nesting (hier zat eerder een bug)
# ---------------------------------------------------------------------------

def test_skip_decode_removes_both_variants():
    cfg = _build_output_config(_args(skip_decode=True))
    assert cfg.variants == []


def test_skip_enrich_keeps_decoded():
    cfg = _build_output_config(_args(skip_enrich=True))
    assert cfg.variants == ["decoded"]
    assert "enriched" not in cfg.variants


def test_skip_decode_ignores_skip_enrich():
    """Als decoded overgeslagen wordt, mag enriched er niet stiekem in sluipen."""
    cfg = _build_output_config(_args(skip_decode=True, skip_enrich=False))
    assert "enriched" not in cfg.variants
    assert "decoded" not in cfg.variants


# ---------------------------------------------------------------------------
# Overige vlaggen
# ---------------------------------------------------------------------------

def test_skip_parquet_produces_empty_formats():
    cfg = _build_output_config(_args(skip_parquet=True))
    assert cfg.formats == []


def test_skip_snake_case():
    cfg = _build_output_config(_args(skip_snake_case=True))
    assert cfg.column_casing == "none"


def test_skip_ev_only():
    cfg = _build_output_config(_args(skip_ev=True))
    assert cfg.convert_ev is False
    assert cfg.convert_vakhavw is True


def test_skip_vakhavw_only():
    cfg = _build_output_config(_args(skip_vakhavw=True))
    assert cfg.convert_vakhavw is False
    assert cfg.convert_ev is True


def test_skip_ev_and_vakhavw_independent():
    cfg = _build_output_config(_args(skip_ev=True, skip_vakhavw=True))
    assert cfg.convert_ev is False
    assert cfg.convert_vakhavw is False


# ---------------------------------------------------------------------------
# decode_columns / enrich_variables
# ---------------------------------------------------------------------------

def test_decode_columns_passed_through():
    cfg = _build_output_config(_args(decode_columns=["geboorteland"]))
    assert cfg.decode_columns == ["geboorteland"]


def test_decode_columns_empty_list_becomes_none():
    cfg = _build_output_config(_args(decode_columns=[]))
    assert cfg.decode_columns is None


def test_enrich_variables_passed_through():
    cfg = _build_output_config(_args(enrich_variables=["Geslacht"]))
    assert cfg.enrich_variables == ["Geslacht"]


def test_enrich_variables_empty_list_becomes_none():
    cfg = _build_output_config(_args(enrich_variables=[]))
    assert cfg.enrich_variables is None


# ---------------------------------------------------------------------------
# bsn_mapping_file
# ---------------------------------------------------------------------------

def test_bsn_mapping_file_default_is_none():
    cfg = _build_output_config(_args())
    assert cfg.bsn_mapping_file is None


def test_bsn_mapping_file_passed_through():
    cfg = _build_output_config(_args(bsn_mapping_file="data/mapping.csv"))
    assert cfg.bsn_mapping_file == "data/mapping.csv"


# ---------------------------------------------------------------------------
# --strict (#210)
# ---------------------------------------------------------------------------


def _fake_pipeline(findings: bool):
    """Stub voor run_turbo_convert_pipeline die het meegegeven rapport vult."""

    def _run(**kwargs):
        report: DecodeReport = kwargs["decode_report"]
        if findings:
            report.record_dec_table_not_loaded("Dec_test.asc", "file_not_found")
        report.log_path = "out/metadata/logs/(7)_decoding_log_latest.json"
        return "", [{"name": "EV.csv", "size": 1, "size_formatted": "0.0 KB"}]

    return _run


@pytest.fixture
def stub_cli_steps(tmp_path, monkeypatch):
    """Draai cmd_convert/cmd_pipeline zonder echte extractie of conversie."""
    monkeypatch.chdir(tmp_path)
    for name in (
        "process_txt_folder", "write_variable_metadata", "process_json_folder",
        "validate_metadata_folder", "match_files",
    ):
        monkeypatch.setattr(cli, name, lambda *a, **k: None)


def _cli_args(**kwargs) -> argparse.Namespace:
    return _args(input="in", output="out", preset=None, fail_on_empty_output=False, **kwargs)


@pytest.mark.parametrize("command", [cli.cmd_convert, cli.cmd_pipeline])
def test_strict_exits_1_with_findings(command, stub_cli_steps, capsys):
    with (
        patch.object(cli, "run_turbo_convert_pipeline", _fake_pipeline(findings=True)),
        pytest.raises(SystemExit) as exc,
    ):
        command(_cli_args(strict=True))
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "Dec-tabel" in err
    assert "(7)_decoding_log_latest.json" in err


@pytest.mark.parametrize("command", [cli.cmd_convert, cli.cmd_pipeline])
def test_without_strict_findings_do_not_fail(command, stub_cli_steps):
    with patch.object(cli, "run_turbo_convert_pipeline", _fake_pipeline(findings=True)):
        command(_cli_args(strict=False))


@pytest.mark.parametrize("command", [cli.cmd_convert, cli.cmd_pipeline])
def test_strict_without_findings_passes(command, stub_cli_steps):
    with patch.object(cli, "run_turbo_convert_pipeline", _fake_pipeline(findings=False)):
        command(_cli_args(strict=True))


def test_strict_flag_in_help(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["eencijferho", "pipeline", "--help"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 0
    assert "--strict" in capsys.readouterr().out

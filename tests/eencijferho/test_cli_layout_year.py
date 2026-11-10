"""CLI behaviour for layout errors and --layout-year (#198)."""

import argparse
import sys

import pytest

from eencijferho import cli
from eencijferho.utils.converter_match import LayoutMatchError


def _layout_error() -> LayoutMatchError:
    return LayoutMatchError([
        {
            "input_file": "EV299XX24_X.asc",
            "reason": "Jaar 2024 in de bestandsnaam komt niet overeen.",
            "candidates": ["Bestandsbeschrijving_1cyferho_2019.xlsx"],
        }
    ])


@pytest.fixture
def stub_steps(monkeypatch, tmp_path):
    """Stub everything around match_files; record its calls."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "in").mkdir()
    calls: dict = {"match": [], "convert": 0}
    monkeypatch.setattr(cli, "validate_metadata_folder", lambda **k: None)
    monkeypatch.setattr(cli, "process_txt_folder", lambda *a, **k: None)
    monkeypatch.setattr(cli, "write_variable_metadata", lambda *a, **k: None)
    monkeypatch.setattr(cli, "process_json_folder", lambda *a, **k: None)

    def fake_convert(**kwargs):
        calls["convert"] += 1
        return "", []

    monkeypatch.setattr(cli, "run_turbo_convert_pipeline", fake_convert)
    return calls


def _fail_match(calls):
    def fake(input_folder, log_path, layout_year=None):
        calls["match"].append(layout_year)
        raise _layout_error()
    return fake


@pytest.mark.parametrize("command", ["validate", "pipeline"])
def test_layout_error_exits_1_on_stderr_before_conversion(
    monkeypatch, capsys, stub_steps, command
):
    monkeypatch.setattr(cli, "match_files", _fail_match(stub_steps))
    monkeypatch.setattr(sys, "argv", ["eencijferho", command, "--input", "in", "--output", "out"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "EV299XX24_X.asc" in err
    assert "Bestandsbeschrijving_1cyferho_2019.xlsx" in err
    assert stub_steps["convert"] == 0


@pytest.mark.parametrize("command", ["validate", "pipeline"])
def test_layout_year_flag_reaches_match_files(monkeypatch, stub_steps, command):
    seen = []
    monkeypatch.setattr(
        cli, "match_files",
        lambda input_folder, log_path, layout_year=None: seen.append(layout_year),
    )
    monkeypatch.setattr(
        sys, "argv",
        ["eencijferho", command, "--input", "in", "--output", "out", "--layout-year", "2023"],
    )

    cli.main()

    assert seen == [2023]


def test_layout_year_defaults_to_none(monkeypatch, stub_steps):
    seen = []
    monkeypatch.setattr(
        cli, "match_files",
        lambda input_folder, log_path, layout_year=None: seen.append(layout_year),
    )

    cli.cmd_validate(argparse.Namespace(input="in", output="out"))

    assert seen == [None]


def test_convert_exits_1_on_a_failed_match_log(monkeypatch, capsys, stub_steps):
    from eencijferho.core.converter import MatchLogNotCompletedError

    def refuse(**kwargs):
        raise MatchLogNotCompletedError("Koppelingslog heeft status 'failed'")

    monkeypatch.setattr(cli, "run_turbo_convert_pipeline", refuse)
    monkeypatch.setattr(sys, "argv", ["eencijferho", "convert", "--input", "in", "--output", "out"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 1
    assert "status 'failed'" in capsys.readouterr().err

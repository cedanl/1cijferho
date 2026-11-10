"""
UI tests for the 'Output valideren' page (src/frontend/Modules/Validate_Output.py).

The page is a module-level Streamlit script, so it is driven through
streamlit.testing.v1.AppTest. The app's data directories are relative to the
working directory ("data/..."), so each test runs from its own tmp_path.
"""

import json
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import eencijferho.utils.dec_validation as dv
from eencijferho.io.text_encoding import REPLACEMENT_CHAR, DescriptionEncodingError

PAGE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "frontend"
    / "Modules"
    / "Validate_Output.py"
)
DEC_BUTTON = "DEC codes valideren"
DEC_LOG = Path("data") / "00-metadata" / "logs" / "(5c)_dec_validation_log_latest.json"


@pytest.fixture
def app_dirs(tmp_path, monkeypatch):
    """Run from tmp_path with a DEC file description in input and a CSV in output."""
    monkeypatch.chdir(tmp_path)
    input_dir = tmp_path / "data" / "01-input"
    output_dir = tmp_path / "data" / "02-output"
    input_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    (input_dir / "Bestandsbeschrijving_Dec-bestanden.txt").write_text(
        "stub", encoding="utf-8"
    )
    (output_dir / "EV_DEMO.csv").write_text("a;b\n1;2\n", encoding="utf-8")
    return tmp_path


def _click_dec_button(at: AppTest) -> AppTest:
    button = next(b for b in at.button if b.label == DEC_BUTTON)
    return button.click().run()


def test_damaged_description_is_shown_as_error_not_traceback(app_dirs, monkeypatch):
    error = DescriptionEncodingError(
        "data/01-input/Bestandsbeschrijving_Dec-bestanden.txt",
        [3, 7],
        f"Opleiding v{REPLACEMENT_CHAR}{REPLACEMENT_CHAR}r",
    )

    def _raise(output_dir, dec_txt_path):
        raise error

    monkeypatch.setattr(dv, "validate_with_dec_files_folder", _raise)

    at = AppTest.from_file(str(PAGE), default_timeout=60).run()
    at = _click_dec_button(at)

    assert not at.exception
    messages = [e.value for e in at.error]
    assert str(error) in messages
    assert any("regels 3, 7" in m and "Bestandsbeschrijving_Dec" in m for m in messages)
    assert not (app_dirs / DEC_LOG).exists()


def test_successful_dec_validation_writes_log_and_reports_ok(app_dirs, monkeypatch):
    summary = {
        "EV_DEMO.csv": {
            "results": {
                "columns_checked": 1,
                "columns_failed": 0,
                "column_results": [
                    {
                        "column": "a",
                        "dec_file": "Dec_a.asc",
                        "status": "ok",
                        "invalid_values": [],
                    }
                ],
            }
        }
    }
    monkeypatch.setattr(
        dv, "validate_with_dec_files_folder", lambda output_dir, dec_txt_path: summary
    )

    at = AppTest.from_file(str(PAGE), default_timeout=60).run()
    at = _click_dec_button(at)

    assert not at.exception
    assert not at.error
    log_path = app_dirs / DEC_LOG
    assert log_path.exists()

    log = json.loads(log_path.read_text(encoding="utf-8"))
    assert log["status"] == "completed"
    assert log["total_files_checked"] == 1
    assert log["total_failed_columns"] == 0
    assert "Alle kolommen OK." in [s.value for s in at.success]

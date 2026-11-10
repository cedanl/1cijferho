"""Run log helpers: three outcome kinds, privacy and --debug handler config (#211)."""

import logging

import polars as pl
import pytest

from eencijferho.utils import run_log
from eencijferho.utils.run_log import (
    RUN_LOG_FILENAME,
    log_failed,
    log_skipped,
    log_unexpected,
    setup_logging,
    teardown_logging,
)

FAKE_BSN = "123456782"


@pytest.fixture
def logger():
    return logging.getLogger("eencijferho.tests.run_log")


@pytest.fixture
def run_log_file(tmp_path):
    """A non-debug run log in tmp_path; handlers removed afterwards."""
    path = setup_logging(str(tmp_path / "logs"), debug=False, allowed_root=str(tmp_path))
    yield tmp_path / "logs" / RUN_LOG_FILENAME
    teardown_logging()
    assert path is not None


def _raise(error: BaseException) -> BaseException:
    try:
        raise error
    except BaseException as caught:
        return caught


def test_each_helper_writes_its_marker_file_column_and_error_type(run_log_file, logger):
    log_skipped(logger, "dec_load", "no_mapping", file="data/x/EV2023.csv", column="geslacht")
    log_failed(logger, "dec_join", _raise(KeyError("k")), file="EV2023.csv", column="code_land")
    log_unexpected(logger, "variable_mapping", _raise(TypeError("t")), file="EV2023.csv")
    teardown_logging()

    lines = run_log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    skipped, failed, unexpected = lines
    assert " INFO outcome=skipped step=dec_load file=EV2023.csv column=geslacht reason=no_mapping" in skipped
    assert " ERROR outcome=failed step=dec_join file=EV2023.csv column=code_land error=KeyError" in failed
    assert " ERROR outcome=unexpected step=variable_mapping file=EV2023.csv error=TypeError" in unexpected


def test_levels_and_exc_info(caplog, logger):
    caplog.set_level(logging.INFO, logger="eencijferho")
    log_skipped(logger, "s", "reason")
    log_failed(logger, "s", _raise(ValueError("v")))
    log_unexpected(logger, "s", _raise(ValueError("v")))

    levels = [r.levelno for r in caplog.records]
    assert levels == [logging.INFO, logging.ERROR, logging.ERROR]
    # Only the catch-all carries the traceback in a non-debug run.
    assert [r.exc_info is not None for r in caplog.records] == [False, False, True]


def test_skipped_with_error_writes_error_type(caplog, logger):
    caplog.set_level(logging.INFO, logger="eencijferho")
    log_skipped(logger, "dec_load", "retry", file="Dec_x.csv", error=_raise(OSError("gone")))
    fields = caplog.records[0].run_log_fields
    assert fields == "outcome=skipped step=dec_load file=Dec_x.csv reason=retry error=OSError"


def test_data_level_failure_keeps_message_and_traceback_out_of_the_file(run_log_file, logger):
    """A polars message can quote a cell value: a non-debug log has the type only."""
    error = _raise(pl.exceptions.ComputeError(f"could not parse `{FAKE_BSN}` as Int64"))
    log_failed(logger, "dec_join", error, file="EV.csv", column="bsn")
    log_unexpected(logger, "dec_join", error, file="EV.csv", column="bsn")
    teardown_logging()

    text = run_log_file.read_text(encoding="utf-8")
    assert "error=ComputeError" in text
    assert FAKE_BSN not in text
    assert "Traceback" not in text


def test_message_only_with_show_message(caplog, logger):
    log_failed(logger, "layout_load", _raise(ValueError("Layout-inconsistent")), show_message=True)
    log_failed(logger, "dec_join", _raise(ValueError("cell 42")))
    shown, hidden = caplog.records
    assert 'message=Layout-inconsistent' in shown.run_log_fields
    assert "Layout-inconsistent" in shown.getMessage()
    assert "cell 42" not in hidden.run_log_fields
    assert "cell 42" not in hidden.getMessage()


def test_debug_writes_tracebacks_to_console_and_file(tmp_path, logger, capsys):
    setup_logging(str(tmp_path), debug=True, allowed_root=str(tmp_path))
    try:
        log_failed(logger, "dec_join", _raise(KeyError("k")), detail="[decoder] Fout")
    finally:
        teardown_logging()

    err = capsys.readouterr().err
    assert "[decoder] Fout (KeyError)" in err
    assert "Traceback" in err
    assert "Traceback" in (tmp_path / RUN_LOG_FILENAME).read_text(encoding="utf-8")


def test_console_without_debug_shows_one_line_and_hides_info(tmp_path, logger, capsys):
    setup_logging(str(tmp_path), debug=False, allowed_root=str(tmp_path))
    try:
        log_skipped(logger, "s", "benign")
        log_unexpected(logger, "s", _raise(KeyError("k")), detail="[decoder] Onverwacht")
    finally:
        teardown_logging()

    err = capsys.readouterr().err
    assert err == "[decoder] Onverwacht (KeyError)\n"


def test_setup_without_logs_dir_has_no_file_handler(tmp_path):
    try:
        assert setup_logging(None) is None
        handlers = logging.getLogger(run_log.LOGGER_NAME).handlers
        assert not any(isinstance(h, logging.FileHandler) for h in handlers)
    finally:
        teardown_logging()


def test_setup_twice_keeps_one_set_of_handlers_and_teardown_resets(tmp_path):
    setup_logging(str(tmp_path), debug=True, allowed_root=str(tmp_path))
    setup_logging(str(tmp_path), debug=False, allowed_root=str(tmp_path))
    eencijferho_logger = logging.getLogger(run_log.LOGGER_NAME)
    try:
        assert len(eencijferho_logger.handlers) == 2
        assert eencijferho_logger.level == logging.INFO
    finally:
        teardown_logging()
    assert eencijferho_logger.handlers == []
    assert eencijferho_logger.level == logging.NOTSET


def test_values_with_spaces_are_quoted(caplog, logger):
    log_failed(logger, "s", _raise(OSError("x")), file="a b.csv", column="naam land")
    assert 'file="a b.csv" column="naam land"' in caplog.records[0].run_log_fields


@pytest.mark.parametrize(
    "logs_dir",
    [
        "elsewhere/logs",  # a folder next to the output folder
        "out/../elsewhere/logs",  # '..' that leaves the output folder
        "out2/logs",  # shares the prefix "out" but is another folder
    ],
)
def test_setup_refuses_a_logs_dir_outside_the_allowed_root(tmp_path, logs_dir):
    root = tmp_path / "out"
    root.mkdir()
    with pytest.raises(ValueError, match="outside the output folder"):
        setup_logging(str(tmp_path / logs_dir), allowed_root=str(root))

    # Refused before anything happens: no folder, no file, no handlers.
    assert not (tmp_path / "elsewhere").exists()
    assert not (tmp_path / "out2").exists()
    assert logging.getLogger(run_log.LOGGER_NAME).handlers == []


def test_setup_refuses_a_symlink_that_leaves_the_allowed_root(tmp_path):
    root = tmp_path / "out"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (root / "metadata").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks not available")

    with pytest.raises(ValueError, match="outside the output folder"):
        setup_logging(str(root / "metadata" / "logs"), allowed_root=str(root))
    assert list(outside.iterdir()) == []


def test_setup_accepts_the_allowed_root_and_folders_inside_it(tmp_path):
    root = tmp_path / "out"
    try:
        assert setup_logging(str(root), allowed_root=str(root)) == str(root / RUN_LOG_FILENAME)
        nested = root / "metadata" / "logs"
        assert setup_logging(str(nested), allowed_root=str(root)) == str(nested / RUN_LOG_FILENAME)
    finally:
        teardown_logging()
    assert (nested / RUN_LOG_FILENAME).is_file()

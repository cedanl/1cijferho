"""
Run log: one text log of what a run skipped, what failed and what was unexpected.

Every broad ``except`` in ``core/`` and ``utils/`` reports through one of three
helpers, so the three outcomes stay apart in the code and in the log:

- :func:`log_skipped`: a step was skipped for a known, benign reason
  (INFO, ``outcome=skipped`` with a ``reason`` code).
- :func:`log_failed`: an operation failed and its result is degraded or
  missing (ERROR, ``outcome=failed``).
- :func:`log_unexpected`: a catch-all caught something nobody planned for
  (ERROR, ``outcome=unexpected``, traceback attached).

Each line carries ``key=value`` pairs: ``outcome``, ``step``, ``file`` and
``column`` when known, then ``reason`` or ``error`` (the exception type).

Privacy: the message of an exception from a data-level step (a join, a
mapping, a CSV read) can quote cell values such as a BSN. The helpers write
only the exception type, unless the caller passes ``show_message=True``; that
is meant for file- and metadata-level failures (a layout, a JSON file).
Tracebacks, which do carry the message, are only written with ``debug``.

:func:`setup_logging` attaches the handlers (the CLI calls it once per
command): a console handler for WARNING and up, one readable line per problem,
and a file handler for INFO and up, ``<logs_dir>/(8)_run_log_latest.log``.
Without it, records go wherever the host application sends them.
"""

import json
import logging
import os
import sys
from typing import Any

LOGGER_NAME = "eencijferho"
RUN_LOG_FILENAME = "(8)_run_log_latest.log"

SKIPPED = "skipped"
FAILED = "failed"
UNEXPECTED = "unexpected"

# Name of the LogRecord attribute that carries the key=value part of a line.
_FIELDS_ATTR = "run_log_fields"
# Marks the handlers setup_logging added, so teardown removes only those.
_HANDLER_MARK = "_eencijferho_run_log"


def _quote(value: Any) -> str:
    """A value for a key=value pair; quoted when it holds spaces, quotes or '='."""
    text = str(value)
    if not text or any(c.isspace() or c in '"=' for c in text):
        return json.dumps(text, ensure_ascii=False)
    return text


def _fields(
    outcome: str,
    step: str,
    file: str | None,
    column: str | None,
    extra: list[tuple[str, Any]],
) -> str:
    pairs: list[tuple[str, Any]] = [("outcome", outcome), ("step", step)]
    if file is not None:
        pairs.append(("file", os.path.basename(str(file))))
    if column is not None:
        pairs.append(("column", column))
    pairs.extend((k, v) for k, v in extra if v is not None)
    return " ".join(f"{k}={_quote(v)}" for k, v in pairs)


def _error_text(error: BaseException, show_message: bool) -> str:
    name = type(error).__name__
    message = str(error).strip()
    return f"{name}: {message}" if show_message and message else name


def _emit(
    logger: logging.Logger,
    level: int,
    outcome: str,
    step: str,
    *,
    file: str | None,
    column: str | None,
    detail: str | None,
    extra: list[tuple[str, Any]],
    error: BaseException | None = None,
    show_message: bool = False,
    exc_info: bool = False,
) -> None:
    fields = _fields(outcome, step, file, column, extra)
    if detail:
        text = detail
    else:
        where = ", ".join(
            part for part in (
                os.path.basename(str(file)) if file is not None else None,
                f"kolom {column}" if column is not None else None,
            ) if part
        )
        text = f"[eencijferho] {step}: {outcome}" + (f" ({where})" if where else "")
    if error is not None:
        text += f" ({_error_text(error, show_message)})"
    logger.log(
        level,
        "%s",
        text,
        exc_info=(type(error), error, error.__traceback__) if exc_info and error is not None else None,
        extra={_FIELDS_ATTR: fields},
    )


def log_skipped(
    logger: logging.Logger,
    step: str,
    reason: str,
    *,
    file: str | None = None,
    column: str | None = None,
    error: BaseException | None = None,
    detail: str | None = None,
) -> None:
    """Log a step skipped for a known reason (INFO, ``outcome=skipped``).

    Args:
        step: Short name of the step (``dec_load``, ``value_validation``).
        reason: Reason code, e.g. ``no_mapping`` or ``file_not_found``.
        file: File the step was about; only its base name is written.
        column: Column the step was about, when there is one.
        error: The exception that led to the skip, if any; only its type is written.
        detail: Human-readable line for the console; built from the fields when None.
    """
    extra: list[tuple[str, Any]] = [("reason", reason)]
    if error is not None:
        extra.append(("error", type(error).__name__))
    _emit(
        logger, logging.INFO, SKIPPED, step,
        file=file, column=column, detail=detail, extra=extra, error=error,
    )


def log_failed(
    logger: logging.Logger,
    step: str,
    error: BaseException,
    *,
    file: str | None = None,
    column: str | None = None,
    detail: str | None = None,
    show_message: bool = False,
) -> None:
    """Log a failed operation whose result is degraded or missing (ERROR, ``outcome=failed``).

    Args:
        step: Short name of the step.
        error: The exception; its type is always written.
        file: File the step was about; only its base name is written.
        column: Column the step was about, when there is one.
        detail: Human-readable line for the console; built from the fields when None.
        show_message: Also write the exception message. Only for file- and
            metadata-level failures: a data-level message can quote cell values.
    """
    _emit(
        logger, logging.ERROR, FAILED, step,
        file=file, column=column, detail=detail,
        extra=_error_fields(error, show_message), error=error, show_message=show_message,
        # The traceback is only rendered with --debug; attach it only then.
        exc_info=logger.isEnabledFor(logging.DEBUG),
    )


def log_unexpected(
    logger: logging.Logger,
    step: str,
    error: BaseException,
    *,
    file: str | None = None,
    column: str | None = None,
    detail: str | None = None,
    show_message: bool = False,
) -> None:
    """Log what a catch-all caught (ERROR, ``outcome=unexpected``, with ``exc_info``).

    Arguments as for :func:`log_failed`. The traceback is attached to the
    record; the handlers of :func:`setup_logging` only render it with ``debug``.
    """
    _emit(
        logger, logging.ERROR, UNEXPECTED, step,
        file=file, column=column, detail=detail,
        extra=_error_fields(error, show_message), error=error, show_message=show_message,
        exc_info=True,
    )


def _error_fields(error: BaseException, show_message: bool) -> list[tuple[str, Any]]:
    extra: list[tuple[str, Any]] = [("error", type(error).__name__)]
    message = str(error).strip()
    if show_message and message:
        extra.append(("message", message))
    return extra


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


class _RunLogFormatter(logging.Formatter):
    """Formatter that renders a traceback only when ``show_traceback`` is set."""

    def __init__(self, fmt: str, show_traceback: bool) -> None:
        super().__init__(fmt)
        self.show_traceback = show_traceback

    def format(self, record: logging.LogRecord) -> str:
        if self.show_traceback:
            return super().format(record)
        saved = record.exc_info, record.exc_text, record.stack_info
        record.exc_info = record.exc_text = record.stack_info = None
        try:
            return super().format(record)
        finally:
            record.exc_info, record.exc_text, record.stack_info = saved


class _FileFormatter(_RunLogFormatter):
    """``<time> <LEVEL> key=value ... msg="<readable line>"``."""

    def __init__(self, show_traceback: bool) -> None:
        super().__init__("%(asctime)s %(levelname)s %(run_log_line)s", show_traceback)

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, _FIELDS_ATTR, None)
        message = record.getMessage()
        record.run_log_line = (
            f"{fields} msg={_quote(message)}" if fields else f"{record.name}: {message}"
        )
        return super().format(record)


def _mark(handler: logging.Handler) -> logging.Handler:
    setattr(handler, _HANDLER_MARK, True)
    return handler


def teardown_logging() -> None:
    """Remove (and close) the handlers that :func:`setup_logging` added; reset the level."""
    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, _HANDLER_MARK, False):
            logger.removeHandler(handler)
            handler.close()
    logger.setLevel(logging.NOTSET)


def setup_logging(logs_dir: str | None, debug: bool = False) -> str | None:
    """Send the ``eencijferho`` log to the console and to the run log file.

    Args:
        logs_dir: Folder for ``(8)_run_log_latest.log`` (overwritten per run).
            None: console only (e.g. for a storage backend that is not the
            local disk).
        debug: Console and file get DEBUG and up, with full tracebacks.
            Otherwise the console gets WARNING and up, one line per problem,
            and the file INFO and up; no tracebacks in either.

    Returns:
        The path of the run log file, or None without one.
    """
    teardown_logging()
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if debug else logging.WARNING)
    console.setFormatter(_RunLogFormatter("%(message)s", show_traceback=debug))
    logger.addHandler(_mark(console))

    if logs_dir is None:
        return None
    os.makedirs(logs_dir, exist_ok=True)
    path = os.path.join(logs_dir, RUN_LOG_FILENAME)
    file_handler = logging.FileHandler(path, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    file_handler.setFormatter(_FileFormatter(show_traceback=debug))
    logger.addHandler(_mark(file_handler))
    return path

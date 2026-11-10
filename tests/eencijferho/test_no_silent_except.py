"""Guard: no silent catch-all in core/ and utils/ (#211).

A handler for ``Exception``, ``BaseException`` or a bare ``except:`` must log
(``log_skipped``/``log_failed``/``log_unexpected`` or ``logger.<level>``) or
re-raise, and an ``except Exception`` line names why it is a catch-all.
"""

import ast
from pathlib import Path

import pytest

import eencijferho

PACKAGE = Path(eencijferho.__file__).parent
SOURCES = sorted(
    p for folder in ("core", "utils") for p in (PACKAGE / folder).rglob("*.py")
)

BROAD = {"Exception", "BaseException"}
RUN_LOG_HELPERS = {"log_skipped", "log_failed", "log_unexpected"}
LOGGER_METHODS = {"debug", "info", "warning", "error", "exception", "critical", "log"}


def _names(node: ast.expr | None) -> set[str]:
    if node is None:
        return set()
    if isinstance(node, ast.Tuple):
        return set().union(*(_names(e) for e in node.elts))
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    return set()


def _is_broad(handler: ast.ExceptHandler) -> bool:
    return handler.type is None or bool(_names(handler.type) & BROAD)


def _logs_or_raises(handler: ast.ExceptHandler) -> bool:
    for stmt in handler.body:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Raise):
                return True
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Name) and func.id in RUN_LOG_HELPERS:
                return True
            if isinstance(func, ast.Attribute) and (
                func.attr in RUN_LOG_HELPERS
                or (
                    func.attr in LOGGER_METHODS
                    and isinstance(func.value, ast.Name)
                    and func.value.id in {"logger", "_logger"}
                )
            ):
                return True
    return False


def _broad_handlers(path: Path):
    source = path.read_text(encoding="utf-8")
    lines = source.splitlines()
    for node in ast.walk(ast.parse(source, filename=str(path))):
        if isinstance(node, ast.ExceptHandler) and _is_broad(node):
            yield node, lines[node.lineno - 1]


def test_sources_found():
    names = {p.name for p in SOURCES}
    assert {"decoder.py", "converter.py", "snapshot_validator.py", "run_log.py"} <= names


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_every_catch_all_logs_or_raises(path):
    silent = [
        f"{path.name}:{h.lineno}" for h, _ in _broad_handlers(path) if not _logs_or_raises(h)
    ]
    assert silent == [], f"catch-all without a log call or raise: {silent}"


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_every_catch_all_is_documented(path):
    undocumented = [
        f"{path.name}:{h.lineno}"
        for h, line in _broad_handlers(path)
        if "# catch-all:" not in line
    ]
    assert undocumented == [], f"catch-all without '# catch-all: <reason>': {undocumented}"


def test_no_bare_except():
    bare = [
        f"{path.name}:{h.lineno}"
        for path in SOURCES
        for h, _ in _broad_handlers(path)
        if h.type is None
    ]
    assert bare == []


def test_fewer_catch_alls_than_the_39_measured_on_main():
    total = sum(
        line.count("except Exception")
        for path in SOURCES
        for line in path.read_text(encoding="utf-8").splitlines()
    )
    assert total < 39


def test_guard_flags_a_silent_handler(tmp_path):
    """The guard itself: a silent catch-all is found, a logged one is not."""
    sample = tmp_path / "sample.py"
    sample.write_text(
        "try:\n    pass\nexcept Exception:\n    pass\n"
        "try:\n    pass\nexcept Exception as e:  # catch-all: x\n    log_failed(logger, 's', e)\n"
        "try:\n    pass\nexcept (ValueError, BaseException):\n    x = 1\n",
        encoding="utf-8",
    )
    found = [(h.lineno, _logs_or_raises(h)) for h, _ in _broad_handlers(sample)]
    assert found == [(3, False), (7, True), (11, False)]

"""De versieregel van de docs-site komt uit pyproject, VERSION en CHANGELOG."""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "docs_meta.py"
_spec = importlib.util.spec_from_file_location("docs_meta", _SCRIPT)
docs_meta = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(docs_meta)


def _repo(tmp_path: Path, pyproject: str, version_file: str, changelog: str) -> Path:
    (tmp_path / "pyproject.toml").write_text(f'[project]\nversion = "{pyproject}"\n')
    (tmp_path / "VERSION").write_text(version_file + "\n")
    (tmp_path / "CHANGELOG.md").write_text(changelog)
    return tmp_path


def test_read_version_accepts_v_prefix(tmp_path):
    root = _repo(tmp_path, "0.1.8", "v0.1.8", "")
    assert docs_meta.read_version(root) == "0.1.8"


def test_read_version_rejects_mismatch(tmp_path):
    root = _repo(tmp_path, "0.1.9", "v0.1.8", "")
    with pytest.raises(ValueError, match="wijken af"):
        docs_meta.read_version(root)


def test_unreleased_with_entries():
    text = "# Changelog\n\n## [Unreleased]\n\n### Added\n- iets\n\n## [0.1.8]\n- oud\n"
    assert docs_meta.has_unreleased(text) is True


def test_unreleased_empty_or_missing():
    assert docs_meta.has_unreleased("## [Unreleased]\n\n## [0.1.8]\n- oud\n") is False
    assert docs_meta.has_unreleased("## [0.1.8]\n- oud\n") is False


def test_line_names_version_commit_and_unreleased():
    line = docs_meta.build_line("0.1.8", "abc1234", "2026-11-26", True)
    assert "0.1.8" in line and "abc1234" in line and "2026-11-26" in line
    assert "[Unreleased]" in line
    assert "[Unreleased]" not in docs_meta.build_line("0.1.8", "abc1234", "2026-11-26", False)


def test_main_writes_file(tmp_path, monkeypatch):
    root = _repo(tmp_path, "0.2.0", "v0.2.0", "## [Unreleased]\n- x\n")
    monkeypatch.setenv("GITHUB_SHA", "1234567890abcdef")
    target = docs_meta.main(root)
    assert target == root / ".docs-meta" / "versie.md"
    assert "0.2.0" in target.read_text() and "1234567" in target.read_text()

"""Schrijf de versieregel voor de documentatiesite.

De regel komt uit ``pyproject.toml`` (versie), ``VERSION`` (moet gelijk zijn),
``CHANGELOG.md`` (staat er iets onder ``[Unreleased]``?) en git (commit).
De docs-workflow draait dit script vóór ``zensical build``; de uitvoer,
``.docs-meta/versie.md``, staat niet in git en wordt in de pagina's ingevoegd
met ``--8<-- ".docs-meta/versie.md"``.

Gebruik: ``python3 scripts/docs_meta.py``
"""

from __future__ import annotations

import datetime as dt
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ".docs-meta/versie.md"
CHANGELOG_URL = "https://github.com/cedanl/1cijferho/blob/main/CHANGELOG.md"


def read_version(root: Path) -> str:
    """Versie uit pyproject.toml; breekt af als VERSION afwijkt."""
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    version = project["project"]["version"]
    version_file = (root / "VERSION").read_text(encoding="utf-8").strip().lstrip("v")
    if version_file != version:
        raise ValueError(
            f"VERSION ({version_file}) en pyproject.toml ({version}) wijken af"
        )
    return version


def has_unreleased(changelog: str) -> bool:
    """True als er een opsommingspunt onder ``## [Unreleased]`` staat."""
    match = re.search(
        r"^## \[Unreleased\]\s*$(.*?)(?=^## \[|\Z)", changelog, re.S | re.M
    )
    return bool(match and re.search(r"^\s*[-*] ", match.group(1), re.M))


def build_line(version: str, commit: str, date: str, unreleased: bool) -> str:
    """De regel zelf, als admonition."""
    text = f"Documentatie bij versie **{version}** · bron: commit `{commit}` · gebouwd op {date}."
    if unreleased:
        text += (
            " Wijzigingen onder `[Unreleased]` in de "
            f"[CHANGELOG]({CHANGELOG_URL}) zitten nog niet in een release."
        )
    return f'!!! info "Versie"\n    {text}\n'


def current_commit(root: Path) -> str:
    sha = os.environ.get("GITHUB_SHA")
    if not sha:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True
        ).stdout.strip()
    return sha[:7]


def main(root: Path = ROOT) -> Path:
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    line = build_line(
        read_version(root),
        current_commit(root),
        dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d"),
        has_unreleased(changelog),
    )
    target = root / OUTPUT
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(line, encoding="utf-8")
    return target


if __name__ == "__main__":
    try:
        print(main())
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        print(f"docs_meta: {exc}", file=sys.stderr)
        sys.exit(1)

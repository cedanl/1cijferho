# lighthouse: core@v0.8.1
"""The heading table `.lighthouse/headings.toml` (decision 8).

Every heading that code or a skill reads as a contract has one variant per
language. Writing uses `[project] language` (`heading`); reading accepts every
variant (`variants`, `any_of`). An unknown key is a `KeyError`; a broken or
missing table is a `ConfigError` that names the file.

Path: `LIGHTHOUSE_HEADINGS`, otherwise `<repo root>/.lighthouse/headings.toml`.

CLI, as a file on its path or with `.lighthouse/lib` on `PYTHONPATH`:

    python3 .lighthouse/lib/lighthouse_lib/headings.py <key>
        rc 0 the heading in the project language, rc 2 unknown key or config error

The syntax stays within Python 3.9, like the rest of the lib.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

# Run as a file on its path: put `.lighthouse/lib` first (see `config.py`).
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib.config import LANGUAGES, ConfigError, config_path, load, repo_root


def path() -> Path:
    override = os.environ.get("LIGHTHOUSE_HEADINGS")
    if override:
        return Path(override)
    return repo_root() / ".lighthouse" / "headings.toml"


def _table() -> dict[str, dict[str, str]]:
    try:
        import tomllib
    except ImportError as exc:
        raise ConfigError(
            f"{path()}: python3 >= 3.11 is required (tomllib), this is {sys.version.split()[0]}"
        ) from exc
    where = path()
    try:
        with where.open("rb") as handle:
            data = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"{where}: file is missing") from exc
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"{where}: unreadable or not valid TOML: {exc}") from exc
    for key, variants in data.items():
        if not isinstance(variants, dict):
            raise ConfigError(f"{where}: key {key} must be a table")
        for language in LANGUAGES:
            value = variants.get(language)
            if not isinstance(value, str) or not value:
                raise ConfigError(f"{where}: key {key}.{language} must be a non-empty string")
    return data


def variants(key: str) -> tuple[str, ...]:
    """Every variant of `key`, `nl` first, without duplicates."""
    table = _table()
    if key not in table:
        raise KeyError(key)
    entry = table[key]
    ordered = ["nl", *(language for language in LANGUAGES if language != "nl")]
    ordered += [language for language in entry if language not in ordered]
    out: list[str] = []
    for language in ordered:
        value = entry.get(language)
        if value is not None and value not in out:
            out.append(value)
    return tuple(out)


def heading(key: str, language: str | None = None) -> str:
    """`key` in `language`; `None` means `[project] language` of the repo."""
    table = _table()
    if key not in table:
        raise KeyError(key)
    if language is None:
        language = load(config_path()).language
    return table[key][language]


def any_of(key: str) -> str:
    """A regex group that matches every variant of `key` literally."""
    return "(?:" + "|".join(re.escape(value) for value in variants(key)) + ")"


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="python3 .lighthouse/lib/lighthouse_lib/headings.py")
    parser.add_argument("key", help="a key of headings.toml")
    args = parser.parse_args(argv)
    try:
        text = heading(args.key)
    except KeyError:
        print(f"headings: unknown key {args.key}", file=sys.stderr)
        return 2
    except ConfigError as exc:
        print(f"headings: {exc}", file=sys.stderr)
        return 2
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

# lighthouse: core@v0.8.1
"""The lighthouse config reader: `lighthouse.toml` in the repo, `host.toml` on the host.

One reader for hooks, scripts and skills. It validates loudly: a configuration
error names the file and the key, and the CLI exits with rc 2. `[forge]` is
optional, and so are its keys: an absent `forge.*` key without a default is not
set (rc 1), and the rest works. The board (`[forge.board]`) is optional too; the status option
`Controller` is required only when a `[controller]` table exists. The keys in
`SCHEMA` are type-checked when present, and `DEFAULTS` fills the absent ones. Any
other table passes through unvalidated.

Paths: `lighthouse.toml` in the repo root (`LIGHTHOUSE_REPO_ROOT`, otherwise
`git rev-parse --show-toplevel`), overridden by `LIGHTHOUSE_TOML`;
`~/.config/lighthouse/host.toml`, overridden by `LIGHTHOUSE_HOST_TOML`.

CLI, as a file on its path or with `.lighthouse/lib` on `PYTHONPATH`:

    python3 .lighthouse/lib/lighthouse_lib/config.py <key>
    python3 -m lighthouse_lib.config <key>     rc 0 value, rc 1 not set, rc 2 config error
    python3 -m lighthouse_lib.config host-check rc 0 this host, rc 1 another host, rc 2 error

A key is a dotted path into `lighthouse.toml` (`forge.repo`,
`forge.board.status_options.In progress`), one of the derived keys
`forge.owner` and `forge.name`, or `host.<key>` from `host.toml`. A list value
prints one element per line.

The syntax stays within Python 3.9, like the rest of the lib; reading TOML
needs `tomllib`, so Python 3.11 or newer.
"""

from __future__ import annotations

import errno
import ipaddress
import os
import re
import socket
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

# Run as a file on its path (`python3 .lighthouse/lib/lighthouse_lib/config.py`):
# put `.lighthouse/lib` first, so `lighthouse_lib` imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# The statuses the skills name; the board maps each to an option ID.
REQUIRED_STATUS_OPTIONS = ("To do", "In progress", "Done", "On hold")
# Required only when the repo has a `[controller]` table (decision 16).
CONTROLLER_STATUS_OPTION = "Controller"
LANGUAGES = ("nl", "en")

# The characters GitHub allows in owner and name. A quote or `<` never reaches a
# URL or a regex this way.
_REPO = re.compile(r"[A-Za-z0-9._-]+/[A-Za-z0-9._-]+")

_STR, _INT, _STRS, _INTS = "str", "int", "list[str]", "list[int]"
_BOOL = "bool"

# The keys beyond `[forge]`, `[forge.board]` and `[project] language`, with their
# type. Each one is checked when present.
SCHEMA: Mapping[str, str] = MappingProxyType(
    {
        "forge.default_branch": _STR,
        "paths.specs": _STR,
        "paths.plans": _STR,
        "paths.tenets": _STR,
        "paths.areas": _STR,
        "paths.history": _STR,
        "paths.labels": _STR,
        "paths.search_dirs": _STRS,
        "paths.mirror_exceptions": _STR,
        "labels.blocked_issue": _STR,
        "labels.blocked_external": _STR,
        "labels.needs_shaping": _STR,
        "labels.needs_decision": _STR,
        "labels.needs_hands": _STR,
        "labels.needs_testing": _STR,
        "labels.location_prefix": _STR,
        "labels.area_prefix": _STR,
        "test_gate.command": _STR,
        "test_gate.deadline_seconds": _INT,
        "test_gate.slots": _INT,
        "test_gate.slot_wait_seconds": _INT,
        "test_gate.unchecked_exit_codes": _INTS,
        "test_gate.evidence_test_dir": _STR,
        "ci.runs_on": _STR,
        "ci.bot_client_id_var": _STR,
        "ci.bot_private_key_secret": _STR,
        "ci.bot_login": _STR,
        "codex_usage.halt_pct_session": _INT,
        "codex_usage.vraag_pct_week": _INT,
        "codex_usage.app_server_timeout_seconds": _INT,
        "codex_usage.review_pct_session": _INT,
        "codex_usage.review_pct_week": _INT,
        "worktree_audit.protected_refs": _STRS,
        "worktree_audit.protected_paths": _STRS,
        "project.instructions_word_ceiling": _INT,
        "project.skill_prefix": _STR,
        "project.milestone_prefix": _STR,
        "python.source_dirs": _STRS,
        "python.lint_dirs": _STRS,
        "python.coverage_command": _STR,
        "python.nightly_workflow": _STR,
        "python.coverage_issue_labels": _STRS,
        "shell.strict_dirs": _STRS,
        "project.code_language": _STR,
        "project.code_language_exempt": _STRS,
        "usage_guard.resume": _BOOL,
        "usage_guard.controller": _BOOL,
        "project.code_language_paths": _STRS,
        "code_language_llm.url": _STR,
        "code_language_llm.models": _STRS,
        "code_language_llm.key_env": _STR,
        "code_language_llm.env_file": _STR,
        "review.roborev": _STR,
    }
)

# The value of an absent key. A key in `SCHEMA` without a default is optional: not set.
# Lists are tuples here; `lookup` hands out a fresh list.
DEFAULTS: Mapping[str, object] = MappingProxyType(
    {
        "forge.default_branch": "main",
        "paths.specs": "docs/developer/planning/specs",
        "paths.plans": "docs/developer/planning/plans",
        "paths.search_dirs": (".",),
        "labels.blocked_issue": "blocked-issue",
        "labels.blocked_external": "blocked-external",
        "labels.needs_shaping": "needs-shaping",
        "labels.needs_decision": "needs-decision",
        "labels.needs_hands": "needs-hands",
        "labels.needs_testing": "needs-testing",
        "test_gate.deadline_seconds": 270,
        "test_gate.slots": 2,
        "test_gate.slot_wait_seconds": 180,
        "test_gate.unchecked_exit_codes": (),
        "test_gate.evidence_test_dir": "tests/",
        "ci.runs_on": "ubuntu-latest",
        "codex_usage.halt_pct_session": 85,
        "codex_usage.vraag_pct_week": 80,
        "codex_usage.app_server_timeout_seconds": 20,
        "codex_usage.review_pct_session": 90,
        "codex_usage.review_pct_week": 95,
        "worktree_audit.protected_refs": (),
        "worktree_audit.protected_paths": (),
        "project.instructions_word_ceiling": 3000,
        "python.source_dirs": ("src",),
        "python.lint_dirs": ("src", "tests"),
        "python.coverage_command": "uv run pytest -q",
        "python.coverage_issue_labels": (),
        "shell.strict_dirs": (".claude/hooks",),
        "usage_guard.resume": False,
        "usage_guard.controller": False,
        "review.roborev": "off",
    }
)

# Keys whose empty value is a setting: `[test_gate] command = ""` turns the test gate
# off, and `--upgrade` leaves it so (design choice 4 of M0 (c)).
_MAY_BE_EMPTY = ("test_gate.command",)

_BASE_KEYS = (
    "forge.kind",
    "forge.repo",
    "forge.owner",
    "forge.name",
    "forge.board.project_number",
    "forge.board.project_id",
    "forge.board.status_field_id",
    "forge.board.priority_field_id",
    "project.language",
)
_OPTION_PREFIXES = ("forge.board.status_options.", "forge.board.priority_options.")
HOST_KEYS = ("host.name", "host.tailscale_ip", "host.gate_slice")
# Host keys a host without `host.toml` simply does not have (rc 1, not rc 2).
_OPTIONAL_HOST_KEYS = ("host.gate_slice",)


class ConfigError(Exception):
    """A configuration error; the text always names the file."""


def _empty() -> Mapping[str, str]:
    return MappingProxyType({})


@dataclass(frozen=True)
class Board:
    project_number: int
    project_id: str
    status_field_id: str
    status_options: Mapping[str, str]
    priority_field_id: str | None = None
    priority_options: Mapping[str, str] = field(default_factory=_empty)


@dataclass(frozen=True)
class Config:
    kind: str | None
    repo: str | None
    language: str
    board: Board | None
    raw: Mapping[str, Any]

    @property
    def owner(self) -> str | None:
        return None if self.repo is None else self.repo.split("/", 1)[0]

    @property
    def name(self) -> str | None:
        return None if self.repo is None else self.repo.split("/", 1)[1]


@dataclass(frozen=True)
class Host:
    name: str
    tailscale_ip: str
    gate_slice: str | None = None


def is_known_key(key: str) -> bool:
    """Is `key` a key this reader knows, so a skill may name it?"""
    if key in _BASE_KEYS or key in SCHEMA or key in HOST_KEYS:
        return True
    return any(key.startswith(p) and len(key) > len(p) for p in _OPTION_PREFIXES)


def repo_root() -> Path:
    override = os.environ.get("LIGHTHOUSE_REPO_ROOT")
    if override:
        return Path(override)
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise ConfigError(f"repository root not found: git did not run: {exc}") from exc
    if result.returncode != 0 or not result.stdout.strip():
        raise ConfigError(
            f"repository root not found: {result.stderr.strip() or 'git rev-parse failed'}"
        )
    return Path(result.stdout.strip())


def config_path() -> Path:
    override = os.environ.get("LIGHTHOUSE_TOML")
    if override:
        return Path(override)
    return repo_root() / "lighthouse.toml"


def host_path() -> Path:
    override = os.environ.get("LIGHTHOUSE_HOST_TOML")
    if override:
        return Path(override)
    return Path.home() / ".config/lighthouse/host.toml"


def _read_toml(path: Path, missing: str) -> dict[str, Any]:
    try:
        import tomllib
    except ImportError as exc:
        raise ConfigError(
            f"{path}: python3 >= 3.11 is required (tomllib), this is {sys.version.split()[0]}"
        ) from exc
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(missing) from exc
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"{path}: unreadable or not valid TOML: {exc}") from exc


def _get(table: Mapping[str, Any], key: str, kind: type, where: str, path: Path) -> Any:
    """One required key of the expected type, or a `ConfigError` naming file and key."""
    if key not in table:
        raise ConfigError(f"{path}: missing key {where}{key}")
    value = table[key]
    if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
        raise ConfigError(f"{path}: key {where}{key} must be of type {kind.__name__}")
    if kind is str and not value:
        raise ConfigError(f"{path}: key {where}{key} is empty")
    return value


def _options(table: Mapping[str, Any], where: str, path: Path) -> dict[str, str]:
    for name, option_id in table.items():
        if not isinstance(option_id, str) or not option_id:
            raise ConfigError(f"{path}: option {where}{name} must be a non-empty string")
    return dict(table)


def _board(board: Mapping[str, Any], needs_controller: bool, path: Path) -> Board:
    where = "forge.board."
    status_options = _options(
        _get(board, "status_options", dict, where, path), where + "status_options.", path
    )
    required = REQUIRED_STATUS_OPTIONS + (
        (CONTROLLER_STATUS_OPTION,) if needs_controller else ()
    )
    for name in required:
        if name not in status_options:
            raise ConfigError(
                f"{path}: missing status option {name!r} in forge.board.status_options"
            )
    priority_field_id = None
    if "priority_field_id" in board:
        priority_field_id = _get(board, "priority_field_id", str, where, path)
    priority_table = board.get("priority_options", {})
    if not isinstance(priority_table, dict):
        raise ConfigError(f"{path}: key forge.board.priority_options must be a table")
    return Board(
        project_number=_get(board, "project_number", int, where, path),
        project_id=_get(board, "project_id", str, where, path),
        status_field_id=_get(board, "status_field_id", str, where, path),
        status_options=MappingProxyType(status_options),
        priority_field_id=priority_field_id,
        priority_options=MappingProxyType(
            _options(priority_table, where + "priority_options.", path)
        ),
    )


def _check_type(value: Any, kind: str) -> bool:
    def is_int(v: Any) -> bool:
        return isinstance(v, int) and not isinstance(v, bool)

    if kind == _STR:
        return isinstance(value, str)
    if kind == _INT:
        return is_int(value)
    if kind == _BOOL:
        return isinstance(value, bool)
    if kind == _STRS:
        return isinstance(value, list) and all(isinstance(v, str) and v for v in value)
    return isinstance(value, list) and all(is_int(v) for v in value)


def _check_schema(data: Mapping[str, Any], path: Path) -> None:
    for table in sorted({key.split(".", 1)[0] for key in SCHEMA}):
        if table in data and not isinstance(data[table], dict):
            raise ConfigError(f"{path}: key {table} must be a table")
    for key, kind in SCHEMA.items():
        table, name = key.split(".", 1)
        if name not in data.get(table, {}):
            continue
        value = data[table][name]
        if not _check_type(value, kind):
            raise ConfigError(f"{path}: key {key} must be of type {kind}")
        if kind == _STR and not value and key not in _MAY_BE_EMPTY:
            raise ConfigError(f"{path}: key {key} is empty")


def load(path: Path) -> Config:
    """Read and validate one `lighthouse.toml`."""
    data = _read_toml(path, f"{path}: file is missing")

    kind = repo = None
    board = None
    if "forge" in data:
        forge = _get(data, "forge", dict, "", path)
        # `kind` and `repo` are optional too: a repo without a GitHub remote may
        # hold only `default_branch` here.
        if "kind" in forge:
            kind = _get(forge, "kind", str, "forge.", path)
            if kind != "github":
                raise ConfigError(f"{path}: key forge.kind must be 'github', not {kind!r}")
        if "repo" in forge:
            repo = _get(forge, "repo", str, "forge.", path)
            if not _REPO.fullmatch(repo):
                raise ConfigError(
                    f"{path}: key forge.repo must have the form owner/name: {repo!r}"
                )
        if "board" in forge:
            board = _board(
                _get(forge, "board", dict, "forge.", path), "controller" in data, path
            )

    language = "en"
    if "project" in data:
        project = _get(data, "project", dict, "", path)
        if "language" in project:
            language = project["language"]
            if language not in LANGUAGES:
                raise ConfigError(
                    f"{path}: key project.language must be one of "
                    f"{', '.join(LANGUAGES)}, not {language!r}"
                )
        if "code_language" in project and project["code_language"] != "en":
            value = project["code_language"]
            raise ConfigError(
                f"{path}: key project.code_language must be en, not {value!r}"
            )

    _check_schema(data, path)

    roborev = data.get("review", {}).get("roborev")
    if roborev is not None and roborev not in ("local", "off"):
        raise ConfigError(f"{path}: key review.roborev must be 'local' or 'off', not {roborev!r}")

    return Config(
        kind=kind,
        repo=repo,
        language=language,
        board=board,
        raw=MappingProxyType(data),
    )


def load_host(path: Path) -> Host:
    """Read and validate one `host.toml`."""
    data = _read_toml(path, f"host unknown: no host.toml ({path})")

    if "host" not in data:
        raise ConfigError(f"{path}: missing key host")
    host = data["host"]
    if not isinstance(host, dict):
        raise ConfigError(f"{path}: key host must be a table")
    if "name" not in host:
        raise ConfigError(f"{path}: missing key host.name")
    name = host["name"]
    if not isinstance(name, str) or not name:
        raise ConfigError(f"{path}: key host.name must be a non-empty string")
    if "tailscale_ip" not in host:
        raise ConfigError(f"{path}: missing key host.tailscale_ip")
    tailscale_ip = host["tailscale_ip"]
    try:
        if not isinstance(tailscale_ip, str):
            raise TypeError("not a string")
        address = ipaddress.IPv4Address(tailscale_ip)
        if address.is_unspecified or address.is_loopback or address.is_multicast:
            raise ValueError("not a host address")
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            f"{path}: key host.tailscale_ip must be an IPv4 address: {tailscale_ip!r}"
        ) from exc
    gate_slice = host.get("gate_slice")
    if gate_slice is not None and (not isinstance(gate_slice, str) or not gate_slice):
        raise ConfigError(f"{path}: key host.gate_slice must be a non-empty string")
    return Host(name=name, tailscale_ip=tailscale_ip, gate_slice=gate_slice)


def is_local(ip: str) -> bool:
    """Is `ip` on an interface of this machine? A `bind` measures that; a connect
    or ping says nothing about locality. With `net.ipv4.ip_nonlocal_bind=1` every
    bind succeeds, and every machine recognises itself as the host."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind((ip, 0))
    except OSError as exc:
        if exc.errno == errno.EADDRNOTAVAIL:
            return False
        raise ConfigError(f"local address {ip} not measurable: {exc}") from exc
    return True


def _walk(table: Mapping[str, Any], key: str) -> Any:
    """Walk a dotted key; per level an exact key wins, otherwise the key `k` for
    which the rest starts with `k + "."`. Option names may hold spaces or dots."""
    if key in table:
        return table[key]
    for name, value in table.items():
        if key.startswith(name + ".") and isinstance(value, Mapping):
            return _walk(value, key[len(name) + 1 :])
    raise KeyError(key)


def lookup(cfg: Config, key: str) -> object:
    """The value of `key`, or its default; `KeyError` when it is not set."""
    if key in ("forge.owner", "forge.name"):
        value = cfg.owner if key == "forge.owner" else cfg.name
        if value is None:
            raise KeyError(key)
        return value
    if key == "project.language":
        return cfg.language
    if key.startswith("host."):
        return _host_value(key)
    try:
        return _walk(cfg.raw, key)
    except KeyError:
        if key not in DEFAULTS:
            raise
        default = DEFAULTS[key]
        return list(default) if isinstance(default, tuple) else default


def _host_value(key: str) -> str:
    if key not in HOST_KEYS:
        raise KeyError(key)
    path = host_path()
    if key in _OPTIONAL_HOST_KEYS and not path.exists():
        raise KeyError(key)
    host = load_host(path)
    values = {
        "host.name": host.name,
        "host.tailscale_ip": host.tailscale_ip,
        "host.gate_slice": host.gate_slice,
    }
    value = values[key]
    if value is None:
        raise KeyError(key)
    return value


def _scalar(key: str, value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    raise ConfigError(f"{key} is a table, not a value")


def _format(key: str, value: object) -> list[str]:
    """The output lines: one for a value, one per element for a list."""
    if isinstance(value, list):
        return [_scalar(key, v) for v in value]
    return [_scalar(key, value)]


def _host_check() -> int:
    path = host_path()
    try:
        host = load_host(path)
        local = is_local(host.tailscale_ip)
    except ConfigError as exc:
        print(f"config: {exc}", file=sys.stderr)
        return 2
    if not local:
        print(
            f"config: host {host.name} ({path}) has tailscale_ip {host.tailscale_ip}, "
            "but that address is not on this machine",
            file=sys.stderr,
        )
        return 1
    print(f"host: {host.name}")
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="python3 -m lighthouse_lib.config")
    parser.add_argument("key", help="a dotted key, or host-check")
    args = parser.parse_args(argv)
    if args.key == "host-check":
        return _host_check()
    try:
        if args.key.startswith("host."):
            value: object = _host_value(args.key)
        else:
            value = lookup(load(config_path()), args.key)
        lines = _format(args.key, value)
    except KeyError:
        print(f"config: {args.key} is not set", file=sys.stderr)
        return 1
    except ConfigError as exc:
        print(f"config: {exc}", file=sys.stderr)
        return 2
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    # Under `-m` or as a file this is `__main__`, while other modules import it as
    # `lighthouse_lib.config`, with a ConfigError class of its own. Delegate.
    import importlib

    sys.exit(importlib.import_module("lighthouse_lib.config").main())

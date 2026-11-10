# lighthouse: core@v0.8.1
"""Layer 2 of the code-language detector (#37): ask a chain of models about doubtful units.

`ask` tries the models in order and the first readable answer counts. Every call runs in a
daemon thread behind a wall-clock deadline (`per_model` seconds each, `total` for the chain),
because a socket timeout alone does not stop a server that drips bytes. After a failed chain
a marker file keeps the hook off the endpoint for `QUIET_S` seconds. Standard library only.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import NamedTuple

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib` imports from
# this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import config

PER_MODEL_S = 4.0
TOTAL_S = 8.0
QUIET_S = 60.0
MAX_TOKENS_FLOOR = 512


class Chain(NamedTuple):
    url: str
    models: tuple[str, ...]
    key_env: str | None
    env_file: Path | None


class Reply(NamedTuple):
    not_english: list[bool] | None  # per item; None when the chain failed
    model: str | None
    failures: list[str]  # "<model>: <reason>", "key: <reason>" or "skipped: <reason>"


def chain_from(cfg: config.Config) -> Chain | None:
    """The `[code_language_llm]` table, or None when it is absent. A table that is there but
    wrong raises `ConfigError`."""
    if "code_language_llm" not in cfg.raw:
        return None

    def get(key: str) -> object:
        try:
            return config.lookup(cfg, f"code_language_llm.{key}")
        except KeyError:
            return None

    url, models, key_env, env_file = get("url"), get("models"), get("key_env"), get("env_file")
    if not url:
        raise config.ConfigError("key code_language_llm.url is missing")
    if not models:
        raise config.ConfigError("key code_language_llm.models is missing or empty")
    path = None
    if env_file:
        path = Path(str(env_file)).expanduser()
        if not path.is_absolute():
            raise config.ConfigError(f"code_language_llm.env_file is relative: {env_file}")
        if not path.is_file():
            raise config.ConfigError(f"code_language_llm.env_file does not exist: {path}")
    return Chain(str(url), tuple(models), str(key_env) if key_env else None, path)  # type: ignore[arg-type]


def build_prompt(items: list[tuple[str, str]]) -> str:
    lines = [f"{i}. {kind}: {' '.join(text.split())}" for i, (kind, text) in enumerate(items, 1)]
    return (
        "For each numbered item (a code identifier, a file name or a comment), decide whether "
        "it is English. Abbreviations and code jargon (getint, asctime, gids, val) count as "
        "English. Any word of another language, such as Dutch, makes the item NOT-EN. "
        'Reply with JSON only, for example {"1": "EN", "2": "NOT-EN"}.\n' + "\n".join(lines)
    )


def parse_answer(content: str, n: int) -> list[bool]:
    """True per item that is not English. `ValueError` when the answer cannot be read."""
    match = re.search(r"\{.*\}", content or "", re.S)
    if not match:
        raise ValueError("no JSON object in the answer")
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise ValueError(str(exc)) from exc
    if not isinstance(data, dict):
        raise ValueError("the answer is not an object")
    out = []
    for i in range(1, n + 1):
        verdict = data.get(str(i))
        if verdict not in ("EN", "NOT-EN"):
            raise ValueError(f"item {i} has no EN or NOT-EN")
        out.append(verdict == "NOT-EN")
    return out


def default_state_dir() -> Path:
    """Per user: the uid is in the name, because `/tmp` is shared."""
    base = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    return Path(base) / f"lighthouse-code-language-{os.getuid()}"


def _marker(state_dir: Path, url: str) -> Path:
    return state_dir / f"layer2-failed-{hashlib.sha1(url.encode()).hexdigest()[:12]}"


def _private(state_dir: Path) -> bool:
    """True when `state_dir` is a real directory of this user that nobody else can write."""
    try:
        info = os.lstat(state_dir)
    except OSError:
        return False
    return (
        stat.S_ISDIR(info.st_mode)
        and info.st_uid == os.getuid()
        and not info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    )


def _quiet_for(state_dir: Path, url: str) -> float | None:
    """Seconds since the chain failed, when that is within `QUIET_S`; else None."""
    if not _private(state_dir):
        return None
    try:
        fd = os.open(_marker(state_dir, url), os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd) as marker:
            age = time.time() - float(marker.read().strip())
    except (OSError, ValueError):
        return None
    return age if 0 <= age < QUIET_S else None


def _mark_failed(state_dir: Path, url: str) -> None:
    """Never follows a symlink and never writes into a directory others can write."""
    try:
        state_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            state_dir.mkdir(mode=0o700)
        except FileExistsError:
            pass
        if not _private(state_dir):
            return
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW
        fd = os.open(_marker(state_dir, url), flags, 0o600)
        with os.fdopen(fd, "w") as marker:
            marker.write(str(time.time()))
    except (OSError, ValueError):
        pass


def _clear_mark(state_dir: Path, url: str) -> None:
    try:
        _marker(state_dir, url).unlink()
    except (OSError, ValueError):
        pass


def _key(chain: Chain) -> str | None:
    """The key from the environment, else from `env_file`; None when there is none."""
    name = chain.key_env
    if name is None:
        return None
    if os.environ.get(name):
        return os.environ[name]
    if chain.env_file is not None:
        try:
            lines = chain.env_file.read_text().splitlines()
        except (OSError, UnicodeDecodeError):
            return None
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            left, value = line.split("=", 1)
            if left.strip() == name:
                return value.strip().strip("\"'") or None
    return None


def _call(chain: Chain, model: str, prompt: str, n: int, key: str | None, timeout: float, out: dict):
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        # A floor for reasoning models, which spend tokens before the answer; the wall
        # clock, not this number, bounds the cost.
        "max_tokens": max(MAX_TOKENS_FLOOR, 20 + 12 * n),
    }
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    request = urllib.request.Request(
        chain.url.rstrip("/") + "/chat/completions", json.dumps(body).encode(), headers
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
        out["answer"] = parse_answer(json.loads(raw)["choices"][0]["message"]["content"], n)
    except urllib.error.HTTPError as exc:
        out["error"] = f"HTTP {exc.code}"
    except urllib.error.URLError as exc:
        out["error"] = str(exc.reason)
    except OSError as exc:
        out["error"] = str(exc) or type(exc).__name__
    except (ValueError, KeyError, IndexError, TypeError):
        out["error"] = "unreadable answer"
    except Exception as exc:  # a thread must always end with a reason
        out["error"] = type(exc).__name__


def ask(
    chain: Chain,
    items: list[tuple[str, str]],
    *,
    state_dir: Path,
    per_model: float = PER_MODEL_S,
    total: float = TOTAL_S,
) -> Reply:
    age = _quiet_for(state_dir, chain.url)
    if age is not None:
        return Reply(None, None, [f"skipped: the chain failed {age:.0f} s ago"])
    key = _key(chain)
    if chain.key_env is not None and key is None:
        return Reply(None, None, [f"key: {chain.key_env} is set in neither the environment nor env_file"])
    prompt = build_prompt(items)
    deadline = time.monotonic() + total
    failures: list[str] = []
    for model in chain.models:
        left = deadline - time.monotonic()
        if left <= 0:
            failures.append(f"{model}: total time of {total:g} s is up")
            continue
        wait = min(per_model, left)
        out: dict = {}
        thread = threading.Thread(
            target=_call, args=(chain, model, prompt, len(items), key, wait, out), daemon=True
        )
        thread.start()
        thread.join(wait)
        if thread.is_alive():
            failures.append(f"{model}: no answer within {wait:.1f} s")
        elif "answer" in out:
            _clear_mark(state_dir, chain.url)
            return Reply(out["answer"], model, failures)
        else:
            failures.append(f"{model}: {out.get('error', 'unreadable answer')}")
    _mark_failed(state_dir, chain.url)
    return Reply(None, None, failures)

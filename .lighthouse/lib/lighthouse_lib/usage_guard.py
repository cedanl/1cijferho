# lighthouse: core@v0.8.1
"""The usage-gate decider: who speaks, once per window, when the usage alarm is up.

The hook `usage-gate.sh` checks the alarm file and the pause flag in shell, and
calls this lib only when one of them is valid. The lib reads the hook payload into
a `Context`, builds the paths and keys of the contract, claims the "once per
window" key, and picks the speaker: the base reasons here, the subagent reason, or
a module next to the hook (`usage-gate/resume.py`, `usage-gate/controller.py`)
when its option in `[usage_guard]` is on. The guard decides whether to speak; a
module supplies what is said and written. At most one JSON object reaches stdout,
and the exit code is always 0: an error never blocks.

A module has `speak(ctx: Context, stand: str) -> dict`; `controller` also has
`pause(ctx: Context) -> dict`. All keys are written here, never by a module.

CLI, as a file on its path, with the hook payload on stdin:

    python3 .lighthouse/lib/lighthouse_lib/usage_guard.py <stand> --hooks <dir> \\
        [--alarm KIND PCT LOCAL AT] [--pause UNTIL FLAG]

The syntax stays within Python 3.9; reading the options needs `tomllib`.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Callable, Optional

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib import config

# Payload ids end up in file names; anything else counts as absent.
SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")
# `LIGHTHOUSE_CONTROLLER_RUN` counts only in the shape of a uuid.
RUN_ID = re.compile(r"[0-9a-f-]+")
KIND_LABEL = {"venster": "session window", "week": "weekly window"}

STANDS = ("prompt", "tool", "subagent", "stop")
KEY_MAX_AGE = 14 * 86400


@dataclass(frozen=True)
class Alarm:
    kind: str
    pct: int
    reset_local: str  # with spaces, not underscores
    reset_at: str


@dataclass(frozen=True)
class Context:
    stand: str  # the mode the hook was called in: prompt | tool | subagent | stop
    alarm: Optional[Alarm]  # None without a valid alarm
    session_id: str  # "" when absent or not SAFE_ID
    agent_id: str  # same
    cwd: str  # payload cwd, else os.getcwd()
    run_id: str  # "" outside a controller run
    pause_until: str  # "" without a valid pause flag
    pause_flag: str  # path of the flag, "" without
    stop_hook_active: bool  # True as well for an unreadable or non-object payload
    payload_ok: bool
    now: datetime  # aware, UTC


@dataclass(frozen=True)
class Options:
    resume: bool
    controller: bool


@dataclass(frozen=True)
class Decision:
    output: Optional[dict]  # the one JSON object, or None
    key_after: Optional[Path]  # stop: the key main writes after printing


SILENT = Decision(None, None)


def _warn(message: str) -> None:
    """One stderr line, also for a multi-line error (git's stderr, say)."""
    print("usage-gate: " + " ".join(message.splitlines()), file=sys.stderr)


def _safe(value: object) -> str:
    return value if isinstance(value, str) and SAFE_ID.fullmatch(value) else ""


def build_context(
    stand: str,
    alarm: Optional[Alarm],
    pause_until: str,
    pause_flag: str,
    payload_text: str,
    now: datetime,
) -> Context:
    try:
        payload = json.loads(payload_text)
    except ValueError:
        payload = None
    ok = isinstance(payload, dict)
    data = payload if ok else {}
    cwd = data.get("cwd")
    run = os.environ.get("LIGHTHOUSE_CONTROLLER_RUN", "")
    return Context(
        stand=stand,
        alarm=alarm,
        session_id=_safe(data.get("session_id")),
        agent_id=_safe(data.get("agent_id")),
        cwd=cwd if isinstance(cwd, str) and cwd else os.getcwd(),
        run_id=run if RUN_ID.fullmatch(run) else "",
        pause_until=pause_until,
        pause_flag=pause_flag,
        # An unreadable payload must not block at Stop: it would loop.
        stop_hook_active=data.get("stop_hook_active") is True if ok else True,
        payload_ok=ok,
        now=now,
    )


def _xdg(name: str, fallback: str) -> Path:
    value = os.environ.get(name)
    return Path(value) if value else Path.home() / fallback


def state_dir() -> Path:
    return _xdg("XDG_STATE_HOME", ".local/state") / "lighthouse" / "usage-guard"


def resume_dir() -> Path:
    return _xdg("XDG_DATA_HOME", ".local/share") / "lighthouse" / "resume"


def handoff_path(ctx: Context) -> Path:
    name = ctx.session_id or f"no-session-{int(ctx.now.timestamp())}"
    return resume_dir() / f"{name}.handoff.md"


def effective_stand(ctx: Context) -> str:
    """A tool call from inside a subagent is handled as stand `subagent`."""
    if ctx.stand == "tool" and ctx.agent_id:
        return "subagent"
    return ctx.stand


def alarm_key(ctx: Context, stand: str) -> Path:
    """`<kind>-<reset_at>.<stand>[.<session_id>][.<run_id>|.<agent_id>].done`."""
    assert ctx.alarm is not None
    parts = [f"{ctx.alarm.kind}-{ctx.alarm.reset_at}", stand]
    if ctx.session_id:
        parts.append(ctx.session_id)
    if stand == "subagent" and ctx.agent_id:
        parts.append(ctx.agent_id)
    elif ctx.run_id:
        parts.append(ctx.run_id)
    return state_dir() / (".".join(parts) + ".done")


def pause_key(ctx: Context) -> Path:
    return state_dir() / f"pause-{ctx.pause_until}.{ctx.run_id}.done"


def marker(ctx: Context, *, session_id: str, window: str, reset_at: str, source: str) -> dict:
    """The six fields every resume marker has; a module adds the rest."""
    return {
        "session_id": session_id,
        "cwd": ctx.cwd,
        "window": window,
        "reset_at": reset_at,
        "created_at": ctx.now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
    }


def write_marker(fields: dict) -> Path:
    """Write `<resume>/<session_id>.json` atomically: a tmp file in that dir, then rename."""
    directory = resume_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{fields['session_id']}.json"
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".marker-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(fields, fh, indent=2)
            fh.write("\n")
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path


def block(reason: str) -> dict:
    return {"decision": "block", "reason": reason}


def prompt_note(text: str) -> dict:
    return {
        "hookSpecificOutput": {"hookEventName": "UserPromptSubmit", "additionalContext": text}
    }


def headline(ctx: Context) -> str:
    alarm = ctx.alarm
    assert alarm is not None
    label = KIND_LABEL.get(alarm.kind, alarm.kind)
    return f"Usage is at {alarm.pct}% of the {label}, which resets at {alarm.reset_local}."


def base_speak(ctx: Context, stand: str, *, wake: str = "", note: str = "") -> dict:
    """The base reasons for `prompt`, `tool` and `stop`.

    A module adds to them without restating them: `wake` is a lowercase clause for a
    step before the handoff ("schedule ..."), `note` a sentence after the handoff step.
    """
    handoff = f"`/lighthouse-write-handoff {handoff_path(ctx)}`"
    head = headline(ctx)
    tail = f" {note}" if note else ""
    if stand == "prompt":
        step = f"{wake}, " if wake else ""
        return prompt_note(
            f"{head} Wrap up: finish only the current step, commit the work in progress, "
            f"{step}run {handoff}, and do not start a new step.{tail}"
        )
    if stand == "stop":
        step = f"{wake[0].upper()}{wake[1:]}. " if wake else ""
        return block(
            f"{head} Commit nothing more. {step}Run {handoff} and note in it what is "
            f"uncommitted and when to resume (after {ctx.alarm.reset_local}).{tail} Then stop."
        )
    step = f"{wake}, " if wake else ""
    return block(
        f"{head} Before anything else: commit the work in progress, {step}run {handoff}, "
        "and report where the work stands (worktree, branch, what is still open)."
        f"{tail} Do not start a new step and do not dispatch a subagent."
    )


def subagent_speak(ctx: Context) -> dict:
    return block(
        f"{headline(ctx)} Stop now and report to the main thread what you did and what "
        "is still open. Do not commit, do not schedule a wake-up and do not write a "
        "handoff: the main thread wraps up the session."
    )


def read_options() -> Options:
    """`[usage_guard]` from `lighthouse.toml`; a config error counts as both off."""
    try:
        cfg = config.load(config.config_path())
        return Options(
            resume=bool(config.lookup(cfg, "usage_guard.resume")),
            controller=bool(config.lookup(cfg, "usage_guard.controller")),
        )
    except config.ConfigError as exc:
        _warn(f"config: {exc}")
        return Options(False, False)


def load_module(hooks_dir: Path, name: str) -> ModuleType:
    """Load `<hooks_dir>/usage-gate/<name>.py`."""
    path = hooks_dir / "usage-gate" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"usage_guard_{name}", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered before it runs: `dataclasses` looks the module up in `sys.modules`.
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(spec.name, None)
        raise
    return module


def _checked(output: object) -> dict:
    """The one JSON object a speaker returns; anything else counts as a failure."""
    if not isinstance(output, dict):
        raise TypeError(f"speaker returned {type(output).__name__}, not a dict")
    json.dumps(output)  # what main prints; a TypeError here counts as a failure
    return output


def _pause_due(ctx: Context) -> bool:
    """The pause key is missing, or older than the flag."""
    try:
        key = pause_key(ctx).stat().st_mtime_ns
    except OSError:
        return True
    try:
        flag = os.stat(ctx.pause_flag).st_mtime_ns
    except OSError:
        return False
    return key < flag


def _prune_keys() -> None:
    cutoff = time.time() - KEY_MAX_AGE
    try:
        for path in state_dir().glob("*.done"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
            except OSError:
                pass
    except OSError:
        pass


def decide(ctx: Context, hooks_dir: Path, options: Callable[[], Options]) -> Decision:
    cached = []

    def opts() -> Options:
        if not cached:
            cached.append(options())
        return cached[0]

    # 1. The pause of a controller run: only in `tool`, also without an alarm.
    if (
        ctx.stand == "tool"
        and ctx.run_id
        and ctx.pause_until
        and not ctx.agent_id
        and _pause_due(ctx)
        and opts().controller
    ):
        try:
            state_dir().mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _warn(f"pause: cannot write keys in {state_dir()}: {exc}")
        else:
            try:
                output = _checked(load_module(hooks_dir, "controller").pause(ctx))
                with open(pause_key(ctx), "w"):
                    pass
                return Decision(output, None)
            except Exception as exc:
                _warn(f"pause: controller module failed: {exc!r}")

    # 2. The alarm.
    if ctx.alarm is None:
        return SILENT

    # 3. Stop speaks once per window, and never within a stop hook.
    stand = effective_stand(ctx)
    if stand == "stop":
        if ctx.stop_hook_active or alarm_key(ctx, "stop").exists():
            return SILENT
        # Without a key dir the stop key never lands: stop would block every turn.
        try:
            state_dir().mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _warn(f"cannot write keys in {state_dir()}: {exc}")
            return SILENT

    # 4. Claim the key before speaking; a parallel call of the same session spoke.
    claimed: Optional[Path] = None
    if stand in ("tool", "subagent"):
        key = alarm_key(ctx, stand)
        try:
            state_dir().mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _warn(f"cannot write keys in {state_dir()}: {exc}")
            return SILENT
        try:
            fd = os.open(key, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return SILENT
        except OSError as exc:
            _warn(f"cannot claim {key}: {exc}")
            return SILENT
        os.close(fd)
        claimed = key

    # 5. The speaker. 6. A failing speaker releases the claim.
    try:
        if stand == "subagent":
            output = subagent_speak(ctx)
        elif ctx.run_id and opts().controller:
            output = load_module(hooks_dir, "controller").speak(ctx, stand)
        elif opts().resume:
            output = load_module(hooks_dir, "resume").speak(ctx, stand)
        else:
            output = base_speak(ctx, stand)
        output = _checked(output)
    except Exception as exc:
        if claimed is not None:
            try:
                claimed.unlink(missing_ok=True)
            except OSError:
                pass
        _warn(f"{stand}: speaker failed: {exc!r}")
        return SILENT

    # 7. Stop writes its key only after the block is out (main does that).
    if stand == "stop":
        _prune_keys()
        return Decision(output, alarm_key(ctx, "stop"))
    return Decision(output, None)


class _Parser(argparse.ArgumentParser):
    def error(self, message: str):  # one stderr line, never rc 2
        raise ValueError(message)


def _parse(argv: Optional[list]) -> argparse.Namespace:
    parser = _Parser(prog="usage_guard.py", add_help=False)
    parser.add_argument("stand", choices=STANDS)
    parser.add_argument("--hooks", required=True, type=Path)
    parser.add_argument("--alarm", nargs=4, metavar=("KIND", "PCT", "LOCAL", "AT"))
    parser.add_argument("--pause", nargs=2, metavar=("UNTIL", "FLAG"))
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    try:
        args = _parse(argv)
        alarm = None
        if args.alarm:
            kind, pct, local, at = args.alarm
            alarm = Alarm(kind, int(pct), local.replace("_", " "), at)
        until, flag = args.pause or ("", "")
        ctx = build_context(
            args.stand, alarm, until, flag, sys.stdin.read(), datetime.now(timezone.utc)
        )
        decision = decide(ctx, args.hooks, read_options)
        if decision.output is not None:
            print(json.dumps(decision.output), flush=True)
        if decision.key_after is not None:
            try:
                decision.key_after.parent.mkdir(parents=True, exist_ok=True)
                with open(decision.key_after, "w"):
                    pass
            except OSError as exc:
                _warn(f"cannot write {decision.key_after}: {exc}")
    except Exception as exc:
        _warn(f"{exc!r}")
    return 0


if __name__ == "__main__":
    # Under `-m` or as a file this is `__main__`, while the modules import it as
    # `lighthouse_lib.usage_guard`, with a Context class of its own. Delegate.
    import importlib

    sys.exit(importlib.import_module("lighthouse_lib.usage_guard").main())

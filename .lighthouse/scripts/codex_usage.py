#!/usr/bin/env python3
# lighthouse: core@v0.8.1
"""Read the usage window of the Codex subscription.

WHY
---
Spec and plan reviews run on Codex and share the ChatGPT quota with other work. With
Codex you only notice the limit when a run fails; `budget` therefore decides before a
review round whether Codex still fits.

WHY THE APP-SERVER
------------------
The session log (`<CODEX_HOME>/sessions/**/rollout-*.jsonl`) is correct, but as old as the
last `codex exec` in that home. `codex app-server` (JSON-RPC over stdio) has a method
`account/rateLimits/read` that gives the fresh, account-wide reading in ~0.7 s and starts NO
model turn, so it costs no quota. The answer also contains the manual reset credits.

The protocol is called `[experimental]`; a Codex update can change it. Every error therefore
gives `NotMeasurable`, never a reading of 0%.

NEVER `account/rateLimitResetCredit/consume`: spending a reset is irreversible and stays
an action of the user.

BUDGET FOR THE REVIEWS
----------------------
`budget` decides before a spec or plan review round: Codex or the fallback (exit 0 or 6, one
JSON object on stdout). It uses its own thresholds (`[codex_usage] review_pct_session`,
`review_pct_week` in `lighthouse.toml`) and never writes a marker and never asks a question:
a review only wants to know whether Codex still fits.

Only `read` and `budget` are in this package (decision 26). The gate, which parks a run,
belongs to the usage gate and is not in here.

Output keys and values are fixed English tokens; a skill or `review.sh` parses them.
"""

from __future__ import annotations

import argparse
import json
import os
import selectors
import subprocess
import sys
import time
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

# Run as a file (`python3 .lighthouse/scripts/codex_usage.py`): put `.lighthouse/lib`
# first, so `lighthouse_lib` imports from this installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib import config  # noqa: E402
from lighthouse_lib.config import ConfigError  # noqa: E402

CLIENT_INFO = {"name": "lighthouse-codex-usage", "version": "1"}


class NotMeasurable(Exception):
    """The reading cannot be read; the message is the reason."""


def codex_homes() -> list[Path]:
    """The two `CODEX_HOME`s: the default and the one of the review skills."""
    home = Path.home()
    return [home / ".codex", home / ".codex-specreview"]


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _send(proc: subprocess.Popen[bytes], message: dict) -> None:
    assert proc.stdin is not None
    try:
        proc.stdin.write((json.dumps(message) + "\n").encode())
        proc.stdin.flush()
    except OSError as exc:
        raise NotMeasurable(f"app-server not writable: {exc}") from exc


def _wait_for_answer(
    proc: subprocess.Popen[bytes],
    sel: selectors.BaseSelector,
    buf: bytearray,
    msg_id: int,
    deadline: float,
) -> dict:
    """Read lines until the answer with `msg_id`; noise is skipped.

    Reads with `os.read` on the fd and a line buffer of its own: a buffered `readline` can
    swallow several lines, after which `select` sees nothing any more.
    """
    assert proc.stdout is not None
    fd = proc.stdout.fileno()
    while True:
        while (end := buf.find(b"\n")) >= 0:
            line = bytes(buf[:end])
            del buf[: end + 1]
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if not isinstance(message, dict) or message.get("id") != msg_id:
                continue
            if "error" in message:
                raise NotMeasurable(f"app-server returned an error: {message['error']}")
            result = message.get("result")
            if not isinstance(result, dict):
                raise NotMeasurable("app-server returned an answer without result")
            return result
        rest = deadline - time.monotonic()
        if rest <= 0:
            raise NotMeasurable("app-server gave no answer within the timeout")
        if not sel.select(timeout=rest):
            continue
        chunk = os.read(fd, 65536)
        if not chunk:
            raise NotMeasurable("app-server stopped without an answer")
        buf.extend(chunk)


def read_app_server(home: Path, *, timeout: float) -> dict:
    """The raw `result` of `account/rateLimits/read` for one `CODEX_HOME`."""
    deadline = time.monotonic() + timeout
    try:
        proc = subprocess.Popen(
            ["codex", "app-server"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "CODEX_HOME": str(home)},
        )
    except FileNotFoundError as exc:
        raise NotMeasurable("codex not found") from exc
    except OSError as exc:
        raise NotMeasurable(f"codex could not start: {exc}") from exc

    try:
        assert proc.stdout is not None
        with selectors.DefaultSelector() as sel:
            sel.register(proc.stdout, selectors.EVENT_READ)
            buf = bytearray()
            _send(
                proc,
                {
                    "id": 1,
                    "method": "initialize",
                    "params": {"clientInfo": CLIENT_INFO},
                },
            )
            _wait_for_answer(proc, sel, buf, 1, deadline)
            _send(proc, {"method": "initialized"})
            _send(
                proc,
                {
                    "id": 2,
                    "method": "account/rateLimits/read",
                    "params": {"excludeResetCreditDetails": False},
                },
            )
            return _wait_for_answer(proc, sel, buf, 2, deadline)
    finally:
        if proc.stdin is not None:
            try:
                proc.stdin.close()
            except OSError:
                pass  # the app-server is already gone; the pipe is then already broken
        proc.terminate()
        try:
            proc.wait(2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        if proc.stdout is not None:
            proc.stdout.close()  # after the wait: no reader any more, do not leave the fd to the GC


def _window(raw: object, name: str) -> dict:
    if not isinstance(raw, dict):
        raise NotMeasurable(f"window {name} missing from the app-server answer")
    try:
        return {
            "pct": int(raw["usedPercent"]),
            "reset_at": fmt_ts(datetime.fromtimestamp(raw["resetsAt"], UTC)),
        }
    except (KeyError, TypeError, ValueError, OverflowError, OSError) as exc:
        raise NotMeasurable(f"window {name} unreadable: {exc!r}") from exc


def _reset_credits(raw: object) -> dict | None:
    if not isinstance(raw, dict):
        return None
    try:
        expires = sorted(
            c["expiresAt"]
            for c in raw.get("credits", [])
            if c.get("status") == "available"
        )
        return {
            "available": int(raw["availableCount"]),
            "expires": [fmt_ts(datetime.fromtimestamp(t, UTC)) for t in expires],
        }
    except (
        KeyError,
        TypeError,
        ValueError,
        AttributeError,
        OverflowError,
        OSError,
    ) as exc:
        raise NotMeasurable(f"reset credits unreadable: {exc!r}") from exc


def parse_app_server(result: dict, *, now: datetime) -> dict:
    """Convert the `result` of `account/rateLimits/read` to the output form.

    Account data is not taken over: only the named fields end up in the reading.
    """
    per_id = result.get("rateLimitsByLimitId")
    limits = per_id.get("codex") if isinstance(per_id, dict) else None
    if limits is None:
        limits = result.get("rateLimits")
    if not isinstance(limits, dict):
        raise NotMeasurable("no rateLimits in the app-server answer")
    return {
        "source": "app-server",
        "generated_at": fmt_ts(now),
        "age_s": 0,
        "source_file": None,
        "lower_bound": False,
        "session": _window(limits.get("primary"), "primary"),
        "week": _window(limits.get("secondary"), "secondary"),
        "reset_credits": _reset_credits(result.get("rateLimitResetCredits")),
    }


def _rollout_window(raw: object) -> dict | None:
    """A window from a log line; `None` if it is no usable object.

    `used_percent` is a float in the log (`54.6`); `int()` truncates, so that the lower
    bound stays a lower bound.
    """
    if not isinstance(raw, dict):
        return None
    try:
        return {
            "pct": int(raw["used_percent"]),
            "reset_at": fmt_ts(datetime.fromtimestamp(raw["resets_at"], UTC)),
        }
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return None


def _rollout_lines(path: Path) -> Iterator[tuple[datetime, dict, dict]]:
    """The usable `rate_limits` lines of a log file: (line time, session, week).

    Usable: `limit_id` is `codex` or missing, and both windows are an object. The log also
    contains lines with `limit_id: "premium"` and `primary: null`; those do not count. An
    unreadable line or an unreadable file is skipped.
    """
    try:
        with path.open(encoding="utf-8") as file:
            for line in file:
                if "rate_limits" not in line:
                    continue
                try:
                    rec = json.loads(line)
                    limits = rec["payload"]["rate_limits"]
                    if limits.get("limit_id", "codex") != "codex":
                        continue
                    line_time = datetime.fromisoformat(rec["timestamp"])
                    if line_time.tzinfo is None:
                        continue
                except (ValueError, KeyError, TypeError, AttributeError):
                    continue
                session = _rollout_window(limits.get("primary"))
                week = _rollout_window(limits.get("secondary"))
                if session is not None and week is not None:
                    yield line_time, session, week
    except (OSError, ValueError):
        return  # unreadable file (not utf-8 either): what was already read stays usable


def read_rollout(homes: Sequence[Path], *, now: datetime) -> dict:
    """The newest usable `rate_limits` line from the session logs of all `homes`.

    The log is as old as the last `codex exec` in that home: the reading is therefore a
    lower bound (`lower_bound: True`) and `age_s` says how old. "Newest" is the youngest
    line time over all homes together. A missing home counts as empty.
    """
    best: tuple[datetime, dict, dict, Path] | None = None
    for home in homes:
        for path in sorted((home / "sessions").glob("**/rollout-*.jsonl")):
            for line_time, session, week in _rollout_lines(path):
                if best is None or line_time > best[0]:
                    best = (line_time, session, week, path)
    if best is None:
        raise NotMeasurable("no usable rate_limits line in the session logs")
    line_time, session, week, path = best
    return {
        "source": "rollout-log",
        "generated_at": fmt_ts(now),
        "age_s": int((now - line_time).total_seconds()),
        "source_file": str(path),
        "lower_bound": True,
        "session": session,
        "week": week,
        "reset_credits": None,
    }


def read_usage(*, homes: Sequence[Path], timeout: float, now: datetime) -> dict:
    """The reading: first the app-server per home, then the session log as a lower bound.

    The first home that gives a readable reading wins; the quota is account-wide, so the
    second home is only a reserve for when the first has no auth or does not answer. Only
    when all homes fail does the log follow. If that fails too, `NotMeasurable` contains the
    reasons of both sources.
    """
    reasons: list[str] = []
    for home in homes:
        try:
            return parse_app_server(read_app_server(home, timeout=timeout), now=now)
        except NotMeasurable as exc:
            reasons.append(f"{home}: {exc}")
    try:
        return read_rollout(homes, now=now)
    except NotMeasurable as exc:
        app_server = "; ".join(reasons) or "no homes"
        raise NotMeasurable(f"app-server: {app_server}; rollout-log: {exc}") from exc


@dataclass(frozen=True)
class BudgetOutcome:
    """The decision of `budget`; the field order is the key order of the JSON.

    `window` and `reset_at` are only filled for `fallback`; `None` is "there is none".
    """

    decision: str
    reason: str
    window: str | None = None
    reset_at: str | None = None
    session_pct: int | None = None
    week_pct: int | None = None
    source: str | None = None
    lower_bound: bool | None = None
    thresholds: dict | None = None


def decide_budget(
    reading: dict | None, *, review_5h: int, review_week: int, now: datetime
) -> BudgetOutcome:
    """Pure: `codex`, `fallback` or `not_measurable` for a review round. First hit
    wins.

    The week goes before the 5-hour window; a log reading is a lower bound: above a threshold
    it counts, below it proves nothing, and an expired reset makes it unusable. A past
    `reset_at` in the window that hits the threshold is an outdated reading: not
    measurable, no fallback.
    """
    thresholds = {"session": review_5h, "week": review_week}
    if reading is None:
        return BudgetOutcome("not_measurable", "not measurable", thresholds=thresholds)
    session, week = reading["session"], reading["week"]
    lower_bound = reading["lower_bound"]

    def outcome(decision: str, reason: str, **extra) -> BudgetOutcome:
        return BudgetOutcome(
            decision,
            reason,
            session_pct=session["pct"],
            week_pct=week["pct"],
            source=reading["source"],
            lower_bound=lower_bound,
            thresholds=thresholds,
            **extra,
        )

    if lower_bound and any(
        datetime.fromisoformat(v["reset_at"]) <= now for v in (session, week)
    ):
        return outcome("not_measurable", "not measurable: log reading expired")
    for window, name, label, pct, threshold in (
        (week, "codex-week", "week", week["pct"], review_week),
        (session, "codex-5h", "5h", session["pct"], review_5h),
    ):
        if pct < threshold:
            continue
        if datetime.fromisoformat(window["reset_at"]) <= now:
            reason = (
                "not measurable: window still rolling "
                f"(reset {window['reset_at']} has passed)"
            )
            return outcome("not_measurable", reason)
        return outcome(
            "fallback",
            f"{label} {pct}% ≥ {threshold}%",
            window=name,
            reset_at=window["reset_at"],
        )
    if lower_bound:
        return outcome(
            "not_measurable",
            "not measurable: log reading below the thresholds is no proof",
        )
    return outcome("codex", f"ok: 5h {session['pct']}%, week {week['pct']}%")


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class CodexUsageConfig:
    """The `[codex_usage]` keys that `read` and `budget` use."""

    app_server_timeout_seconds: int
    review_pct_session: int
    review_pct_week: int


def load_config() -> CodexUsageConfig:
    """`[codex_usage]` from `lighthouse.toml`, with the defaults of the config reader."""
    cfg = config.load(config.config_path())
    return CodexUsageConfig(
        **{
            name: config.lookup(cfg, f"codex_usage.{name}")
            for name in (
                "app_server_timeout_seconds",
                "review_pct_session",
                "review_pct_week",
            )
        }
    )


def _config() -> CodexUsageConfig:
    """`load_config()`, with every load error as `NotMeasurable("config: ...")`.

    The CLI contracts only know 'not measurable'.
    """
    try:
        return load_config()
    except ConfigError as exc:
        raise NotMeasurable(f"config: {exc}") from exc


def cmd_read() -> int:
    """Print the reading as JSON (exit 0); not measurable: reason on stderr, exit 1."""
    try:
        reading = read_usage(
            homes=codex_homes(),
            timeout=_config().app_server_timeout_seconds,
            now=_now(),
        )
    except NotMeasurable as exc:
        print(f"not measurable: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(reading, indent=2))
    return 0


def cmd_budget() -> int:
    """Decide before a review round; print one JSON object, exit 6 on `fallback`, else 0."""
    try:
        cfg = _config()
    except NotMeasurable as exc:
        # The 0/6 contract stays, but a broken config must not be only in the JSON.
        print(f"codex-usage: {exc}", file=sys.stderr)
        outcome = BudgetOutcome("not_measurable", f"not measurable: {exc}")
    else:
        now = _now()
        reason = None
        try:
            reading = read_usage(
                homes=codex_homes(), timeout=cfg.app_server_timeout_seconds, now=now
            )
        except NotMeasurable as exc:
            reading, reason = None, f"not measurable: {exc}"
        outcome = decide_budget(
            reading,
            review_5h=cfg.review_pct_session,
            review_week=cfg.review_pct_week,
            now=now,
        )
        if reason is not None:
            outcome = replace(outcome, reason=reason)
    print(json.dumps(asdict(outcome), ensure_ascii=False))
    return 6 if outcome.decision == "fallback" else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read the Codex usage window.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("read", help="print the reading as JSON")
    sub.add_parser(
        "budget", help="decide before a review round: codex or fallback (exit 0/6)"
    )
    args = parser.parse_args(argv)
    if args.cmd == "read":
        return cmd_read()
    if args.cmd == "budget":
        return cmd_budget()
    raise AssertionError(f"unknown subcommand: {args.cmd}")  # argparse already prevents this


if __name__ == "__main__":
    sys.exit(main())

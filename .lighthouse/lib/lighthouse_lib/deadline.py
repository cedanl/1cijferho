# lighthouse: core@v0.8.1
"""Run a command until an absolute epoch deadline, then kill the process group.

A PreToolUse hook that hits its own timeout is fail-open per the Claude Code
documentation: Claude sees no error, only silence, and the test gate (task 3)
would be skipped unnoticed. By having the gate itself end on a deadline (well
within the hook timeout) and hard-killing the whole child, subprocesses
included, the gate fails loudly instead of dropping out silently.
"""

# `X | None` in annotations: the hooks also run under python3 3.9 (macOS).
from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping

TIMEOUT_EXIT = 124
NOT_FOUND_EXIT = 127

# Signals that can stop the hook itself. The child runs in its own session and so
# does not get them by itself: without forwarding, the suite kept running as an orphan.
FORWARDED = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)

USAGE = "usage: deadline.py <epoch-seconds> -- <cmd> [args...]"


class _Stopped(Exception):
    """A forwarded signal interrupts the wait for the child."""

    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


class Interrupted(Exception):
    """`run_until(..., raise_on_signal=True)`: a forwarded signal stopped the child.

    `signum` is the signal, `output` what the child wrote up to then (`b""`
    without `capture`). That way "this process got a signal" can be told apart
    from a child that itself ended with 128 + n.
    """

    def __init__(self, signum: int, output: bytes) -> None:
        super().__init__(signum)
        self.signum = signum
        self.output = output


class _Forwarder:
    """Signal handler for FORWARDED while `run_until` runs.

    The first signal interrupts the wait (`_Stopped`); outside it, e.g. in the
    middle of Popen, it is only noted, so a child is never left unmanaged.
    A second signal skips the grace: SIGKILL.
    """

    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.signum: int | None = None
        self.waiting = False

    def __call__(self, signum: int, _frame: object) -> None:
        if self.signum is not None:
            if self.proc is not None:
                _kill_group(self.proc.pid, signal.SIGKILL)
            return
        self.signum = signum
        if self.waiting:
            raise _Stopped(signum)

    def wait(self, proc: subprocess.Popen, timeout: float) -> int:
        """`proc.wait(timeout)`, or `_Stopped` on a signal received (earlier too)."""
        self.proc = proc
        self.waiting = True
        try:
            if self.signum is not None:
                raise _Stopped(self.signum)
            return proc.wait(timeout=timeout)
        finally:
            self.waiting = False


def _kill_group(pgid: int, sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


def _stop_group(proc: subprocess.Popen, grace: float) -> None:
    """SIGTERM to the process group, wait `grace` seconds, then SIGKILL."""
    # start_new_session makes the child session and group leader: pid == pgid.
    # No os.getpgid: it fails if the child is already gone just after the timeout.
    pgid = proc.pid
    _kill_group(pgid, signal.SIGTERM)
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        _kill_group(pgid, signal.SIGKILL)
        proc.wait()


def _start(
    argv: list[str],
    cwd: str | None,
    stdin: object,
    out: object,
    env: Mapping[str, str] | None = None,
) -> subprocess.Popen | None:
    """Start `argv` in its own session; None (plus one line of stderr) if it does not exist."""
    kwargs: dict[str, object] = {}
    if out is not None:
        kwargs["stdout"] = out
        kwargs["stderr"] = subprocess.STDOUT
    try:
        return subprocess.Popen(
            argv, start_new_session=True, cwd=cwd, stdin=stdin, env=env, **kwargs
        )
    except FileNotFoundError:
        sys.stderr.write(f"deadline.py: command not found: {argv[0]}\n")
        return None


def _read_captured(out: object) -> bytes:
    """Read everything back from the shared tempfile, wherever the write position is."""
    out.seek(0)
    return out.read()


def run_until(
    deadline: float,
    argv: list[str],
    grace: float = 10.0,
    *,
    cwd: str | None = None,
    stdin_bytes: bytes | None = None,
    capture: bool = False,
    raise_on_signal: bool = False,
    env: Mapping[str, str] | None = None,
) -> int | tuple[int, bytes]:
    """Run `argv` until `deadline` (epoch seconds); exit code or TIMEOUT_EXIT.

    A deadline that has already passed does not start the command and returns
    TIMEOUT_EXIT at once (with `capture=True`: `(TIMEOUT_EXIT, b"")`, the
    command was never started). Without `capture`, stdout/stderr are inherited
    from the parent, exactly the behaviour from before these parameters. When
    the deadline passes, first SIGTERM to the whole process group, then wait
    `grace` seconds, then SIGKILL. A missing command returns NOT_FOUND_EXIT
    with one line on stderr, no traceback: 'could not start' is something
    other than 'tests fail'. If this process gets SIGTERM/SIGINT/SIGHUP, it
    stops the group the same way and returns 128 + signal number; a second
    signal during the cleanup skips the grace and kills at once.

    `cwd` passes the child's working directory (otherwise it inherits the
    parent's). `stdin_bytes`, if set, goes to the child through a
    `tempfile.TemporaryFile` (seeked to 0) instead of a pipe; with None the
    child inherits the parent's stdin, as before.

    `capture=True` makes the child's stdout and stderr both write to one shared
    `tempfile.TemporaryFile` (never a pipe: it fills up at ~64 KiB while the
    parent waits on the child) and returns `(exitcode, output)`, read back
    after it ends, also on TIMEOUT_EXIT (what had been written up to then) and
    on NOT_FOUND_EXIT (then `b""`; the missing-command message goes only to
    this process's stderr in that case, not into the buffer). Without
    `capture` the return type stays a plain `int`.

    `env` is the child's whole environment; with None it inherits the parent's,
    as before.

    `raise_on_signal=True` raises `Interrupted` on a forwarded signal (after
    stopping the group) instead of returning 128 + signal number: the child
    can have that number as its own exit code too.
    """
    remaining = deadline - time.time()
    if remaining <= 0:
        return (TIMEOUT_EXIT, b"") if capture else TIMEOUT_EXIT

    with contextlib.ExitStack() as files:
        stdin_file = None
        if stdin_bytes is not None:
            stdin_file = files.enter_context(tempfile.TemporaryFile())
            stdin_file.write(stdin_bytes)
            stdin_file.seek(0)
        out_file = files.enter_context(tempfile.TemporaryFile()) if capture else None

        forwarder = _Forwarder()
        previous = {sig: signal.signal(sig, forwarder) for sig in FORWARDED}
        try:
            proc = _start(argv, cwd, stdin_file, out_file, env)
            if proc is None:
                return (NOT_FOUND_EXIT, b"") if capture else NOT_FOUND_EXIT
            signum = None
            try:
                code = forwarder.wait(proc, remaining)
                return (code, _read_captured(out_file)) if capture else code
            except subprocess.TimeoutExpired:
                code = TIMEOUT_EXIT
            except _Stopped as stopped:
                signum = stopped.signum
                code = 128 + signum
            _stop_group(proc, grace)
            output = _read_captured(out_file) if capture else b""
            if signum is not None and raise_on_signal:
                raise Interrupted(signum, output)
            return (code, output) if capture else code
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def main(argv: list[str]) -> int:
    if len(argv) < 3 or argv[1] != "--":
        sys.stderr.write(USAGE + "\n")
        return 2
    try:
        deadline = float(argv[0])
    except ValueError:
        sys.stderr.write(USAGE + "\n")
        return 2
    cmd = argv[2:]
    return run_until(deadline, cmd)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

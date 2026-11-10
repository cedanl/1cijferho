# lighthouse: core@v0.8.1
"""Take one of the gate slots with `flock`; the slot outlives this process.

The test gate lets at most two suites run at once (#1184). The hook's bash
opens the lock files as fd 8 and 9; this script inherits those fds, tries them
in order with `flock(LOCK_EX | LOCK_NB)` and prints the 1-based number of the
first slot that succeeds. If all are taken, it retries every `interval`
seconds until the wait ceiling, and then ends with `SLOT_BUSY_EXIT`.

Why Python and not `flock(1)`: macOS does not ship that command, and the hook
runs there too.

Why the slot stays held: a `flock` belongs to the *open file description*, not
to the process. That description is the same one as that of the bash that
opened the fd. When this script ends, the lock therefore stays held as long as
the bash (and `deadline.py`, which inherits the fd) keeps it open; only when
both are gone does the slot go free. The hook itself closes the fd of the slot
it did not get.
"""

# `X | None` in annotations: the hooks also run under python3 3.9 (macOS).
from __future__ import annotations

import argparse
import fcntl
import sys
import time

SLOT_BUSY_EXIT = 75


def acquire(fds: list[int], wait: float, interval: float = 1.0) -> int | None:
    """Take the first free slot; return its 1-based index, or None after `wait` seconds.

    The first round is immediate; after that it sleeps `interval` between rounds
    until `time.monotonic()` is past start + `wait`. `BlockingIOError` means
    "taken"; every other `OSError` (e.g. a closed fd) propagates, because then
    it was not tested whether the slot was free.
    """
    deadline = time.monotonic() + wait
    while True:
        for index, fd in enumerate(fds, start=1):
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            return index
        if time.monotonic() >= deadline:
            return None
        time.sleep(interval)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="gate_slot.py")
    parser.add_argument("--fd", type=int, action="append", required=True)
    parser.add_argument("--wait", type=float, required=True)
    args = parser.parse_args(argv)
    try:
        slot = acquire(args.fd, args.wait)
    except OSError as err:
        sys.stderr.write(f"gate_slot.py: {err}\n")
        return 1
    if slot is None:
        return SLOT_BUSY_EXIT
    sys.stdout.write(f"{slot}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

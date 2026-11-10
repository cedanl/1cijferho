"""Parallel conversion must finish when the parent process handles SIGTERM itself.

Streamlit installs its own SIGTERM handler, which does not exit the process. With
the fork start method the pool workers inherit that handler. Pool.terminate()
takes the task queue lock, so a worker that reaches the queue afterwards can only
be stopped with SIGTERM. The inherited handler swallows that signal and the pool
waited on the worker forever: Turbo Convert never finished on Linux.
"""

import json
import multiprocessing as mp
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from eencijferho.core import converter

_SRC_DIR = Path(converter.__file__).resolve().parents[2]
_TIMEOUT_SECONDS = 30

# Runs in its own interpreter: the SIGTERM handler, the start method and the fork
# hook are process-wide settings that must not leak into the test session.
_CHILD_SCRIPT = """
import json
import multiprocessing as mp
import os
import signal
import time

mp.set_start_method("fork")
signal.signal(signal.SIGTERM, lambda signum, frame: None)

# Every worker after the first starts slowly: it is still starting when the first
# worker has converted all chunks (a small file on a machine with several cores).
forks = []
os.register_at_fork(
    before=lambda: forks.append(None),
    after_in_child=lambda: time.sleep(1) if len(forks) > 1 else None,
)

from eencijferho.core import converter

converter.mp.cpu_count = lambda: 3  # two workers
lines = [f"{i:02d}ab\\n" for i in range(12)]
rows = converter._run_parallel(lines, positions=[(0, 2), (2, 4)])
print(json.dumps(rows))
"""


def _kill_with_workers(process: subprocess.Popen) -> None:
    """Kill the child and its pool workers (they share one process group)."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


@pytest.mark.skipif(
    "fork" not in mp.get_all_start_methods(),
    reason="workers only inherit signal handlers with the fork start method",
)
def test_run_parallel_finishes_when_parent_handles_sigterm():
    env = {**os.environ, "PYTHONPATH": str(_SRC_DIR)}
    process = subprocess.Popen(
        [sys.executable, "-c", _CHILD_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        _kill_with_workers(process)
        pytest.fail(
            f"_run_parallel did not return within {_TIMEOUT_SECONDS} s: "
            "the pool shutdown waits on a worker that ignores SIGTERM"
        )

    assert process.returncode == 0, stderr
    assert json.loads(stdout) == [
        "00;ab",
        "01;ab",
        "02;ab",
        "03;ab",
        "04;ab",
        "05;ab",
        "06;ab",
        "07;ab",
        "08;ab",
        "09;ab",
        "10;ab",
        "11;ab",
    ]

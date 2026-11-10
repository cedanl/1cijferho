# lighthouse: python@v0.8.1
"""Git environment without repo-binding variables.

`prcov.py` needs the same cleanup as the coverage arm of
`ratchet.py` — one source, two callers.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

# Variables with which git pins a process to one repo. git reads them before
# the cwd, so with GIT_DIR set even `git -C /tmp/fixture rev-parse` ignores
# that -C and points at the real repo.
#
# That is not theory. git sets these when it calls a hook, and the
# coverage arm runs the full pytest suite from inside a hook. Without this
# cleanup every test that creates a fixture repo and calls `git add -A` +
# `git commit` builds in the REAL repo, and replaces the branch tip with a
# fixture commit.
_GIT_SCOPING_VARS = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_QUARANTINE_PATH",
    "GIT_COMMON_DIR",
    "GIT_NAMESPACE",
    "GIT_PREFIX",
)


def suite_env(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """The environment for a git subprocess, without git's repo-binding variables.

    `environ` is injectable so a test can imitate the hook case
    without mutating os.environ.
    """
    source = os.environ if environ is None else environ
    return {k: v for k, v in source.items() if k not in _GIT_SCOPING_VARS}

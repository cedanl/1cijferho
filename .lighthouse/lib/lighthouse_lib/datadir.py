# lighthouse: core@v0.8.1
"""Where the review records live (decision 27).

`LIGHTHOUSE_DATA` wins; otherwise `${XDG_DATA_HOME:-~/.local/share}/lighthouse/
review-records/`. The records carry session content and must never land in a
repository, so `data_dir()` refuses a directory inside a git work tree. It
creates the directory, like `data_dir()` in the source repository did.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


class DataDirInRepoError(RuntimeError):
    """The data directory is inside a git work tree; records could be committed."""


def _in_git_tree(path: Path) -> bool:
    return any((p / ".git").exists() for p in (path, *path.parents))


def data_dir(env: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env is None else env
    raw = env.get("LIGHTHOUSE_DATA")
    if raw:
        path = Path(raw).expanduser()
    else:
        # The XDG base directory spec ignores a relative XDG_DATA_HOME.
        xdg = env.get("XDG_DATA_HOME", "")
        base = Path(xdg) if os.path.isabs(xdg) else Path.home() / ".local" / "share"
        path = base / "lighthouse" / "review-records"
    path = path.resolve()
    if _in_git_tree(path):
        raise DataDirInRepoError(f"data directory is inside a git work tree: {path}")
    path.mkdir(parents=True, exist_ok=True)
    return path

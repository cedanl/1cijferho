# lighthouse: core@v0.8.1
"""Is a tree a checkout of this project's repo, beyond `--git-common-dir`? (#36)

The gates first compare `--git-common-dir`: the main checkout plus each of its
worktrees. A worktree of a separate clone of the same repo (forge's bare
clone, for one) has another common dir. This lib settles that case on the
`origin` remote: the same host and path, whatever the form (https, ssh, scp),
is the same repo. A local path or `file://` remote says nothing about the
upstream and never matches: precision before recall.

Used by `commit-test-gate.sh` (as a script), `pr_body_gate.py` and
`bash_writes.py`. Only stdlib: the hooks run under the host `python3`.

    python3 repo_identity.py <project_dir> <root>

Exit 0: same origin. Exit 1: not, with the reason on stdout. Exit 2: git
failed, with the error on stderr.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

GIT_TIMEOUT_S = 10

# `[user@]host:path` without a scheme; a `/` before the `:` makes it a local path.
_SCP_RE = re.compile(r"^(?:[^@/]+@)?([^:/]+):(.+)$")


class OriginError(Exception):
    """`remote.origin.url` could not be read: git failed or timed out."""


def remote_key(url: str) -> str | None:
    """`host/owner/repo` (lower case) of a remote URL, or `None` without host and path."""
    rest = url.strip()
    if "://" in rest:
        scheme, rest = rest.split("://", 1)
        if scheme.casefold() == "file":
            return None
        authority, _, path = rest.partition("/")
        host = authority.rsplit("@", 1)[-1].split(":", 1)[0]
    else:
        match = _SCP_RE.match(rest)
        if match is None:
            return None
        host, path = match.groups()
    path = path.strip("/").removesuffix(".git").strip("/")
    if not host or not path:
        return None
    return f"{host}/{path}".casefold()


def origin(root: str, *, timeout: float) -> str | None:
    """`remote.origin.url` of `root`, or `None` when it is not set."""
    try:
        proc = subprocess.run(
            ["git", "-C", root, "config", "--get", "remote.origin.url"],
            capture_output=True,
            text=True,
            timeout=max(0.1, timeout),
            env=dict(os.environ, LC_ALL="C"),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise OriginError(f"git timeout reading the origin of {root}") from exc
    except OSError as exc:
        raise OriginError(str(exc)) from exc
    if proc.returncode == 0:
        return proc.stdout.strip()
    if proc.returncode == 1 and not proc.stderr.strip():
        return None
    raise OriginError(f"git failed (rc {proc.returncode}): {proc.stderr.strip()}")


def same_origin(project_dir: str, root: str, *, timeout: float) -> tuple[bool, str]:
    """`(True, "")` when `root` has the same origin as `project_dir`.

    Otherwise `(False, "origin <root's> vs <project's>")`, `none` for a missing
    one. Raises `OriginError` when git fails for either.
    """
    ours = origin(project_dir, timeout=timeout)
    theirs = origin(root, timeout=timeout)
    key = remote_key(ours) if ours else None
    if key is not None and theirs and remote_key(theirs) == key:
        return True, ""
    return False, f"origin {theirs or 'none'} vs {ours or 'none'}"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: repo_identity.py <project_dir> <root>", file=sys.stderr)
        return 2
    try:
        same, reason = same_origin(argv[0], argv[1], timeout=GIT_TIMEOUT_S)
    except OriginError as exc:
        print(exc, file=sys.stderr)
        return 2
    if same:
        return 0
    print(reason)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

#!/usr/bin/env python3
# lighthouse: core@v0.8.1
"""Post a committed spec or plan file as a version chain of issue comments.

Steps (spec *scripts/post_artefact.py*):
    1. resolve the path, find the git toplevel, take the kind from the directory
       (`paths.specs` or `paths.plans`)
    2. the file must be committed and clean
    3. the commit must be on GitHub (push if needed, except on `forge.default_branch`,
       HEAD or with an open PR)
    4. read the existing comments and find the current complete version
    5. same hash and a commit that is still reachable: unchanged, post nothing
    6. split, render all parts, then post them in order
    7. if part k fails, report how many parts did get posted
    8. success line on stdout

USAGE
    post_artefact.py <issue-nr> <file>

Exit codes: 0 posted or unchanged, 1 every `artefact:` error (on stderr), 2 usage error.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib` imports
# from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib.artefacts import (
    Meta,
    current,
    hash_of,
    next_version,
    parse_meta,
    render,
    split,
)
from lighthouse_lib.config import ConfigError, config_path, load, lookup
from lighthouse_lib.protocol import Comment


class ArtefactError(Exception):
    """`str(err)` is the full `artefact:` line."""


def _config_error(err: ConfigError) -> ArtefactError:
    return ArtefactError(f"artefact: config: {' '.join(str(err).split())}")


def _setting(key: str) -> str:
    """A key from `lighthouse.toml`, with its default."""
    try:
        return str(lookup(load(config_path()), key))
    except ConfigError as err:
        raise _config_error(err) from err


def _repo() -> str:
    """`forge.repo`; an `ArtefactError` if it is missing or the config is broken."""
    try:
        repo = load(config_path()).repo
    except ConfigError as err:
        raise _config_error(err) from err
    if repo is None:
        raise ArtefactError(f"artefact: forge.repo is not set in {config_path()}")
    return repo


def _dirs() -> dict[str, str]:
    """Directory of the file (from the git root) to its kind, with a single trailing `/`."""
    return {
        _setting("paths.specs").strip("/") + "/": "spec",
        _setting("paths.plans").strip("/") + "/": "plan",
    }


class RunError(Exception):
    """git or gh did not return 0 (or took too long); the message carries the stderr."""


def _run(command: list[str], stdin_text: str | None = None) -> str:
    # 300 s, not 60: `git push` runs the whole pre-push chain.
    try:
        return subprocess.run(
            command,
            input=stdin_text,
            capture_output=True,
            text=True,
            timeout=300,
            check=True,
        ).stdout.strip()
    except subprocess.CalledProcessError as err:
        # The tail: pre-push hooks print their `ok` lines first, the real error comes last.
        raise RunError((err.stderr or "").strip()[-300:]) from err
    except subprocess.TimeoutExpired as err:
        raise RunError(f"timed out after {err.timeout:.0f} s") from err


def git(cwd: Path, *args: str) -> str:
    return _run(["git", "-C", str(cwd), *args])


def gh(*args: str, stdin_text: str | None = None) -> str:
    return _run(["gh", *args], stdin_text)


def commit_known(sha: str) -> bool:
    """Is `sha` on GitHub? Only a 404/422 from gh counts as no; every other error propagates."""
    try:
        gh("api", f"repos/{_repo()}/commits/{sha}", "--jq", ".sha")
    except RunError as err:
        if "HTTP 404" in str(err) or "HTTP 422" in str(err):
            return False
        raise
    return True


def open_pr(branch: str) -> bool:
    return (
        gh(
            "pr",
            "list",
            "--repo",
            _repo(),
            "--head",
            branch,
            "--state",
            "open",
            "--json",
            "number",
            "--jq",
            "length",
        )
        != "0"
    )


def read_comments(nr: int) -> list[Comment]:
    out = gh(
        "api",
        "--paginate",
        f"repos/{_repo()}/issues/{nr}/comments?per_page=100",
        "--jq",
        ".[] | {id, body, created_at, html_url}",
    )
    return [Comment.from_gh(json.loads(line)) for line in out.splitlines() if line]


def post_comment(nr: int, body: str) -> tuple[int, str]:
    out = gh(
        "api",
        f"repos/{_repo()}/issues/{nr}/comments",
        "--input",
        "-",
        "--jq",
        "{id, html_url}",
        stdin_text=json.dumps({"body": body}),
    )
    data = json.loads(out)
    return int(data["id"]), str(data["html_url"])


def _resolve_file(path_arg: str) -> tuple[Path, str, str]:
    """(git root, path relative to that root, kind) of `path_arg`."""
    path = Path(path_arg).resolve()
    if not path.is_file():
        raise ArtefactError(f"artefact: {path_arg} does not exist")
    try:
        top = Path(git(path.parent, "rev-parse", "--show-toplevel"))
    except RunError as err:
        raise ArtefactError(f"artefact: {path_arg} is not in a git repository") from err
    file = path.relative_to(top).as_posix()
    for prefix, kind in _dirs().items():
        if file.startswith(prefix):
            return top, file, kind
    raise ArtefactError(f"artefact: {file} is no spec or plan")


def _committed_sha(top: Path, file: str) -> str:
    """The last commit of `file`; an error if the file is modified or untracked."""
    if git(top, "status", "--porcelain", "--", file):
        raise ArtefactError(f"artefact: {file} not committed")
    sha = git(top, "log", "-1", "--format=%H", "--", file)
    if not sha:
        raise ArtefactError(f"artefact: {file} not committed")
    return sha


def _commit_known_or_gh_error(sha: str) -> bool:
    try:
        return commit_known(sha)
    except RunError as err:
        raise ArtefactError(f"artefact: gh failed: {err}") from err


def _ensure_commit_on_github(top: Path, sha: str) -> None:
    if _commit_known_or_gh_error(sha):
        return
    branch = git(top, "rev-parse", "--abbrev-ref", "HEAD")
    try:
        has_pr = open_pr(branch)
    except RunError as err:
        raise ArtefactError(f"artefact: gh failed: {err}") from err
    # A force-push to the default branch, a detached HEAD or a branch with an open PR is
    # never ours to do.
    if branch in (_setting("forge.default_branch"), "HEAD") or has_pr:
        raise ArtefactError(f"artefact: commit {sha[:7]} not on GitHub")
    try:
        git(top, "push", "-u", "--force-with-lease", "origin", "HEAD")
    except RunError as err:
        raise ArtefactError(f"artefact: push of {branch} failed: {err}") from err


def _unchanged_meta(latest: list[Comment], hash_: str) -> Meta | None:
    """The meta of the current version if it has the same content and a reachable commit."""
    meta = parse_meta(latest[0].body) if latest else None
    if (
        meta is not None
        and meta.hash == hash_
        and _commit_known_or_gh_error(meta.commit)
    ):
        return meta
    return None


def _post_parts(nr: int, bodies: list[str]) -> list[str]:
    """Post the parts in order; return the urls. If part k fails, an `ArtefactError`."""
    urls: list[str] = []
    for k, body in enumerate(bodies, start=1):
        try:
            urls.append(post_comment(nr, body)[1])
        except RunError as err:
            raise ArtefactError(
                f"artefact: part {k}/{len(bodies)} not posted ({k - 1} were): {err}"
            ) from err
    return urls


def _post_artefact(nr: int, path_arg: str) -> str:
    _repo()
    top, file, kind = _resolve_file(path_arg)
    try:
        sha = _committed_sha(top, file)
        _ensure_commit_on_github(top, sha)
    except RunError as err:
        raise ArtefactError(f"artefact: git failed: {err}") from err

    try:
        comments = read_comments(nr)
    except RunError as err:
        raise ArtefactError(f"artefact: gh failed: {err}") from err
    latest = current(comments, kind, file)

    data = (top / file).read_bytes()
    hash_ = hash_of(data)
    if meta := _unchanged_meta(latest, hash_):
        return f"artefact: {kind} unchanged (version {meta.version})"

    version = next_version(comments, kind, file)
    replaces = latest[0].url if latest else None
    parts = split(data.decode("utf-8"))
    try:
        # Render everything first: on a refusal nothing is left half posted.
        bodies = [
            render(
                Meta(kind, version, k, len(parts), file, sha, hash_, replaces), part
            )
            for k, part in enumerate(parts, start=1)
        ]
    except ConfigError as err:
        raise _config_error(err) from err
    except ValueError as err:
        raise ArtefactError(f"artefact: metadata unusable: {err}") from err
    urls = _post_parts(nr, bodies)
    return f"artefact: {kind} version {version} posted, {len(parts)} parts → {urls[0]}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("nr", type=int, help="issue number")
    parser.add_argument("file", help="spec or plan file")
    args = parser.parse_args(argv)
    try:
        line = _post_artefact(args.nr, args.file)
    except ArtefactError as err:
        print(err, file=sys.stderr)
        return 1
    print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())

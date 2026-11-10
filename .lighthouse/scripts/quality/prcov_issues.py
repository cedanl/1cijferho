# lighthouse: python@v0.8.1
"""Issue, handoff and board per PR: one per PR below the threshold.

Feeds the nightly arm of `ratchet.py`: for every PR with too low diff coverage
this module opens (or repairs) a GitHub issue, pastes the `🤖 Handoff` comment
of the comment protocol (`lighthouse_lib.protocol`), and puts the issue on the
board with status `Controller`. An issue goes through four steps, in this
order: (1) `issue create` with the labels from `python.coverage_issue_labels`,
(2) the handoff comment, (3) on the board, (4) status `Controller`. The comment
comes before the status: the controller only looks at issues on `Controller`,
so it never sees the issue without a handoff. An issue from an earlier night
goes through the same four steps, but skips every step that is already done
(reconcile, no second issue, no duplicate comment).

The board is optional (decision 16): without `[forge.board]` only the
issue and the handoff come. Without a status option `Controller` the item stays on `To do`.
Repo, language, board and default branch come from `lighthouse.toml`.
The text of issue and handoff is English, also with `[project] language = "nl"`;
only the heading follows the project language (decision 7 of the M0 package spec).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

# `quality.diffcov` needs the scripts/ directory on sys.path (one directory above this one),
# `lighthouse_lib` the lib/ directory beside it: installed `.lighthouse/scripts/quality/`
# next to `.lighthouse/lib/` (design choice 6 of (b), for a script in a subdirectory).
_SCRIPTS = Path(__file__).resolve().parents[1]
_LIB = Path(__file__).resolve().parents[2] / "lib"
for _path in (_SCRIPTS, _LIB):
    if _path.is_dir() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from lighthouse_lib import config, headings, protocol
from quality.diffcov import DiffCoverage

Gh = Callable[[Sequence[str], Mapping[str, str] | None, str | None], str]

BOARD_TOKEN_ENV = "LIGHTHOUSE_BOARD_TOKEN"
# The worktree in the handoff is a path on the host where the controller runs, not
# on the runner: the nightly sets it in this env. Without the env, the main checkout of the
# repo in which the script runs.
HANDOFF_WORKTREE_ENV = "LIGHTHOUSE_HANDOFF_WORKTREE"
START_COMMAND = "/lighthouse-start-issue"

# Status `None` (no item yet) or `"To do"` (Auto-add just put it there) —
# in both cases the controller or the user has not yet taken over the
# issue. Every other status (`Controller`, `In progress`, `On hold`,
# `Done`) stays.
NOT_TAKEN_OVER = (None, "To do")

def _config() -> config.Config:
    """`lighthouse.toml`, re-read on every call; `forge.repo` is required."""
    cfg = config.load(config.config_path())
    if cfg.repo is None:
        raise config.ConfigError(f"{config.config_path()}: forge.repo is not set")
    return cfg


def _setting(key: str) -> object:
    return config.lookup(_config(), key)


def board() -> config.Board | None:
    """The board from `[forge.board]`, or `None` if the repo has none."""
    return _config().board


def default_branch() -> str:
    return str(_setting("forge.default_branch"))


def labels() -> list[str]:
    value = _setting("python.coverage_issue_labels")
    return [str(label) for label in value] if isinstance(value, list) else []


def nightly_workflow() -> str | None:
    """The workflow of the watermark (`python.nightly_workflow`), or `None`."""
    try:
        return str(_setting("python.nightly_workflow"))
    except KeyError:
        return None


def handoff_worktree() -> str:
    """The worktree in the handoff: `LIGHTHOUSE_HANDOFF_WORKTREE`, otherwise the directory above
    the shared `.git` of this repo (the main checkout, also from a worktree)."""
    override = os.environ.get(HANDOFF_WORKTREE_ENV)
    if override:
        return override
    proc = subprocess.run(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=config.repo_root(),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        raise config.ConfigError(
            f"the main checkout is not found ({proc.stderr.strip() or 'git rev-parse failed'}); "
            f"set {HANDOFF_WORKTREE_ENV}"
        )
    return str(Path(proc.stdout.strip()).parent)


# Owner and name go along as GraphQL variables: the query does not know the repo.
_STATUS_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    issue(number: $number) {
      projectItems(first: 20) {
        nodes {
          id
          project { id }
          fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
        }
      }
    }
  }
}
"""


class _GhError(subprocess.CalledProcessError):
    """A `CalledProcessError` (`isinstance` stays correct), but with the stderr
    of `gh` in `str(error)` — otherwise a naive `print(error)` in the
    nightly log shows only "returned non-zero exit status N", not what `gh` itself said."""

    def __str__(self) -> str:
        basis = super().__str__()
        stderr = (self.stderr or "").strip()
        return f"{basis}\n{stderr}" if stderr else basis


def run_gh(
    args: Sequence[str],
    env: Mapping[str, str] | None = None,
    input: str | None = None,
) -> str:
    """The real `Gh`: calls `gh`, with `env` on top of the existing environment.

    `timeout=60`: a hanging `gh` call must not
    hold up the nightly. On rc ≠ 0 a `subprocess.CalledProcessError`
    (via `_GhError`, see there) — never silently carry on past an error from `gh`.
    """
    full_env = {**os.environ, **env} if env else None
    try:
        proc = subprocess.run(
            ["gh", *args],
            input=input,
            capture_output=True,
            text=True,
            env=full_env,
            timeout=60,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        raise _GhError(error.returncode, error.cmd, error.output, error.stderr) from error
    return proc.stdout


def title(pr: int, pct: float, threshold: int) -> str:
    return f"Diff-coverage #{pr} {pct:.1f}% < {threshold}%"


def ranges(lines: Sequence[int]) -> str:
    """`[3, 4, 5, 9]` → `"3-5, 9"` — consecutive line numbers as a range."""
    ordered = sorted(set(lines))
    if not ordered:
        return ""
    parts: list[str] = []
    start = previous = ordered[0]
    for number in ordered[1:]:
        if number == previous + 1:
            previous = number
            continue
        parts.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = number
    parts.append(str(start) if start == previous else f"{start}-{previous}")
    return ", ".join(parts)


def body(pr: int, result: DiffCoverage, threshold: int, run_url: str | None) -> str:
    lines = [
        (
            f"PR #{pr} is on `{default_branch()}` with {result.pct:.1f}% diff coverage on the lines "
            f"it still owns, below the {threshold}% floor in `quality-baseline.toml`."
        ),
        "",
        "Uncovered lines:",
    ]
    for path in sorted(result.uncovered):
        lines.append(f"- `{path}`: {ranges(result.uncovered[path])}")
    lines.append("")
    if run_url:
        lines.append(f"Found by the nightly run: {run_url}")
    lines.append(
        "Cover these lines with a test, or mark code that cannot run with "
        "`# pragma: no cover` and a reason."
    )
    return "\n".join(lines)


def last_success_sha(gh: Gh, workflow: str) -> str | None:
    """`headSha` of the last successful run of `workflow` on the default branch,
    or `None`.

    `--event schedule` excludes a `workflow_dispatch`: a manual run with
    `since=`/a low `min_diff_pct` is otherwise also `success` and shifts the
    watermark, after which PRs in between are never judged.
    """
    output = gh(
        [
            "run",
            "list",
            "--workflow",
            workflow,
            "--branch",
            default_branch(),
            "--status",
            "success",
            "--event",
            "schedule",
            "-L",
            "1",
            "--json",
            "headSha",
        ],
        None,
        None,
    )
    runs = json.loads(output)
    if not runs:
        return None
    return str(runs[0]["headSha"])


def ensure_issue(
    gh: Gh, pr: int, result: DiffCoverage, threshold: int, run_url: str | None
) -> tuple[str, int, str] | None:
    """Step 1: find, reconcile or create the issue for this PR.

    `(action, number, url)` with action `"created"` or `"reconciled"`; `None` if
    only a closed issue exists for this PR (already handled). Open
    beats closed. Reconcile only adds missing labels, never
    a second issue.
    """
    wanted = labels()
    prefix = f"Diff-coverage #{pr} "
    output = gh(
        [
            "issue",
            "list",
            "--state",
            "all",
            "--search",
            f'"{prefix}" in:title',
            "--json",
            "number,state,url,title,labels",
        ],
        None,
        None,
    )
    candidates = json.loads(output)
    matches = [c for c in candidates if str(c.get("title", "")).startswith(prefix)]
    open_matches = [c for c in matches if str(c.get("state", "")).upper() == "OPEN"]
    if open_matches:
        issue = open_matches[0]
        number = int(issue["number"])
        url = str(issue["url"])
        present = {label["name"] for label in issue.get("labels", [])}
        missing = [label for label in wanted if label not in present]
        if missing:
            edit_args = ["issue", "edit", str(number)]
            for label in missing:
                edit_args += ["--add-label", label]
            gh(edit_args, None, None)
        return ("reconciled", number, url)
    if matches:
        return None  # only closed — already handled
    new_title = title(pr, result.pct, threshold)
    new_body = body(pr, result, threshold, run_url)
    create_args = [
        "issue",
        "create",
        "--title",
        new_title,
        "--body",
        new_body,
    ]
    for label in wanted:
        create_args += ["--label", label]
    url = gh(create_args, None, None).strip()
    number = int(url.rstrip("/").rsplit("/", 1)[-1])
    return ("created", number, url)


def handoff_comment(issue_number: int, pr: int, threshold: int) -> str:
    """The `🤖 Handoff` comment of the protocol: the heading in `[project] language`, the
    keys in English (decision 21), so the controller parses and starts it."""
    cfg = _config()
    worktree = handoff_worktree()
    branch = str(config.lookup(cfg, "forge.default_branch"))
    start = f"{START_COMMAND} {issue_number}"
    return (
        f"{protocol.ROBOT} {headings.heading('kind_handoff', cfg.language)}\n"
        f"issue: #{issue_number}\n"
        f"worktree: {worktree}\n"
        f"branch: {branch}\n"
        f"anchor: #{issue_number}\n"
        f"next: {start}\n"
        "when: now\n"
        "\n"
        "```\n"
        f"Work in the main checkout {worktree} (branch {branch}). Goal: cover the "
        f"lines of PR #{pr} that the nightly found below {threshold}%.\n"
        "\n"
        f"Anchor: issue #{issue_number} (the uncovered lines are in the body)\n"
        "State: new issue from the nightly run; no worktree yet, nothing started.\n"
        f"First step: {start}\n"
        "```"
    )


def ensure_handoff(gh: Gh, issue_number: int, pr: int, threshold: int) -> bool:
    """Step 2: paste the handoff comment, unless it is already there.

    `--jq '.[].body | @json'` gives one line per comment: its whole body as a
    JSON string. `protocol.parse_header` reads the first line of that, so a
    comment that starts with `🤖 Handoff · …` counts, and so does a handoff from before
    the move (`volgende:`, `wanneer:`), but a comment that quotes the heading somewhere
    in the middle does not. `--paginate`: an issue with many comments otherwise returns
    only the first page.
    """
    output = gh(
        [
            "api",
            "--paginate",
            f"repos/{_config().repo}/issues/{issue_number}/comments",
            "--jq",
            ".[].body | @json",
        ],
        None,
        None,
    )
    for line in output.splitlines():
        if not line.strip():
            continue
        header = protocol.parse_header(str(json.loads(line)))
        if header is not None and header.kind == "handoff":
            return False
    gh(
        ["issue", "comment", str(issue_number), "--body-file", "-"],
        None,
        handoff_comment(issue_number, pr, threshold),
    )
    return True


def _read_status(
    gh: Gh, token: str, issue_number: int
) -> tuple[str, str | None] | None:
    """`(item_id, status)` of this issue on our board, or `None` if it is not on it."""
    repo = _config()
    assert repo.board is not None
    output = gh(
        [
            "api",
            "graphql",
            "-f",
            f"query={_STATUS_QUERY}",
            "-f",
            f"owner={repo.owner}",
            "-f",
            f"name={repo.name}",
            "-F",
            f"number={issue_number}",
        ],
        {"GH_TOKEN": token},
        None,
    )
    data = json.loads(output)
    nodes = data["data"]["repository"]["issue"]["projectItems"]["nodes"]
    for node in nodes:
        if node["project"]["id"] == repo.board.project_id:
            value = node.get("fieldValueByName") or {}
            return str(node["id"]), value.get("name")
    return None


def _set_controller(gh: Gh, token: str, item_id: str, board: config.Board) -> None:
    gh(
        [
            "project",
            "item-edit",
            "--id",
            item_id,
            "--project-id",
            board.project_id,
            "--field-id",
            board.status_field_id,
            "--single-select-option-id",
            board.status_options[config.CONTROLLER_STATUS_OPTION],
        ],
        {"GH_TOKEN": token},
        None,
    )


def _item_id_from_add(output: str) -> str:
    """`id` from `project item-add --format json`, or `""` if the answer is empty
    or unreadable.

    Seed value for `item_id`: the GraphQL status read after this overwrites it
    as soon as it finds a node on our project itself. Without this seed an
    `item-edit` runs with an empty `--id` if that read unexpectedly does not
    find the item — `project item-add` returns the same node id, only synchronously.
    """
    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return ""
    return str(data.get("id") or "") if isinstance(data, dict) else ""


def ensure_on_board(
    gh: Gh,
    issue_number: int,
    issue_url: str,
    token: str,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Steps 3 and 4: on the board, and status `Controller` if nobody has taken it over yet.

    Auto-add puts a new item on `To do` asynchronously; an `item-edit` before that
    step is overwritten. So first re-read as long as the Status is `None`
    (at most 5 times, with `sleep(3)` in between). If it is then in
    `NOT_TAKEN_OVER`, go to `Controller`, then `sleep(3)` and read once
    more: if it is then on `To do` (a late Auto-add), one more
    `item-edit`. Every other status stays.

    Without `[forge.board]` no board call at all; without a status option
    `Controller` it does go on the board, but the status stays. Both cases
    are in the returned line, neither is an error.
    """
    repo = _config()
    if repo.board is None:
        return "board: skipped — no [forge.board] configured"
    env = {"GH_TOKEN": token}
    added = gh(
        [
            "project",
            "item-add",
            str(repo.board.project_number),
            "--owner",
            repo.owner,
            "--url",
            issue_url,
            "--format",
            "json",
        ],
        env,
        None,
    )
    item_id = _item_id_from_add(added)

    status: str | None = None
    for attempt in range(5):
        found = _read_status(gh, token, issue_number)
        if found is not None:
            found_id, status = found
            if found_id:
                item_id = found_id
        if status is not None:
            break
        if attempt < 4:
            sleep(3)

    if status in NOT_TAKEN_OVER and config.CONTROLLER_STATUS_OPTION not in repo.board.status_options:
        return (
            f"#{issue_number} on board, status {status or 'none'}; "
            "board: status Controller skipped — no such status option"
        )
    if status in NOT_TAKEN_OVER:
        _set_controller(gh, token, item_id, repo.board)
        status = "Controller"
        sleep(3)
        found = _read_status(gh, token, issue_number)
        if found is not None:
            found_id, status = found
            if found_id:
                item_id = found_id
        if status == "To do":
            _set_controller(gh, token, item_id, repo.board)
            status = "Controller"

    return f"#{issue_number} on board, status {status}"

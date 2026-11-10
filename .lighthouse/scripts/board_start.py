#!/usr/bin/env python3
# lighthouse: core@v0.8.1
"""Move a picked-up issue from board status `To do` or `On hold` to `In progress`.

Called by `start-issue` right after the worktree step. Whoever deliberately picks up a
parked issue lifts the parking. `Controller` stays put because the controller selects on it
and runs `start-issue` itself; `In progress` and `Done` stay put as well.

Never fails hard: always exit 0 and exactly one line starting with `board: `, so that
picking up an issue does not block on it.

Mode `--reopen <nr>`: move a reopened issue from `Done` back to `In progress`.
Here the opposite is intended: fail-closed. Any other start status, an issue off the
board, an exception or a read-back status ≠ `In progress` gives one line
`board: ERROR — <cause>` and exit 1; this way `close-issue` never silently reports the board as fine.

Without `[forge.board]` both modes give `board: skipped — no [forge.board] configured` and
exit 0 (decision 16, 24). A broken config gives `board: ERROR — <cause>` in both modes
and exit 2.

USAGE
    board_start.py <nr>
    board_start.py --reopen <nr>
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib` imports
# from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib.config import Config, ConfigError, config_path, load

# Owner and name go along as GraphQL variables: the query does not know the repo.
QUERY = """
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
PICKABLE = ("To do", "On hold")
SKIPPED = "board: skipped — no [forge.board] configured"
# What `gh` and the GraphQL parsing can raise; shared by the default path and --reopen.
GH_ERRORS = (
    ConfigError,
    subprocess.CalledProcessError,
    subprocess.TimeoutExpired,
    OSError,
    KeyError,
    TypeError,
    ValueError,
)


def error_name(error: Exception) -> str:
    """Type name, for a `ConfigError` also the message (path and key), on one line."""
    if isinstance(error, ConfigError):
        return f"{type(error).__name__}: {' '.join(str(error).split())}"
    return type(error).__name__


def identity() -> Config:
    """The repo's config; a board without `forge.repo` is a config error."""
    cfg = load(config_path())
    if cfg.board is not None and cfg.repo is None:
        raise ConfigError(f"{config_path()}: [forge.board] needs forge.repo")
    return cfg


def gh(*args: str) -> str:
    return subprocess.run(
        ["gh", *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout


def read_item(number: int) -> tuple[str, str | None] | None:
    """`(item_id, status)` of the issue on the board, or `None` if it is not on it."""
    repo = identity()
    data = json.loads(
        gh(
            "api",
            "graphql",
            "-f",
            f"query={QUERY}",
            "-f",
            f"owner={repo.owner}",
            "-f",
            f"name={repo.name}",
            "-F",
            f"number={number}",
        )
    )
    nodes = data["data"]["repository"]["issue"]["projectItems"]["nodes"]
    for node in nodes:
        if node["project"]["id"] == repo.board.project_id:
            value = node.get("fieldValueByName") or {}
            return str(node["id"]), value.get("name")
    return None


def set_in_progress(item_id: str) -> None:
    board = identity().board
    gh(
        "project",
        "item-edit",
        "--id",
        item_id,
        "--project-id",
        board.project_id,
        "--field-id",
        board.status_field_id,
        "--single-select-option-id",
        board.status_options["In progress"],
    )


def reopen(number: int) -> int:
    """Set `Done` to `In progress` and read back; any deviation is exit 1 (fail-closed)."""
    try:
        item = read_item(number)
        if item is None:
            print(f"board: ERROR — #{number} is not on the board")
            return 1
        item_id, status = item
        if status == "In progress":
            print(f"board: #{number} is already In progress")
            return 0
        if status != "Done":
            print(
                f"board: ERROR — #{number} is "
                f"{'at ' + status if status else 'without status'}, not at Done"
            )
            return 1
        set_in_progress(item_id)
        read_back = read_item(number)
    except GH_ERRORS as error:
        print(f"board: ERROR — {error_name(error)}")
        return 1
    after = read_back[1] if read_back else None
    if after != "In progress":
        print(f"board: ERROR — #{number} is at {after or 'no status'} after the edit")
        return 1
    print(f"board: #{number} Done → In progress")
    return 0


def _no_board() -> int | None:
    """`None` with a board; without a board the message and exit 0, with a config error exit 2."""
    try:
        cfg = identity()
    except ConfigError as error:
        print(f"board: ERROR — {error_name(error)}")
        return 2
    if cfg.board is None:
        print(SKIPPED)
        return 0
    return None


def main(argv: list[str]) -> int:
    if argv[:1] == ["--reopen"]:
        if len(argv) != 2 or not argv[1].isdigit():
            print("board: usage: board_start.py --reopen <nr>")
            return 1
        code = _no_board()
        return reopen(int(argv[1])) if code is None else code
    if len(argv) != 1 or not argv[0].isdigit():
        print("board: usage: board_start.py <nr>")
        return 0
    code = _no_board()
    if code is not None:
        return code
    number = int(argv[0])
    try:
        item = read_item(number)
        if item is None:
            print(f"board: #{number} is not on the board")
            return 0
        item_id, status = item
        if status not in PICKABLE:
            print(
                f"board: #{number} stays {'at ' + status if status else 'without status'}"
            )
            return 0
        set_in_progress(item_id)
    except GH_ERRORS as error:
        print(f"board: skipped ({error_name(error)})")
        return 0
    print(f"board: #{number} {status} → In progress")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

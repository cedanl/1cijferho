# lighthouse: core@v0.8.1
"""Write the permission note of a controller run (#115).

    python3 note.py <state dir> <run id> < Notification payload

For a `permission_prompt` or `worker_permission_prompt` it writes
`<state dir>/runs/<run id>/permission.json` (mode 0600, tmp plus rename, each prompt
overwrites). `tool` and `path` come from the newest `tool_use` (by `timestamp`) in
`transcript_path` and in `<transcript without .jsonl>/subagents/*.jsonl`; `thread` is
`""` for the main transcript, else the file name without `.jsonl`. Without a readable
`tool_use`, `tool` comes from the message (`... to use <Tool>`), `path` and `thread`
are `""`. `at` is RFC 3339 UTC with milliseconds, as transcript lines are: the `tool_use`
behind the prompt is usually written in the same second, and an `at` cut to whole seconds
would lie before it. Exit 0 always; a failure goes to stderr. Only stdlib.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

KINDS = {"permission_prompt", "worker_permission_prompt"}
PATH_KEYS = ("file_path", "path", "notebook_path")
COMMAND_CHARS = 80
EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def _moment(entry: dict) -> datetime:
    stamp = entry.get("timestamp")
    if not isinstance(stamp, str):
        return EPOCH
    try:
        moment = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return EPOCH
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def _tool_path(name: str, tool_input: dict) -> str:
    if name == "Bash":
        command = tool_input.get("command")
        return command[:COMMAND_CHARS] if isinstance(command, str) else ""
    for key in PATH_KEYS:
        value = tool_input.get(key)
        if isinstance(value, str):
            return value
    return ""


def _newest_in(file: Path, thread: str, best: tuple | None) -> tuple | None:
    """The newest `tool_use` of `file`, if not older than `best`; a bad line is skipped."""
    try:
        text = file.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return best
    for line in text.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if not isinstance(entry, dict) or entry.get("type") != "assistant":
            continue
        message = entry.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            continue
        moment = _moment(entry)
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            tool_input = block.get("input")
            name = block.get("name")
            if not isinstance(name, str) or not isinstance(tool_input, dict):
                continue
            if best is None or moment >= best[0]:
                best = (moment, name, _tool_path(name, tool_input), thread)
    return best


def newest_tool_use(transcript: Path) -> tuple[str, str, str] | None:
    """`(tool, path, thread)` of the newest `tool_use`, or None."""
    best = _newest_in(transcript, "", None)
    subagents = transcript.with_suffix("") / "subagents"
    try:
        files = sorted(subagents.glob("*.jsonl"))
    except OSError:
        files = []
    for file in files:
        best = _newest_in(file, file.name.removesuffix(".jsonl"), best)
    return None if best is None else (best[1], best[2], best[3])


def build_note(payload: dict, now: datetime) -> dict:
    message = payload.get("message")
    message = message if isinstance(message, str) else ""
    found = None
    transcript = payload.get("transcript_path")
    if isinstance(transcript, str) and transcript:
        found = newest_tool_use(Path(transcript))
    if found is None:
        match = re.search(r"to use (\S+)", message)
        found = (match.group(1) if match else "", "", "")
    tool, path, thread = found
    return {
        "version": 1,
        "at": now.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "tool": tool,
        "path": path,
        "thread": thread,
        "message": message,
    }


def write_note(target: Path, note: dict) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=".permission-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(note, handle)
            handle.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, target)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("permission-check: COULD NOT CHECK — usage: note.py <state dir> <run id>", file=sys.stderr)
        return 0
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict) or payload.get("notification_type") not in KINDS:
            return 0
        note = build_note(payload, datetime.now(timezone.utc))
        write_note(Path(argv[1]) / "runs" / argv[2] / "permission.json", note)
    except Exception as err:  # fail open: a note that cannot be written never stops the session
        print(f"permission-check: COULD NOT CHECK — {err}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

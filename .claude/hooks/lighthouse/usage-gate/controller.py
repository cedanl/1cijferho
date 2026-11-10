# lighthouse: core@v0.8.1
"""Usage-gate module `controller` (`[usage_guard] controller = true`): the wording and the
resume marker of a controller run, which the controller resumes after the reset.

The guard decides whether to speak; this module supplies what is said and written. In
`tool` and `stop` it parks the run: one marker per session, with `"controller": true`, and
no handoff and no wake-up, because the controller resumes the session itself. Without a
`session_id` in the payload the run id is the session id: it is the session uuid, and
without a marker the controller never resumes the run. `prompt` only warns. `pause` parks
the run for the controller's PAUSE flag. The lib writes the keys, never this module.

The syntax stays within Python 3.9.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from lighthouse_lib.usage_guard import (
    Context,
    block,
    headline,
    marker,
    prompt_note,
    resume_dir,
    write_marker,
)

PARK = (
    "commit the work in progress, then stop: do not start a new step, do not dispatch a "
    "subagent, do not schedule a wake-up and do not write a handoff"
)


def _clock(until: str) -> str:
    """`HH:MM` of an ISO time with offset, as written; the raw text when it does not parse."""
    try:
        return datetime.fromisoformat(until.replace("Z", "+00:00")).strftime("%H:%M")
    except ValueError:
        return until


def _aware(text: object) -> Optional[datetime]:
    """An ISO time with offset as an aware datetime; None when it is none of that."""
    if not isinstance(text, str):
        return None
    try:
        at = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return at if at.tzinfo is not None else None


def _existing(session_id: str) -> Optional[dict]:
    """The marker of this session when it is a readable object with a valid `reset_at`."""
    try:
        data = json.loads((resume_dir() / f"{session_id}.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if isinstance(data, dict) and _aware(data.get("reset_at")) is not None:
        return data
    return None


def _park(ctx: Context, *, window: str, reset_at: str, source: str, **flags: bool) -> str:
    """Write the marker; the later resume moment wins over an existing marker of the session.

    A pause and an alarm can both hold: the run must resume after the later of the two,
    and a `pause` marker must keep its `pause` field. Returns the resume moment that won,
    as the clock time the reason names.
    """
    session_id = ctx.session_id or ctx.run_id
    fields = marker(ctx, session_id=session_id, window=window, reset_at=reset_at, source=source)
    clock = _clock(reset_at) if window == "pause" else ctx.alarm.reset_local
    old = _existing(session_id)
    if old is not None and _aware(old["reset_at"]) > _aware(reset_at):
        fields["window"] = old.get("window")
        fields["reset_at"] = old["reset_at"]
        flags = {"pause": True} if old.get("pause") is True else {}
        at = _aware(old["reset_at"])
        # A pause keeps the offset of its flag; an alarm's UTC time is shown local.
        clock = (at if old.get("window") == "pause" else at.astimezone()).strftime("%H:%M")
    write_marker(dict(fields, controller=True, **flags))
    return clock


def speak(ctx: Context, stand: str) -> dict:
    """`stand` is prompt, tool or stop."""
    assert ctx.alarm is not None
    if stand == "prompt":
        return prompt_note(
            f"{headline(ctx)} This controller run parks at the next tool call: finish only "
            "the current step and commit the work in progress."
        )
    clock = _park(ctx, window=ctx.alarm.kind, reset_at=ctx.alarm.reset_at, source=stand)
    return block(
        f"{headline(ctx)} Now {PARK}. The controller resumes this session after {clock}."
    )


def pause(ctx: Context) -> dict:
    """The controller's PAUSE flag is valid: park the run until the time in the flag."""
    clock = _park(ctx, window="pause", reset_at=ctx.pause_until, source="tool", pause=True)
    return block(
        f"The controller has paused this run until {_clock(ctx.pause_until)}. Now {PARK}. "
        f"The controller resumes this session after {clock}."
    )

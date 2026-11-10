# lighthouse: core@v0.8.1
"""Usage-gate module `resume` (`[usage_guard] resume = true`): the wording of an ordinary
session's alarm, plus the resume marker and the wake-up step.

The guard decides whether to speak; this module supplies what is said and written. In
`tool` and `stop` it writes one marker per session (only with a `session_id`). A
`venster` alarm also asks for a one-shot wake-up via `CronCreate`; a `week` alarm never.
`prompt` names the same steps but parks nothing, so it writes no marker.

The syntax stays within Python 3.9.
"""

from __future__ import annotations

from datetime import datetime, timedelta, tzinfo
from typing import Optional

from lighthouse_lib.usage_guard import (
    Context,
    base_speak,
    handoff_path,
    marker,
    write_marker,
)

WAKE_DELAY = timedelta(minutes=2)


def wake_cron(reset_at: str, tz: Optional[tzinfo] = None) -> str:
    """`reset_at` plus 2 minutes in `tz` (None: local) as `M H D Mon *`, no leading zeros."""
    at = datetime.fromisoformat(reset_at.replace("Z", "+00:00")) + WAKE_DELAY
    at = at.astimezone(tz)
    return f"{at.minute} {at.hour} {at.day} {at.month} *"


def _wake_step(ctx: Context) -> str:
    """A clause for `base_speak(wake=...)`."""
    assert ctx.alarm is not None
    cron = wake_cron(ctx.alarm.reset_at)
    return (
        f'schedule one wake-up with CronCreate (cron "{cron}", recurring false, prompt = '
        "the first step from the handoff); if you already set a wake-up in this window, "
        "check with CronList and take over that job ID instead of setting a second one"
    )


def speak(ctx: Context, stand: str) -> dict:
    """`stand` is prompt, tool or stop."""
    assert ctx.alarm is not None
    if ctx.alarm.kind == "venster":
        output = base_speak(
            ctx,
            stand,
            wake=_wake_step(ctx),
            note="Name the wake-up's job ID in the handoff.",
        )
    else:
        output = base_speak(ctx, stand)

    if stand != "prompt" and ctx.session_id:
        fields = marker(
            ctx,
            session_id=ctx.session_id,
            window=ctx.alarm.kind,
            reset_at=ctx.alarm.reset_at,
            source=stand,
        )
        write_marker(dict(fields, handoff_path=str(handoff_path(ctx))))
    return output

#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# Usage gate (#66): when the usage alarm of `lighthouse usage` is up, the session
# wraps up once per window instead of running into the limit. Thin wrapper; who
# speaks and what is said is in lighthouse_lib/usage_guard.py and the modules in
# usage-gate/ next to this file.
#
#   usage-gate.sh prompt    UserPromptSubmit: starts a detached
#                            `lighthouse usage refresh`; with an alarm a note
#                            as additionalContext, never a block.
#   usage-gate.sh tool      PostToolUse (empty matcher): with an alarm one
#                            block per window; in a controller run also the
#                            pause flag.
#   usage-gate.sh subagent  SubagentStop: with an alarm one block per subagent.
#   usage-gate.sh stop      Stop: with an alarm one block per window.
#
# The alarm is `${XDG_CACHE_HOME:-$HOME/.cache}/lighthouse/usage-window.alarm`,
# one line `<kind> <pct> <reset_local> <reset_at>`. It counts only as a plain
# file with exactly those four fields, kind venster or week, a whole pct and a
# reset_at in the future; anything else is no alarm. The guard never removes
# it: only the meter writes and prunes it.
#
# Fast path: tool and subagent run on every tool call. Without an alarm (and
# without a valid pause flag in a controller run) the cost is one `[ -f ]`: no
# fork, stdin unread. Only a valid alarm or flag starts python3.
#
# Fail-open by design: always exit 0, at most one JSON object on stdout. A
# usage gate that cannot check must not stop the work; it says so on stderr.
set -u

# 1. The stand. Inside the child session of a meter refresh the guard is silent.
STAND="${1:-}"
[ "${LIGHTHOUSE_USAGE_REFRESH:-}" = "1" ] && exit 0
case "$STAND" in
  prompt | tool | subagent | stop) ;;
  *) exit 0 ;;
esac

# 2. The alarm path, and the run id of a controller run (only in the shape of a uuid).
ALARM="${XDG_CACHE_HOME:-${HOME:-}/.cache}/lighthouse/usage-window.alarm"
RUN_ID="${LIGHTHOUSE_CONTROLLER_RUN:-}"
case "$RUN_ID" in
  "" | *[!0-9a-f-]*) RUN_ID="" ;;
esac

# 3. Refresh the meter, detached: stdin, stdout and stderr closed, so Claude
# does not wait for it and it does not eat the payload. The meter keeps its
# own TTL and lock.
if [ "$STAND" = "prompt" ] && command -v lighthouse >/dev/null 2>&1; then
  (LIGHTHOUSE_USAGE_REFRESH=1 lighthouse usage refresh </dev/null >/dev/null 2>&1 &)
fi

# 4. The pause flag of a controller run, only in tool: a plain file whose first
# line is a time with an offset, in the future. No python3 until it is valid.
PAUSE_UNTIL=""
PAUSE_FLAG=""
if [ -n "$RUN_ID" ] && [ "$STAND" = "tool" ]; then
  FLAG="${LIGHTHOUSE_CONTROLLER_STATE:-${XDG_STATE_HOME:-${HOME:-}/.local/state}/lighthouse/controller}/PAUSE"
  if [ -f "$FLAG" ] && [ ! -L "$FLAG" ]; then
    UNTIL=""
    { IFS= read -r UNTIL; } 2>/dev/null <"$FLAG" || true
    case "$UNTIL" in
      "" | *[!0-9T:+Z-]*) UNTIL="" ;;
      *T*[+-]* | *T*Z) ;;
      *) UNTIL="" ;;
    esac
    if [ -n "$UNTIL" ]; then
      # BSD date wants the offset without its colon, and +0000 for Z.
      BSD_UNTIL="${UNTIL%Z}"
      if [ "$BSD_UNTIL" != "$UNTIL" ]; then
        BSD_UNTIL="$BSD_UNTIL+0000"
      elif [[ "$UNTIL" == *[+-][0-9][0-9]:[0-9][0-9] ]]; then
        OFFSET="${UNTIL: -6}"
        BSD_UNTIL="${UNTIL%??????}${OFFSET%:*}${OFFSET#*:}"
      fi
      UNTIL_EPOCH="$(date -d "$UNTIL" +%s 2>/dev/null)" ||
        UNTIL_EPOCH="$(date -j -f '%Y-%m-%dT%H:%M:%S%z' "$BSD_UNTIL" +%s 2>/dev/null)" ||
        UNTIL_EPOCH=""
      NOW="$(date +%s)"
      case "$UNTIL_EPOCH" in
        "" | *[!0-9]*) ;;
        *)
          if [ "$UNTIL_EPOCH" -gt "$NOW" ]; then
            PAUSE_UNTIL="$UNTIL"
            PAUSE_FLAG="$FLAG"
          fi
          ;;
      esac
    fi
  fi
fi

# 5. The fast path. Everything above it forks nothing without a refresh or a flag.
if [ -z "$PAUSE_UNTIL" ]; then
  [ -f "$ALARM" ] || exit 0
fi

# 6. The alarm: the rules live in usage-gate/alarm.sh. The fast path above
# loads nothing; the helper is sourced only once a flag or alarm file exists.
case "${BASH_SOURCE[0]}" in
  */*) HOOKS_DIR="${BASH_SOURCE[0]%/*}" ;;
  *) HOOKS_DIR="." ;;
esac
if [ -f "$HOOKS_DIR/usage-gate/alarm.sh" ]; then
  # shellcheck source-path=SCRIPTDIR
  # shellcheck source=usage-gate/alarm.sh
  . "$HOOKS_DIR/usage-gate/alarm.sh"
else
  printf 'usage-gate: COULD NOT CHECK — usage-gate/alarm.sh is missing\n' >&2
  exit 0
fi
KIND=""
if usage_alarm_valid; then
  KIND="$USAGE_ALARM_KIND"
  PCT="$USAGE_ALARM_PCT"
  LOCAL="$USAGE_ALARM_LOCAL"
  AT="$USAGE_ALARM_AT"
fi

# 7. Neither a pause nor an alarm: done, stdin unread.
[ -n "$KIND" ] || [ -n "$PAUSE_UNTIL" ] || exit 0

# 8. The lib lives in the lighthouse lib of this repo. Found through
# LIGHTHOUSE_REPO_ROOT or git, like bash-write-check.sh.
LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git -C "$HOOKS_DIR" rev-parse --show-toplevel 2>/dev/null)}"
LIB="$LIB_ROOT/.lighthouse/lib/lighthouse_lib/usage_guard.py"
export LIGHTHOUSE_REPO_ROOT="$LIB_ROOT"

if [ -n "$KIND" ]; then
  VALID="the alarm is valid"
else
  VALID="the pause flag is valid"
fi
if ! command -v python3 >/dev/null 2>&1; then
  printf 'usage-gate: COULD NOT CHECK — python3 is missing; %s\n' "$VALID" >&2
  exit 0
fi
if [ ! -f "$LIB" ]; then
  printf 'usage-gate: COULD NOT CHECK — %s is missing; %s\n' \
    ".lighthouse/lib/lighthouse_lib/usage_guard.py" "$VALID" >&2
  exit 0
fi

# 9. One python3 with the payload on stdin. Arguments through `set --`: an
# empty array under `set -u` is an unbound variable in bash before 4.4.
set -- "$STAND" --hooks "$HOOKS_DIR"
if [ -n "$KIND" ]; then
  set -- "$@" --alarm "$KIND" "$PCT" "$LOCAL" "$AT"
fi
if [ -n "$PAUSE_UNTIL" ]; then
  set -- "$@" --pause "$PAUSE_UNTIL" "$PAUSE_FLAG"
fi
python3 "$LIB" "$@"
exit 0

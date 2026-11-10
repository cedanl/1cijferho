#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# Permission note (#115): in a controller run, a permission prompt of the session is
# written to `<LIGHTHOUSE_STATE_DIR>/runs/<run>/permission.json`, so the dispatcher can
# tell "waiting for a permission" from "silent". Thin wrapper; the work is in
# permission-check/note.py next to this file.
#
#   permission-check.sh    Notification (matcher permission_prompt|worker_permission_prompt)
#
# Fast path: outside a controller run (LIGHTHOUSE_CONTROLLER_RUN empty or not in the
# shape of a uuid) the cost is one `case`: no fork, stdin unread.
#
# Fail-open by design: always exit 0, never stdout. The hook does not refuse the
# prompt (a Notification hook cannot); a note that cannot be written says so on stderr.
set -u

RUN_ID="${LIGHTHOUSE_CONTROLLER_RUN:-}"
case "$RUN_ID" in
  "" | *[!0-9a-f-]*) exit 0 ;;
esac

STATE_DIR="${LIGHTHOUSE_STATE_DIR:-}"
if [ -z "$STATE_DIR" ]; then
  printf 'permission-check: COULD NOT CHECK — LIGHTHOUSE_STATE_DIR is empty\n' >&2
  exit 0
fi

case "${BASH_SOURCE[0]}" in
  */*) HOOKS_DIR="${BASH_SOURCE[0]%/*}" ;;
  *) HOOKS_DIR="." ;;
esac
NOTE="$HOOKS_DIR/permission-check/note.py"
if ! command -v python3 >/dev/null 2>&1; then
  printf 'permission-check: COULD NOT CHECK — python3 is missing\n' >&2
  exit 0
fi
if [ ! -f "$NOTE" ]; then
  printf 'permission-check: COULD NOT CHECK — permission-check/note.py is missing\n' >&2
  exit 0
fi

python3 "$NOTE" "$STATE_DIR" "$RUN_ID" >/dev/null || true
exit 0

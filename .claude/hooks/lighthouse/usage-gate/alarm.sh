# lighthouse: core@v0.8.1
# shellcheck shell=bash
# The rules of the usage alarm (#66, #74), sourced by usage-gate.sh and by the
# blocking Stop gates (go-lint-gate.sh, ty-gate.sh). Source it; do not run it.
#
# The alarm is `${XDG_CACHE_HOME:-$HOME/.cache}/lighthouse/usage-window.alarm`, one
# line `<kind> <pct> <reset_local> <reset_at>`. It counts only as a plain file (no
# symlink) with exactly those four fields, kind venster or week, a pct of digits
# only and a reset_at of the exact shape YYYY-MM-DDTHH:MM:SSZ that lies in the
# future. Anything else is no alarm. Nobody here removes it: only the meter writes
# and prunes it.
#
# Works with and without `set -u`. Calls no `exit`, turns `set -f` off after splitting
# (callers must not rely on `set -f`), and forks nothing when there is no alarm file.

USAGE_ALARM="${XDG_CACHE_HOME:-${HOME:-}/.cache}/lighthouse/usage-window.alarm"

# usage_alarm_valid: return 0 for a valid alarm and set USAGE_ALARM_KIND,
# USAGE_ALARM_PCT, USAGE_ALARM_LOCAL (`_` becomes a space) and USAGE_ALARM_AT;
# otherwise return 1 with those four empty. No arguments, no output.
usage_alarm_valid() {
  # shellcheck disable=SC2034 # read by the scripts that source this file
  USAGE_ALARM_KIND="" USAGE_ALARM_PCT="" USAGE_ALARM_LOCAL="" USAGE_ALARM_AT=""
  [ -f "$USAGE_ALARM" ] && [ ! -L "$USAGE_ALARM" ] || return 1
  local LINE="" AT_RE AT_EPOCH NOW
  { IFS= read -r LINE; } 2>/dev/null <"$USAGE_ALARM" || true
  set -f
  # shellcheck disable=SC2086 # split into fields on purpose, globbing is off
  set -- $LINE
  set +f
  AT_RE='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
  if [ $# -eq 4 ] && { [ "$1" = "venster" ] || [ "$1" = "week" ]; } &&
    [[ "$2" =~ ^[0-9]+$ ]] && [[ "$4" =~ $AT_RE ]]; then
    AT_EPOCH="$(date -u -d "$4" +%s 2>/dev/null)" ||
      AT_EPOCH="$(date -j -u -f '%Y-%m-%dT%H:%M:%SZ' "$4" +%s 2>/dev/null)" ||
      AT_EPOCH=""
    NOW="$(date +%s)"
    if [[ "$AT_EPOCH" =~ ^[0-9]+$ ]] && [ "$AT_EPOCH" -gt "$NOW" ]; then
      # shellcheck disable=SC2034 # read by the scripts that source this file
      USAGE_ALARM_KIND="$1" USAGE_ALARM_PCT="$2" USAGE_ALARM_LOCAL="${3//_/ }" USAGE_ALARM_AT="$4"
      return 0
    fi
  fi
  return 1
}

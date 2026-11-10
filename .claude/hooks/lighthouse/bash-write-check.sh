#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# Pre/PostToolUse hook on matcher Bash: the Edit|Write checks (shell-lint-check.sh,
# ruff-format-check.sh, code-language-check.sh for .py, .sh, .bash and .go, and
# md-link-check.sh) also see what a Bash call writes (#871). Thin wrapper; the logic is in
# lighthouse_lib/bash_writes.py.
#
# Counterpart: the norms of the checks carry their own counterparts up front
# (the shell and python packages, the link rule in AGENTS.md). This hook checks
# afterwards what a Bash call wrote. A check whose package is not installed is
# reported per file, without blocking (decision 19).
#
#   bash-write-check.sh pre   PreToolUse: baseline and marker. Always exit 0 - a
#                             PreToolUse exit 2 blocks the Bash call itself.
#   bash-write-check.sh post  PostToolUse and PostToolUseFailure: dirty files
#                             through their check. Exit 0 or 2.
#
# NOT fail-open: without python3, without bash_writes.py, or when the
# dispatcher ends with anything but 0 or 2, nothing was checked. That is exit 2
# with COULD NOT CHECK, to stderr and as additionalContext to stdout - the same
# output channel as the dispatcher itself. Pre stays silent by design: post
# reports it loudly.
set -u

MODE="${1:-}"

# The lib lives in the lighthouse lib of this repo. Found through
# LIGHTHOUSE_REPO_ROOT or git, never from this file's own path; bash_writes.py
# looks for the checks under the same root.
LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel 2>/dev/null)}"
LIB="$LIB_ROOT/.lighthouse/lib/lighthouse_lib/bash_writes.py"
export LIGHTHOUSE_REPO_ROOT="$LIB_ROOT"

# First into a variable: the payload goes to python3, and on a wrapper error
# the event can still be read from it without python.
INPUT="$(cat)"

loud() { # message - fixed text plus a number, so no JSON escaping needed
  local event
  event="$(printf '%s\n' "$INPUT" |
    sed -n 's/.*"hook_event_name" *: *"\([A-Za-z]*\)".*/\1/p')"
  event="${event%%$'\n'*}"
  [ -n "$event" ] || event="PostToolUse"
  printf '%s\n' "$1" >&2
  printf '{"hookSpecificOutput":{"hookEventName":"%s","additionalContext":"%s"}}\n' \
    "$event" "$1"
  exit 2
}

case "$MODE" in
  pre)
    if command -v python3 >/dev/null 2>&1 && [ -f "$LIB" ]; then
      printf '%s' "$INPUT" | python3 "$LIB" pre >/dev/null 2>&1
    fi
    exit 0
    ;;
  post)
    command -v python3 >/dev/null 2>&1 ||
      loud "bash-write-check: COULD NOT CHECK — python3 is missing"
    # Without the lib python3 ends with rc 2: that would pass as a verdict.
    [ -f "$LIB" ] ||
      loud "bash-write-check: COULD NOT CHECK — .lighthouse/lib/lighthouse_lib/bash_writes.py is missing"
    out="$(printf '%s' "$INPUT" | python3 "$LIB" post)"
    rc=$?
    case "$rc" in
      0 | 2)
        [ -z "$out" ] || printf '%s\n' "$out"
        exit "$rc"
        ;;
      *)
        loud "bash-write-check: COULD NOT CHECK — dispatcher failed (rc=$rc)"
        ;;
    esac
    ;;
  *)
    loud "bash-write-check: COULD NOT CHECK — unknown mode, expected pre or post"
    ;;
esac

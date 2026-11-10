#!/bin/bash
# lighthouse: core@v0.8.1
# PreToolUse hook: check the body of every `gh pr create` (#1091).
# Matcher: Bash. Blocking: exit 2 + stderr. Passed: exit 0, notes as JSON on
# stdout.
#
# Counterpart: /lighthouse-write-pr
# The skill carries the norm up front (build the body with Closes evidence);
# this hook enforces it at the moment of `gh pr create`: a body without
# `Closes #N` + Evidence table, `Refs #N — <reason>` or `No issue: <reason>`
# blocks. The check itself is in lighthouse_lib/pr_body_gate.py.
#
# NOT fail-open: without tooling, with a command that cannot be parsed, or with
# a parser that breaks, nothing was checked — that is no green light and blocks
# with COULD NOT CHECK. The only loud pass is the AC count (gh missing or
# broken): the lib reports it on stdout and the hook passes the note on to user
# and model.
#
# The lib deadline (DEADLINE_S = 12 s in pr_body_gate.py) must stay under the
# hook timeout (15 s in hooks.json): a hook that times out lets the call pass.

HOOK_NAME="pr-create-gate"

MISSING_TOOLS=""
command -v jq >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS jq"
command -v python3 >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS python3"
command -v git >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS git"
if [ -n "$MISSING_TOOLS" ]; then
  {
    echo "$HOOK_NAME: COULD NOT CHECK — missing tooling:$MISSING_TOOLS"
    echo ""
    echo "This hook checks the body of every 'gh pr create'."
    echo "Without that tooling nothing was checked; that is not a green light."
  } >&2
  exit 2
fi

# The lib lives in the lighthouse lib of this repo. Found through
# LIGHTHOUSE_REPO_ROOT or git, never from this file's own path.
LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null)}"
LIB="$LIB_ROOT/.lighthouse/lib/lighthouse_lib"

INPUT=$(cat)
if ! CMD=$(printf '%s' "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null); then
  {
    echo "$HOOK_NAME: COULD NOT CHECK — the payload is not valid JSON."
    echo ""
    echo "Without the command nothing was checked; that is not a green light."
  } >&2
  exit 2
fi
if [ -z "$CMD" ]; then
  exit 0
fi
CWD=$(printf '%s' "$INPUT" | jq -r '.cwd // empty')
[ -n "$CWD" ] || CWD="$PWD"

block() {
  {
    echo "$HOOK_NAME: COULD NOT CHECK — $1"
    echo ""
    echo "This command may contain a 'gh pr create'. 'Could not check' is"
    echo "not a green light, so this does not pass silently."
  } >&2
  exit 2
}

# No lib (a `lib/` in .gitignore leaves `.lighthouse/lib/` out of a clone):
# the two-step rule below, so a missing lib does not lock up every command.
if [ ! -f "$LIB/pr_body_gate.py" ]; then
  case "$CMD" in
    *gh*) ;;
    *) exit 0 ;;
  esac
  case "$CMD" in
    *pr*) ;;
    *) exit 0 ;;
  esac
  block "the lighthouse lib is missing ($LIB). Run 'lighthouse init --upgrade'."
fi

# The stderr of the lib (the block text) passes through unfiltered.
OUT=$(printf '%s' "$CMD" | LIGHTHOUSE_REPO_ROOT="$LIB_ROOT" python3 "$LIB/pr_body_gate.py" "$CWD")
STATUS=$?

case "$STATUS" in
  0)
    if [ -n "$OUT" ]; then
      # additionalContext reaches the model, systemMessage the user.
      jq -cn --arg m "$OUT" '{systemMessage: $m, hookSpecificOutput: {hookEventName: "PreToolUse", additionalContext: $m}}'
    fi
    exit 0
    ;;
  2)
    exit 2
    ;;
  3)
    # Two-step rule: without `gh` and `pr` in the raw text there is nothing to
    # check, and one stray apostrophe locks nothing up.
    case "$CMD" in
      *gh*) ;;
      *) exit 0 ;;
    esac
    case "$CMD" in
      *pr*) ;;
      *) exit 0 ;;
    esac
    block "the command cannot be parsed (open quote). Split the command."
    ;;
  *)
    block "the body parser failed (rc $STATUS)."
    ;;
esac

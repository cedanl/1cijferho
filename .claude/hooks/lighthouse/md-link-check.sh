#!/bin/bash
# lighthouse: core@v0.8.1
# PostToolUse hook: markdown links must be relative and must exist (#886).
# Matcher: Edit|Write, filters itself on .md. Blocking: exit 2 + stderr.
#
# Counterpart: the link rule in AGENTS.md carries norm and rationale up
# front; this hook checks the written file afterwards.
#
# Norm (#886, AC 2): a relative markdown link must exist, resolved against
# the directory of the file — everywhere, also a link to a file that only
# exists on this branch (a plan that links to a spec committed just now is no
# problem). An absolute link is always a problem, whether it exists or not:
# dead on the rendered site and as soon as the worktree is gone.
#
# The name fits: the hook checks markdown links after a write. The chat half of
# the AGENTS.md rule "Links: file relative, chat absolute" has no hook: it is
# about prose in a chat answer, not about a file this hook sees.
#
# Deliberately no project filter, unlike commit-test-gate.sh (#1014): that gate
# filters because it runs the suite of this repo. This hook checks a file
# against the root of its own repo; its verdict holds in every repo, and a
# filter let a dead link pass silently there. A directory outside every git
# repo has no root to resolve against and falls outside the check.
#
# NOT fail-open: without jq, git or python3, with a payload that is not JSON,
# a path that cannot be resolved, a git that breaks, or a link parser that
# fails, nothing was checked; a silent exit 0 would read as "all links are
# fine". Every such exit is exit 2 with `Linkcheck: COULD NOT CHECK — <reason>`
# on the first line (the token bash_writes.classify reads, decision 18).

cannot_check() {
  {
    echo "Linkcheck: COULD NOT CHECK — $1"
    echo ""
    echo "This hook checks that markdown links are relative and exist."
    echo "Nothing was checked; that is not a green light."
    if [ -n "${2:-}" ]; then
      echo ""
      echo "$2"
    fi
  } >&2
  exit 2
}

MISSING_TOOLS=""
command -v jq >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS jq"
command -v git >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS git"
command -v python3 >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS python3"
if [ -n "$MISSING_TOOLS" ]; then
  cannot_check "missing tooling:$MISSING_TOOLS"
fi

# `realpath -m` only exists in GNU coreutils; the BSD realpath on macOS
# rejects the flag. The flag is needed because the target does not have to
# exist yet.
canon_path() {
  realpath -m "$1" 2>/dev/null && return 0
  python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$1" 2>/dev/null
}

INPUT=$(cat)
if ! FILE_PATH=$(printf '%s' "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null); then
  cannot_check "the payload is not valid JSON."
fi
if [ -z "$FILE_PATH" ]; then
  exit 0
fi

case "$FILE_PATH" in
  *.md|*.markdown) ;;
  *) exit 0 ;;
esac

CWD=$(printf '%s' "$INPUT" | jq -r '.cwd // empty')
case "$FILE_PATH" in
  /*) ABS="$FILE_PATH" ;;
  *)  ABS="${CWD:-$PWD}/$FILE_PATH" ;;
esac
CANON=$(canon_path "$ABS") || CANON=""
if [ -z "$CANON" ]; then
  cannot_check "cannot resolve '$ABS' (no 'realpath -m' and no python3)."
fi
ABS="$CANON"
if [ ! -f "$ABS" ]; then
  exit 0
fi

# "Not a git repository" is a real outcome and stays silent; any other git
# failure is not. LC_ALL=C: git translates the text matched here.
DIR=$(dirname "$ABS")
if ! ROOT=$(LC_ALL=C git -C "$DIR" rev-parse --show-toplevel 2>&1); then
  case "$ROOT" in
    *"not a git repository"*) exit 0 ;;
  esac
  cannot_check "git could not read '$DIR'." "$ROOT"
fi
if [ -z "$ROOT" ]; then
  cannot_check "git gave no repository root for '$DIR'."
fi

# Proof that this code path was reached. Without the marker a green test does
# not show whether the hook looked or stepped out early.
if [ -n "${LIGHTHOUSE_LINKCHECK_TRACE:-}" ]; then
  echo "linkcheck: checking in $ROOT" >&2
fi

# The parser lives in the lighthouse lib of this repo. Found through
# LIGHTHOUSE_REPO_ROOT or git, never from this file's own path. No `|| exit 0`
# and no `2>/dev/null`: a parser that breaks must not read as "no problems".
LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null)}"
if ! PROBLEMS=$(python3 "$LIB_ROOT/.lighthouse/lib/lighthouse_lib/md_links.py" "$ABS" "$ROOT" 2>&1); then
  cannot_check "the link parser failed." "$PROBLEMS"
fi
if [ -z "$PROBLEMS" ]; then
  exit 0
fi

MESSAGE=""
while IFS=$'\t' read -r KIND TARGET; do
  [ -n "$KIND" ] || continue
  case "$KIND" in
    dead)
      REASON="does not exist, resolved against the directory of the file"
      ;;
    absolute)
      REASON="an absolute path is dead on the rendered site and as soon as the worktree is gone; use a relative path"
      ;;
    *)
      REASON="unknown kind of problem"
      ;;
  esac
  MESSAGE="$MESSAGE  $TARGET — $REASON"$'\n'
done <<< "$PROBLEMS"

{
  echo "Linkcheck: markdown link(s) that are not relative-and-existing."
  echo ""
  printf '%s' "$MESSAGE"
  echo "In: $ABS"
} >&2
exit 2

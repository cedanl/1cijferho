#!/usr/bin/env bash
# lighthouse: shell@v0.8.1
# PostToolUse hook: shellcheck on every edited shell script.
# Matcher: Edit|Write. Counterpart of the shell-standard skill.
#
# Why this hook is NOT fail-open: skipping silently is itself the error the
# hook must prevent. shellcheck is pinned, sha256-verified and lives in the
# shared tool cache, so "I could not look" is a defect and not a normal
# state. Missing tool = fail loudly (exit 2, first line
# `shell-lint-check: COULD NOT CHECK — …`), never exit 0.
#
# Two blocking thresholds, deliberately two different sets:
#   - the directories in `shell.strict_dirs` (default `.claude/hooks`): everything,
#     down to style.
#   - all other .sh: error + warning. Info and style findings in the rest
#     of a repo would block every edit on debt that has nothing to do with
#     the change.
set -uo pipefail

fail() { # reason...
  echo "shell-lint-check: COULD NOT CHECK — $*" >&2
  exit 2
}

INPUT=$(cat)

# Two parsers, because this hook runs on EVERY Edit/Write - also on .py and .md.
# Failing loudly because jq is missing would then block the whole session on
# something that has nothing to do with shell. Picking JSON apart with grep
# yourself is the other side of the same error, so: jq, else python3, and only
# when both are missing fail loudly.
if command -v jq >/dev/null 2>&1; then
  FILE_PATH=$(printf '%s' "$INPUT" | jq -r '.tool_input.file_path // empty')
elif command -v python3 >/dev/null 2>&1; then
  FILE_PATH=$(printf '%s' "$INPUT" |
    python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))')
else
  fail "neither jq nor python3 is present, so the edited path cannot be read."
fi

# No path in the payload: this tool call touched no file. Nothing to do.
[ -n "$FILE_PATH" ] || exit 0

case "$FILE_PATH" in
  *.sh|*.bash) ;;
  *) exit 0 ;;
esac

# Deleted or renamed between the edit and the hook: nothing to check.
[ -f "$FILE_PATH" ] || exit 0

# The lighthouse root: LIGHTHOUSE_REPO_ROOT, else the tree of the EDITED
# file, and only then that of the hook. $CLAUDE_PROJECT_DIR points to the
# main checkout, and an edit in a worktree would then be checked with the
# script of another branch.
FILE_DIR="$(dirname "$FILE_PATH")"
FILE_ROOT="$(git -C "$FILE_DIR" rev-parse --show-toplevel 2>/dev/null)" || FILE_ROOT=""
candidates=()
if [ -n "${LIGHTHOUSE_REPO_ROOT:-}" ]; then
  candidates+=("$LIGHTHOUSE_REPO_ROOT")
else
  [ -z "$FILE_ROOT" ] || candidates+=("$FILE_ROOT")
  if hook_root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null)"; then
    candidates+=("$hook_root")
  fi
fi

ROOT=""
searched=""
for root in ${candidates[@]+"${candidates[@]}"}; do
  searched="$searched $root"
  if [ -x "$root/.lighthouse/scripts/shellcheck-repo.sh" ]; then
    ROOT="$root"
    break
  fi
done
if [ -z "$ROOT" ]; then
  fail "no .lighthouse/scripts/shellcheck-repo.sh found (searched:${searched:- no lighthouse root})."
fi
SCRIPT="$ROOT/.lighthouse/scripts/shellcheck-repo.sh"

# The strict directories are repo-relative: compare the path within the git tree
# of the file, else within the lighthouse root.
if ! STRICT_DIRS=$(LIGHTHOUSE_REPO_ROOT="$ROOT" python3 "$ROOT/.lighthouse/lib/lighthouse_lib/config.py" shell.strict_dirs 2>&1); then
  fail "shell.strict_dirs could not be read: ${STRICT_DIRS:-no message}"
fi
ABS="$(cd "$FILE_DIR" && pwd -P)/${FILE_PATH##*/}"
REL=""
for base in "$FILE_ROOT" "$ROOT"; do
  [ -n "$base" ] || continue
  base="$(cd "$base" && pwd -P)"
  case "$ABS" in
    "$base"/*) REL="${ABS#"$base"/}"; break ;;
  esac
done

severity=warning
while IFS= read -r dir; do
  dir="${dir%/}"
  [ -n "$dir" ] && [ -n "$REL" ] || continue
  case "$REL" in
    "$dir"/*) severity=style; break ;;
  esac
done <<EOF
$STRICT_DIRS
EOF

# stderr separately: a deviating shellcheck version reports itself there, also at rc 0.
ERR_FILE="$(mktemp)" || fail "mktemp failed."
out=$("$SCRIPT" --severity "$severity" -- "$FILE_PATH" 2>"$ERR_FILE")
rc=$?
err="$(cat "$ERR_FILE")"
rm -f "$ERR_FILE"

case "$rc" in
  0)
    # Clean, but possibly with a different version than the pin: not silent.
    if [ -n "$err" ]; then
      msg="shell-lint-check: $err"
      if command -v jq >/dev/null 2>&1; then
        jq -cn --arg m "$msg" '{systemMessage: $m}'
      else
        MSG="$msg" python3 -c 'import json,os; print(json.dumps({"systemMessage": os.environ["MSG"]}))'
      fi
    fi
    exit 0
    ;;
  1)
    echo "shell-lint-check: fix this in the code. If a rule really has to go, give a reason:" >&2
    echo "'# shellcheck disable=SCxxxx <reason>' on the line above." >&2
    echo "No silent suppression." >&2
    echo "" >&2
    [ -z "$err" ] || echo "$err" >&2
    echo "$out" >&2
    exit 2
    ;;
  *)
    fail "shellcheck-repo.sh returned rc=$rc, nothing was checked:" "$err" "$out"
    ;;
esac

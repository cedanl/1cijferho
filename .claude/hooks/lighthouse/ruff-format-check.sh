#!/bin/bash
# lighthouse: python@v0.8.1
# PostToolUse hook: auto-run ruff format + check --fix on edited Python files.
# Matcher: Edit|Write — runs after every file edit.
# Pass 1 (format + --fix) is non-blocking and silent. Pass 2 blocks (exit 2) on
# the eight BLOCKING_RULES below — repo-wide zero, so they can't false-positive
# on legacy code.
#
# NOT fail-open: without jq, with a payload that is not JSON, without uvx and
# ruff, or with a ruff that does not start, nothing was checked; a silent exit 0
# would read as "the file is clean". Every such exit is exit 2 with
# `ruff-format-check: COULD NOT CHECK — <reason>` on the first line (the token
# bash_writes.classify reads, decision 18). No file path, another extension or a
# file that does not exist leave nothing to check: exit 0.

HOOK_NAME="ruff-format-check"

cannot_check() {
  {
    echo "$HOOK_NAME: COULD NOT CHECK — $1"
    echo ""
    echo "This hook formats the Python file and blocks on the blocking ruff rules."
    echo "Nothing was checked; that is not a green light."
  } >&2
  exit 2
}

if ! command -v jq >/dev/null 2>&1; then
  cannot_check "jq is missing."
fi

INPUT=$(cat)
# jq reads an empty stdin as no input at all and exits 0: that is no payload.
[ -n "${INPUT//[[:space:]]/}" ] || cannot_check "the payload is empty."
if ! FILE_PATH=$(printf '%s' "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null); then
  cannot_check "the payload is not valid JSON."
fi

if [ -z "$FILE_PATH" ]; then
  exit 0
fi

# Only process Python files
case "$FILE_PATH" in
  *.py) ;;
  *) exit 0 ;;
esac

if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

# The ruff version lives in one place: .ruff-version in the repository root. The
# ratchet (.lighthouse/scripts/quality/ratchet.py) reads the same file. Without
# that file the hook runs ruff unpinned: pinning is a project choice.
ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null)}"
RUFF_VERSION=""
if [ -n "$ROOT" ] && [ -f "$ROOT/.ruff-version" ]; then
  RUFF_VERSION="$(tr -d '[:space:]' < "$ROOT/.ruff-version")"
fi

# Prefer uvx ruff (not in project venv), fall back to ruff
if command -v uvx >/dev/null 2>&1; then
  if [ -n "$RUFF_VERSION" ]; then
    RUFF="uvx ruff@$RUFF_VERSION"
  else
    RUFF="uvx ruff"
  fi
elif command -v ruff >/dev/null 2>&1; then
  RUFF="ruff"
else
  cannot_check "neither uvx nor ruff is on the PATH."
fi

# `uvx ruff check` returns the same exit 1 for "found violations" and for
# "could not resolve the tool" — the exit code alone can't tell them apart.
# Probe separately: a tool that can't even start is not a verdict on the code.
if ! PROBE=$($RUFF --version 2>&1); then
  cannot_check "$RUFF does not start (offline, or the pin does not resolve): $(printf '%s' "$PROBE" | head -n 3)"
fi

# Pass 1 — best effort, silent: format + everything autofixable. Its output goes
# nowhere, so the first line of this hook is always its own.
# `--force-exclude` on all three calls: ruff ignores `exclude` for a path named
# explicitly, and `.lighthouse/` is package code that `lighthouse init --upgrade`
# refuses once it differs locally.
$RUFF format --force-exclude "$FILE_PATH" >/dev/null 2>&1 || true
$RUFF check --force-exclude --fix "$FILE_PATH" >/dev/null 2>&1 || true

# Pass 2 — blocking, on rules that are at zero repository-wide. On purpose NOT
# the full selection: a project with legacy hits would see every edit in an
# existing file blocked. These eight cannot be false positives on legacy code,
# and PGH003/PGH004 close exactly the escape route (a bare `# type: ignore` /
# bare `# noqa` instead of fixing the error).
#
# Keep naming the list here explicitly, also when pyproject.toml selects more:
# the linted set and the blocking set are two different sets on purpose. The KPI
# row of the python-standard skill names the same eight.
BLOCKING_RULES="E722,B006,F403,F811,B023,PT012,PGH003,PGH004"

# ruff exits 1 on findings and 2 when it could not lint (a broken [tool.ruff], a
# bad argument): only 1 is a verdict on the code.
out=$($RUFF check --force-exclude --select "$BLOCKING_RULES" "$FILE_PATH" 2>&1)
rc=$?
if [ "$rc" -eq 1 ]; then
  echo "$HOOK_NAME: fix this in the code — do not relax a rule and do not paste a" >&2
  echo "# noqa / # type: ignore over it. If you really want a rule off, ask." >&2
  echo "" >&2
  echo "$out" >&2
  exit 2
fi
if [ "$rc" -ne 0 ]; then
  cannot_check "ruff exited $rc (a broken [tool.ruff] in pyproject.toml or ruff.toml?):
$out"
fi

exit 0

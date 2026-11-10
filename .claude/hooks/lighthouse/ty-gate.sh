#!/bin/bash
# lighthouse: python@v0.8.1
# Stop hook: run `ty check` on the directories in python.source_dirs at the end
# of a turn.
# Matcher: "" — runs on every Stop.
# Blocking: exit 2 with stderr sends the diagnostics back to the model, which
# fixes them in the same session with the context still warm.
#
# Deliberately NOT a PostToolUse hook: a type error is often only judgeable once
# several files are done, so per-edit feedback would burn turns on half-finished
# state.
#
# Looks at every checkout the turn wrote in (turn_roots.py, #54), not the `cwd` of
# the payload: that is the shell's directory at Stop, and a session in the main
# checkout that edits a worktree stops there. It checks only a checkout in which
# the turn touched a Python path: a `.py` or `.pyi` file in a source dir, or
# pyproject.toml, ty.toml or uv.lock (an edit, a Bash write or a commit in the turn,
# `turn_roots.py --match`, #80). The rest gets `not checked`.
#
# Could not check (no jq or uvx, no repository, no venv, a ty that does not
# start, a configuration that cannot be read, a turn whose checkout cannot be
# determined): exit 0 with a systemMessage `ty-gate: COULD NOT CHECK — <reason>`
# (design choice 13). Blocking there would keep the session from stopping; staying
# silent would read as "type-clean".
# While the usage alarm is up (#74) the gate says `not checked` and exits 0: the
# usage gate has the session wrap up, and fixing a diagnostic costs budget that is
# no longer there. Without `usage-gate/alarm.sh` next to this file (no pack/core)
# there is no alarm to read and the check runs as before.
# `python.source_dirs = []` turns the check off, without a message, alarm or not: the
# alarm is read after that exit.

# The pin of ty. The python-standard skill names the same version.
TY_VERSION="0.0.72"
HOOK_NAME="ty-gate"

# A message to the user, without blocking. Without jq the JSON is built by hand:
# the reasons below hold no quote or backslash.
report() {
  if command -v jq >/dev/null 2>&1; then
    jq -cn --arg m "$1" '{systemMessage: $m}'
  else
    printf '{"systemMessage":"%s"}\n' "$1"
  fi
  exit 0
}

cannot_check() {
  report "$HOOK_NAME: COULD NOT CHECK — $1 Nothing was type-checked; that is not a green light."
}

# uvx is checked per checkout, after the skip: an untouched root needs none.
for tool in jq git python3; do
  command -v "$tool" >/dev/null 2>&1 || cannot_check "$tool is missing."
done

INPUT=$(cat)
if ! ACTIVE=$(printf '%s' "$INPUT" | jq -r '.stop_hook_active // false' 2>/dev/null); then
  cannot_check "the payload is not valid JSON."
fi

# Claude Code re-invokes the model when a Stop hook blocks. Without this guard
# a failing check would loop forever.
if [ "$ACTIVE" = "true" ]; then
  exit 0
fi

# The lib lives in the lighthouse lib of this repo. Found through
# LIGHTHOUSE_REPO_ROOT or git, never from this file's own path.
LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null)}"
LIB="$LIB_ROOT/.lighthouse/lib/lighthouse_lib"

if ! DIRS_RAW=$(LIGHTHOUSE_REPO_ROOT="$LIB_ROOT" python3 "$LIB/config.py" python.source_dirs 2>&1); then
  # \134 is the backslash in octal: a literal one at the end of the set reads as an escaped quote.
  cannot_check "the configuration cannot be read (python.source_dirs): $(printf '%s' "$DIRS_RAW" | tr '\n"\134' "   ")."
fi

# An empty list turns the check off.
[ -z "$DIRS_RAW" ] && exit 0

# What makes a path a Python path (fnmatch on the path relative to the root; `*`
# also matches `/`): `.py` and `.pyi` files in a source dir, and the project files.
MATCH_ARGS=(--match pyproject.toml --match ty.toml --match uv.lock)
while IFS= read -r dir; do
  [ -n "$dir" ] || continue
  dir="${dir#./}"
  dir="${dir%/}"
  if [ -z "$dir" ] || [ "$dir" = "." ]; then
    MATCH_ARGS+=(--match '*.py' --match '*.pyi')
  else
    MATCH_ARGS+=(--match "$dir/*.py" --match "$dir/*.pyi")
  fi
done <<EOF
$DIRS_RAW
EOF

# The checkouts this turn wrote in, one per line: `touched<TAB>root` or
# `untouched<TAB>root` (#54, #80).
if ! ROOTS=$(printf '%s' "$INPUT" | LIGHTHOUSE_REPO_ROOT="$LIB_ROOT" python3 "$LIB/turn_roots.py" "${MATCH_ARGS[@]}" 2>&1); then
  # \134 is the backslash in octal: a literal one at the end of the set reads as an escaped quote.
  cannot_check "the checkout of this turn cannot be determined: $(printf '%s' "$ROOTS" | tr '\n"\134' "   ")"
fi

# The usage alarm (#74): the rules live in usage-gate/alarm.sh, installed by
# pack/core next to this file. A missing helper or an invalid alarm checks as usual.
case "${BASH_SOURCE[0]}" in
  */*) HOOKS_DIR="${BASH_SOURCE[0]%/*}" ;;
  *) HOOKS_DIR="." ;;
esac
# shellcheck source=/dev/null
if [ -f "$HOOKS_DIR/usage-gate/alarm.sh" ] && . "$HOOKS_DIR/usage-gate/alarm.sh" && usage_alarm_valid; then
  report "$HOOK_NAME: not checked — the usage alarm is up until $USAGE_ALARM_LOCAL; the usage gate wraps up this session."
fi

# One checkout, in a subshell: `report` and `cannot_check` end only that one.
check_root() (
  ROOT="$1"
  VERDICT="$2"
  cd "$ROOT" 2>/dev/null || cannot_check "the checkout $ROOT does not exist."

  if [ "$VERDICT" = "untouched" ]; then
    report "$HOOK_NAME: not checked — the turn touched no Python path in $ROOT."
  fi

  command -v uvx >/dev/null 2>&1 || cannot_check "uvx is missing."

  # The directories that exist; one per line in the config output.
  DIRS=()
  while IFS= read -r dir; do
    [ -n "$dir" ] && [ -d "$dir" ] && DIRS+=("$dir")
  done <<EOF
$DIRS_RAW
EOF
  if [ "${#DIRS[@]}" -eq 0 ]; then
    report "$HOOK_NAME: not checked — no directory from python.source_dirs exists in $ROOT (set it in lighthouse.toml; [] turns the check off)"
  fi
  WHERE="${DIRS[*]} of $ROOT"

  # ty needs a venv to resolve third-party imports. Without one every import falls
  # over. A fresh worktree has no .venv until the first `uv run`, so fall back to
  # the main checkout's.
  VENV=""
  if [ -d "$ROOT/.venv" ]; then
    VENV="$ROOT/.venv"
  elif [ -n "$CLAUDE_PROJECT_DIR" ] && [ -d "$CLAUDE_PROJECT_DIR/.venv" ]; then
    VENV="$CLAUDE_PROJECT_DIR/.venv"
  else
    cannot_check "no .venv in $ROOT or the main checkout; run uv sync first."
  fi

  # `uvx ty@version check` returns the same exit 1 for "found diagnostics" and for
  # "could not resolve the tool" — the exit code alone can't tell them apart.
  # Probe separately: a tool that can't even start is not a verdict on the code.
  if ! uvx "ty@$TY_VERSION" --version >/dev/null 2>&1; then
    cannot_check "ty@$TY_VERSION does not start (offline, or the pin does not resolve)."
  fi

  # `--color never` pins the format the code recognition below anchors on. Color
  # puts an ANSI escape before `error[`, and then the line anchor no longer
  # matches.
  # ty exits 1 on diagnostics and 2 when it could not check (an invalid [tool.ty], a
  # bad argument): only 1 is a verdict on the code.
  OUT=$(uvx "ty@$TY_VERSION" check --color never --python "$VENV" "${DIRS[@]}" 2>&1)
  TY_RC=$?
  if [ "$TY_RC" -ne 0 ] && [ "$TY_RC" -ne 1 ]; then
    cannot_check "ty exited $TY_RC in $ROOT (an invalid [tool.ty] in pyproject.toml or ty.toml?): $(printf '%s' "$OUT" | head -5 | tr '\n' ' ')"
  fi
  if [ "$TY_RC" -eq 1 ]; then
    # Which rule codes came out? ty puts them at the line start as
    # `error[<code>]:` or `warning[<code>]:`. Empty = a failure without
    # diagnostics; that falls through to the plain block.
    CODES=$(printf '%s\n' "$OUT" |
      sed -nE 's/^(error|warning)\[([a-z0-9-]+)\].*/\2/p' | sort -u)

    # A set of only `unresolved-import` points to the environment rather than the
    # code: a venv behind on uv.lock drops a package that is simply in
    # [project].dependencies. "Fix them in the code" sends the reader down the
    # wrong track then.
    #
    # Deliberately exit 2 and not a message: `unresolved-import` is also exactly
    # the signal that someone added an import without declaring the dependency.
    # That must not drop silently. Both branches are actionable at once (run
    # `uv sync`, or add the dependency). It cannot loop: the stop_hook_active
    # guard at the top lets the hook fire once per turn.
    if [ "$CODES" = "unresolved-import" ]; then
      echo "$HOOK_NAME: ty finds only unresolvable imports in $WHERE, no type errors." >&2
      echo "That points to a skewed environment rather than wrong code." >&2
      echo "" >&2
      echo "Check your venv first:  uv sync  (with the extras you use)." >&2
      echo "If the venv is right and the import stays unresolvable, the" >&2
      echo "dependency is missing in pyproject.toml — add it there. Do not" >&2
      echo "remove the import from the code." >&2
      echo "" >&2
      echo "$OUT" >&2
      exit 2
    fi

    echo "$HOOK_NAME: ty finds errors in $WHERE. Fix them in the code — do not" >&2
    echo "turn off a ty rule and do not paste a bare # type: ignore over it." >&2
    echo "" >&2
    echo "$OUT" >&2
    exit 2
  fi
  exit 0
)

# Every checkout gets its check. A finding in one blocks; a message from another
# still reaches the reader.
ERRS=$(mktemp) || cannot_check "mktemp failed."
trap 'rm -f "$ERRS"' EXIT
MESSAGES=""
BLOCKED=0
TAB=$(printf '\t')
while IFS= read -r line; do
  [ -n "$line" ] || continue
  verdict="${line%%"$TAB"*}"
  root="${line#*"$TAB"}"
  MSG=$(check_root "$root" "$verdict" 2>>"$ERRS")
  RC=$?
  if [ "$RC" -eq 2 ]; then
    BLOCKED=1
  elif [ "$RC" -ne 0 ]; then
    MSG=$(jq -cn --arg m "$HOOK_NAME: COULD NOT CHECK — the check of $root ended with exit $RC." '{systemMessage: $m}')
  fi
  if [ -n "$MSG" ]; then
    MESSAGES="${MESSAGES:+$MESSAGES
}$(printf '%s' "$MSG" | jq -r '.systemMessage')"
  fi
done <<EOF
$ROOTS
EOF

if [ "$BLOCKED" -eq 1 ]; then
  cat "$ERRS" >&2
  [ -n "$MESSAGES" ] && printf '\n%s\n' "$MESSAGES" >&2
  exit 2
fi
[ -n "$MESSAGES" ] && report "$MESSAGES"
exit 0

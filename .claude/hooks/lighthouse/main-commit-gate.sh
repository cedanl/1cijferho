#!/bin/bash
# lighthouse: core@v0.8.1
# PreToolUse hook: block `git commit` and `git push` on the default branch:
# `forge.default_branch` from lighthouse.toml, `main` when it is not set.
# Matcher: Bash. Blocking: exit 2 + stderr.
#
# Counterpart: the rule "nothing lands directly on main" in AGENTS.md carries
# the norm and its rationale up front; this hook draws the hard line at commit
# time. (The worktree rule is the counterpart of worktree-check.sh; that hook
# warns at the first edit, this one blocks at commit and push.)
#
# This is the branch protection GitHub does not give a free private repo.
# Unlike worktree-check.sh, this hook looks at the COMMIT moment, and it reads
# `git -C <path>` and preceding `cd` — exactly the escape through which a
# subagent once silently moved main forward.
#
# Escape hatch: `touch .claude/allow-main` in the repo root. A file on purpose:
# it works mid-session and shows up in `git status`.
#
# NOT fail-open: without tooling, without a usable git result, and with a path
# that cannot be resolved, this hook cannot see where a commit goes, and
# letting it through silently is exactly the gap. Three outcomes, not two:
# pass, block, and COULD NOT CHECK — the last one blocks too.
#
# One narrow exception, in two steps: a command that cannot be parsed (open
# quote) only blocks if the raw text contains `git` together with `commit` or
# `push` at all. If it does not, there is nothing to protect and it passes
# silently — otherwise one stray apostrophe outside any repo would lock up
# every following command.

MISSING_TOOLS=""
command -v jq >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS jq"
command -v python3 >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS python3"
command -v git >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS git"
command -v cat >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS cat"
if [ -n "$MISSING_TOOLS" ]; then
  {
    echo "Main-commit gate: COULD NOT CHECK — missing tooling:$MISSING_TOOLS"
    echo ""
    echo "This hook makes sure nothing lands directly on the default branch."
    echo "Without that tooling nothing was checked; that is not a green light."
  } >&2
  exit 2
fi

INPUT=$(cat)
if ! CMD=$(printf '%s' "$INPUT" | jq -r '.tool_input.command // empty'); then
  {
    echo "Main-commit gate: COULD NOT CHECK — the payload is not valid JSON."
    echo ""
    echo "Without the command nothing was checked; that is not a green light."
  } >&2
  exit 2
fi
if [ -z "$CMD" ]; then
  exit 0
fi
CWD=$(echo "$INPUT" | jq -r '.cwd // empty')
[ -n "$CWD" ] || CWD="$PWD"

# Marker the parser uses for a path it could not resolve with certainty
# (unknown variable, command substitution, glob).
UNRESOLVED="<unknown>"

# Keep the streams apart: parser stderr must not end up in the findings,
# which are read line by line as fields.
PARSER_ERR=$(mktemp 2>/dev/null) || PARSER_ERR="${TMPDIR:-/tmp}/main-commit-gate.$$"
trap 'rm -f "$PARSER_ERR"' EXIT

# The parser lives in the lighthouse lib of this repo. Found through
# LIGHTHOUSE_REPO_ROOT or git, never from this file's own path. Run as a script,
# not with `-m`: `-m` puts the cwd first on sys.path, and the cwd is the user's
# repo. A missing lib is handled right below.
LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null)}"
LIB="$LIB_ROOT/.lighthouse/lib/lighthouse_lib"

# No lib (a `lib/` in .gitignore leaves `.lighthouse/lib/` out of a clone): the
# same two-step rule as for a command that cannot be parsed, so a missing lib
# does not lock up every command.
if [ ! -f "$LIB/git_invocations.py" ]; then
  case "$CMD" in
    *git*) ;;
    *) exit 0 ;;
  esac
  case "$CMD" in
    *commit* | *push*) ;;
    *) exit 0 ;;
  esac
  {
    echo "Main-commit gate: COULD NOT CHECK — the lighthouse lib is missing ($LIB)."
    echo ""
    echo "Run 'lighthouse init --upgrade'. Without the lib no commit check was done;"
    echo "that is not a green light."
  } >&2
  exit 2
fi

# The protected branch comes from the config of the repo these hooks are
# installed in. Only read when the text can hold a commit or push at all: a
# broken config must not block `ls`. A config that cannot be read blocks the
# commit — guarding 'main' while the repo protects another branch is no check.
DEFAULT_BRANCH="main"
case "$CMD" in
  *git*)
    case "$CMD" in
      *commit* | *push*)
        if ! DEFAULT_BRANCH=$(LIGHTHOUSE_REPO_ROOT="$LIB_ROOT" python3 "$LIB_ROOT/.lighthouse/lib/lighthouse_lib/config.py" forge.default_branch 2>"$PARSER_ERR"); then
          if ROOT=$(git -C "$CWD" rev-parse --show-toplevel 2>/dev/null) && [ -e "$ROOT/.claude/allow-main" ]; then
            exit 0
          fi
          {
            echo "Main-commit gate: COULD NOT CHECK — the protected branch could not be read."
            echo ""
            cat "$PARSER_ERR" 2>/dev/null
            echo ""
            echo "Fix the cause above, or — deliberately and once —"
            echo "'touch <repo-root>/.claude/allow-main'."
          } >&2
          exit 2
        fi
        ;;
    esac
    ;;
esac

FINDINGS=$(printf '%s' "$CMD" | python3 "$LIB_ROOT/.lighthouse/lib/lighthouse_lib/git_invocations.py" --default-branch "$DEFAULT_BRANCH" "$CWD" 2>"$PARSER_ERR")
STATUS=$?
PARSER_MESSAGE=$(cat "$PARSER_ERR" 2>/dev/null)

if [ "$STATUS" -eq 3 ]; then
  # Two steps, and the first one matters most. 'Cannot parse' and 'cannot look
  # at a commit' are not the same: if the raw text nowhere contains `git` with
  # `commit` or `push`, there is nothing to protect here and nothing to
  # report. Without that step a single `echo $'it\'s'` outside any repo
  # blocked every following command — a lockout the norm gains nothing from.
  # If a commit may still be in there, the norm applies in full and this
  # blocks after all.
  case "$CMD" in
    *git*) ;;
    *) exit 0 ;;
  esac
  case "$CMD" in
    *commit* | *push*) ;;
    *) exit 0 ;;
  esac
  {
    echo "Main-commit gate: COULD NOT CHECK — the command cannot be parsed"
    echo "(open quote), so no commit check was done."
    echo ""
    echo "That is not a green light: 'no commit found' and 'could not look'"
    echo "must not both pass. Split the command, or — deliberately and once —"
    echo "'touch <repo-root>/.claude/allow-main'."
  } >&2
  exit 2
fi
if [ "$STATUS" -ne 0 ]; then
  {
    echo "Main-commit gate: COULD NOT CHECK — the command parser failed."
    echo ""
    echo "$PARSER_MESSAGE"
  } >&2
  exit 2
fi
if [ -z "$FINDINGS" ]; then
  exit 0
fi

# Repo root of the cwd: only needed as the escape hatch for a path the parser
# could not resolve.
CWD_ROOT=$(git -C "$CWD" rev-parse --show-toplevel 2>/dev/null) || CWD_ROOT=""

block_uncheckable() {
  {
    echo "Main-commit gate: COULD NOT CHECK — $1"
    echo ""
    echo "This command does contain a '$2'. 'Could not look' is not a"
    echo "negative result, so this does not pass silently."
    echo ""
    echo "What you can do:"
    echo "  1. Spell the path out, without a variable or substitution."
    echo "  2. If it really has to be this way, deliberately and once:"
    echo "     touch <repo-root>/.claude/allow-main   # and remove it afterwards"
  } >&2
  exit 2
}

while IFS=$'\t' read -r ACTION TARGET TO_MAIN RAW; do
  if [ -z "$ACTION" ]; then
    continue
  fi

  if [ "$TARGET" = "$UNRESOLVED" ]; then
    if [ -n "$CWD_ROOT" ] && [ -e "$CWD_ROOT/.claude/allow-main" ]; then
      continue
    fi
    block_uncheckable "the path '$RAW' cannot be resolved." "$ACTION"
  fi

  # LC_ALL=C: the message below is matched as text, and git translates it.
  ROOT=$(LC_ALL=C git -C "$TARGET" rev-parse --show-toplevel 2>"$PARSER_ERR") || ROOT=""
  GIT_MESSAGE=$(cat "$PARSER_ERR" 2>/dev/null)
  if [ -z "$ROOT" ]; then
    # Distinguish: '$TARGET is not a repo' is a real outcome and may pass.
    # A git that breaks is not, and must block.
    case "$GIT_MESSAGE" in
      *"not a git repository"*|*"Not a git repository"*|*"No such file or directory"*|*"cannot change to"*)
        continue
        ;;
      *)
        if [ -n "$CWD_ROOT" ] && [ -e "$CWD_ROOT/.claude/allow-main" ]; then
          continue
        fi
        block_uncheckable "git could not read '$TARGET': ${GIT_MESSAGE:-no message}." "$ACTION"
        ;;
    esac
  fi

  # Escape hatch. In the repo root on purpose, visible in `git status`.
  if [ -e "$ROOT/.claude/allow-main" ]; then
    continue
  fi

  if ! BRANCH=$(git -C "$ROOT" branch --show-current 2>"$PARSER_ERR"); then
    GIT_MESSAGE=$(cat "$PARSER_ERR" 2>/dev/null)
    block_uncheckable "git could not determine the branch of '$ROOT': ${GIT_MESSAGE:-no message}." "$ACTION"
  fi

  if [ "$TO_MAIN" = "all" ]; then
    # `--all` / `--mirror` pushes every local branch; only relevant if there
    # actually is a local default branch here.
    if git -C "$ROOT" show-ref --verify --quiet "refs/heads/$DEFAULT_BRANCH"; then
      TO_MAIN="yes"
    else
      TO_MAIN="no"
    fi
  fi

  # Destination cannot be resolved (`git push origin $BR`): the same rule as
  # for an unresolvable path. If the repo itself is on main, the message
  # below is clearer — that one takes precedence.
  if [ "$TO_MAIN" = "unresolved" ] && [ "$BRANCH" != "$DEFAULT_BRANCH" ]; then
    block_uncheckable "the destination of this push cannot be resolved." "$ACTION"
  fi

  if [ "$BRANCH" != "$DEFAULT_BRANCH" ] && [ "$TO_MAIN" != "yes" ]; then
    continue
  fi

  if [ "$TO_MAIN" = "yes" ] && [ "$BRANCH" != "$DEFAULT_BRANCH" ]; then
    REASON="this push explicitly targets '$DEFAULT_BRANCH' ($ACTION from branch '$BRANCH')"
  else
    REASON="'$ROOT' is on branch '$DEFAULT_BRANCH' ($ACTION)"
  fi

  {
    echo "Main-commit gate: blocked — $REASON."
    echo ""
    echo "This repo has no branch protection on GitHub (free private repo),"
    echo "so the line is drawn here. Nothing lands directly on '$DEFAULT_BRANCH'."
    echo ""
    echo "What you can do:"
    echo "  1. Work in a worktree on its own branch:"
    echo "     git worktree add .claude/worktrees/<name> -b issue-<nr>-<slug>"
    echo "  2. Is the work already in the main checkout? Move it:"
    echo "     git -C '$ROOT' stash push -u -m move-<tag>"
    echo "  3. If it really has to go on '$DEFAULT_BRANCH', deliberately and once:"
    echo "     touch '$ROOT/.claude/allow-main'   # and remove it afterwards"
  } >&2
  exit 2
done <<< "$FINDINGS"

exit 0

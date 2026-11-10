#!/bin/bash
# lighthouse: core@v0.8.1
# The test gate of `git commit`, in two modes (#80):
#
#   commit-test-gate.sh pre   PreToolUse `Bash`
#   commit-test-gate.sh post  PostToolUse and PostToolUseFailure `Bash`
#
# Matcher: Bash. Blocking: exit 2 + stderr. Passed: exit 0 + JSON on stdout.
#
# The gate runs after the commit. `pre` blocks `--no-verify`, runs the tests
# up front only for a commit to the default branch (`forge.default_branch`, the
# branch main-commit-gate.sh protects), and for every other root of this
# project records the HEAD before the commit (lighthouse_lib/commit_base.py);
# `post` tests what the commit touched: per recorded root whose HEAD moved, it
# runs the command with LIGHTHOUSE_GATE_BASE, LIGHTHOUSE_GATE_HEAD and
# LIGHTHOUSE_GATE_FILES (the changed paths, one per line). Red is exit 2 after
# the commit: the commit stays, the fix comes in a new one.
#
# Counterpart: the test gate in the instruction block carries the norm up
# front; this hook enforces it at the moment of the commit.
#
# The command comes from `[test_gate] command` in lighthouse.toml and runs as
# `bash -c "$COMMAND"` in the root of the tree being committed, with
# LIGHTHOUSE_GATE_TMP set to an empty directory (for example for
# `--basetemp="$LIGHTHOUSE_GATE_TMP"`). Without a command the commit passes
# with a note: nothing was tested, and the hook says so (decision 20).
#
# The commit detection is that of main-commit-gate.sh
# (lighthouse_lib/git_invocations.py): a commit after `cd`, `;`, `|`, `&&` or
# with `git -C <path>` is seen, and the tree that is committed is the tree that
# is tested — not the `.cwd` of the payload and not $CLAUDE_PROJECT_DIR. Only
# repos of this project (same `--git-common-dir` as $CLAUDE_PROJECT_DIR: the
# main checkout plus every worktree; or the same `origin`: a worktree of a
# separate clone, such as forge's, lighthouse_lib/repo_identity.py) are tested;
# another repo is skipped visibly, with both common dirs and both origins.
#
# NOT fail-open: without tooling, with a command that cannot be parsed, a path
# that cannot be resolved, a directory that does not exist yet, a git that
# breaks or a configuration that cannot be read, nothing was tested — and that
# is no green light. Those cases block loudly with COULD NOT CHECK.
#
# Deliberate asymmetry with the guard, now only for a commit to the default
# branch: main-commit-gate.sh honours `.claude/allow-main` for an unresolvable
# path or a broken git; this hook does not. `allow-main` allows a commit on the
# default branch, not a commit without tests. The same parser thus deliberately
# gives two outcomes on one input.
#
# A Bash call with `run_in_background` returns before the commit lands, so
# `post` would see no moved HEAD: `pre` records nothing for it and says
# visibly that the commit is not tested.
#
# Why its own deadline: a command hook whose timeout expires does not block the
# tool call ("don't count on a stalled hook to act as a gate",
# https://code.claude.com/docs/en/hooks.md) — a timeout is fail-open. The hook
# therefore stops itself: one deadline (`[test_gate] deadline_seconds`, default
# 270 s) for the whole command (lighthouse_lib/deadline.py, kill of the process
# group, grace 10 s), within the hook timeout of 300 s in hooks.json. A
# deadline above MAX_DEADLINE leaves no room and blocks.
#
# Why slots: without a brake every session runs its suite at once, and that
# breaks the host. The gate therefore takes one of `[test_gate] slots` slots
# (`flock` on slot-<n> in a lock dir, via lighthouse_lib/gate_slot.py) before
# the loop. The slot stays open as fd <n + 2> until the hook ends, and
# `deadline.py` inherits it: if the hook is killed while the tests run, the
# slot stays taken until the deadline (+ grace) of `deadline.py`. Fixed fd
# numbers 3-9 through `eval`, not `exec {var}>>`: that is bash 4.1+ and macOS'
# /bin/bash is 3.2 — the hook would die on that line with an exit that is not 2,
# and the commit would pass untested. That caps the slots at MAX_SLOTS. The slot
# comes before the loop and holds for all roots: the deadline is also one for
# the whole loop, and the wait counts towards it. A commit in another project
# takes no slot (there is nothing to test).
#
# Why a memory brake: the slots cap the number of runs, not the memory. The
# brake is a property of the host: `gate_slice` in `[host]` of host.toml names a
# systemd slice with MemoryHigh/MemoryMax. With it, the command runs inside that
# slice: `systemd-run --user --scope --slice=<slice>`. A scope (not a service)
# runs the command itself: same PID, same process group, so the group kill of
# `deadline.py` keeps working. `choom -n 800` on top, because a scope cannot set
# `OOMScoreAdjust`: without a high score the kernel would rather pick the
# session than the gate. The check whether the brake is there looks at
# `FragmentPath` and `MemoryMax`, not at `LoadState`: systemd synthesises every
# slice name and reports it `loaded`, also without a unit file, and
# `systemd-run --slice=` into such a slice runs without a ceiling. Without
# `gate_slice` (Mac, devcontainer) the gate still tests, with the slots, and the
# output says why. The counters in `<slice>/memory.events` before and after a
# run turn an OOM kill or a memory-pressure deadline into a notice of its own
# instead of "tests failed".

HOOK_NAME="commit-test-gate"

MODE="${1:-}"
case "$MODE" in
  pre) ;;
  post) ;;
  *)
    {
      echo "$HOOK_NAME: COULD NOT CHECK — unknown mode, expected pre or post"
      echo ""
      echo "The registration in .claude/settings.json names no known mode; nothing was tested."
    } >&2
    exit 2
    ;;
esac

UNRESOLVED="<unknown>"
# A summary line of a selector (#80): `gate-<name>:` or `gate:`.
SELECTOR_LINE='^gate(-[A-Za-z0-9_.-]+)?:'
NEW_DIR="<new>"
# Hook timeout in hooks.json (300) minus the grace of deadline.py (10) minus a
# margin: above this the hook timeout fires first, and that is fail-open.
MAX_DEADLINE=280
# fds 3-9: single digits, bash 3.2.
MAX_SLOTS=7

MISSING_TOOLS=""
command -v jq >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS jq"
command -v python3 >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS python3"
command -v git >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS git"
command -v cat >/dev/null 2>&1 || MISSING_TOOLS="$MISSING_TOOLS cat"
if [ -n "$MISSING_TOOLS" ]; then
  {
    echo "$HOOK_NAME: COULD NOT CHECK — missing tooling:$MISSING_TOOLS"
    echo ""
    echo "This hook runs the test command of every commit."
    echo "Without that tooling nothing was tested; that is not a green light."
  } >&2
  exit 2
fi

# The lib lives in the lighthouse lib of this repo. Found through
# LIGHTHOUSE_REPO_ROOT or git, never from this file's own path.
LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null)}"
LIB="$LIB_ROOT/.lighthouse/lib/lighthouse_lib"

INPUT=$(cat)
if ! CMD=$(printf '%s' "$INPUT" | jq -r '.tool_input.command // empty'); then
  {
    echo "$HOOK_NAME: COULD NOT CHECK — the payload is not valid JSON."
    echo ""
    echo "Without the command nothing was tested; that is not a green light."
  } >&2
  exit 2
fi
if [ -z "$CMD" ]; then
  exit 0
fi
CWD=$(printf '%s' "$INPUT" | jq -r '.cwd // empty')
[ -n "$CWD" ] || CWD="$PWD"

# `post` only: the rows (`root<TAB>base<TAB>head`) of the commits of this call,
# and how many of them already passed.
POST_ROWS=""
ROWS_DONE=0

# $1 single-quoted for bash, so a pasted line runs it unchanged.
shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

# `post` only, printed under a COULD NOT CHECK: the commit is already there, and
# repeating it gives "nothing to commit" — HEAD does not move and `post` stays
# silent, which reads as green. So name every commit not tested yet and the
# line that tests it by hand. Prints nothing in `pre`.
post_untested() {
  [ "$MODE" = "post" ] || return 0
  local n=0 root base head command
  echo ""
  if [ -z "$POST_ROWS" ]; then
    echo "A commit of this call may be untested. Do not repeat the commit to retry:"
    echo "that gives 'nothing to commit' and tests nothing. Check 'git log', then run"
    echo "[test_gate] command by hand on the files of that commit."
    return 0
  fi
  if [ -n "${COMMAND:-}" ]; then
    command=$(shell_quote "$COMMAND")
  else
    command="'<[test_gate] command>'"
  fi
  echo "The commit is made but not tested. Do not repeat the commit to retry: that"
  echo "gives 'nothing to commit' and tests nothing. Test it by hand instead:"
  while IFS=$'\t' read -r root base head; do
    [ -n "$root" ] || continue
    n=$((n + 1))
    [ "$n" -gt "$ROWS_DONE" ] || continue
    echo ""
    echo "  Untested: $base..$head in $root"
    echo "  cd $(shell_quote "$root") && f=\$(mktemp) && t=\$(mktemp -d) && git -c core.quotePath=false diff --name-only --no-renames $base $head > \"\$f\" && LIGHTHOUSE_GATE_BASE=$base LIGHTHOUSE_GATE_HEAD=$head LIGHTHOUSE_GATE_FILES=\"\$f\" LIGHTHOUSE_GATE_TMP=\"\$t\" bash -c $command"
  done <<< "$POST_ROWS"
}

block() {
  {
    echo "$HOOK_NAME: COULD NOT CHECK — $1"
    echo ""
    if [ "$MODE" = "post" ]; then
      echo "'Could not test' is not a green light."
      post_untested
    else
      echo "This command does contain a 'git commit'. 'Could not test' is not a"
      echo "green light, so this does not pass silently."
    fi
  } >&2
  exit 2
}

# No lib (a `lib/` in .gitignore leaves `.lighthouse/lib/` out of a clone):
# the two-step rule below, so a missing lib does not lock up every command.
if [ ! -f "$LIB/git_invocations.py" ]; then
  case "$CMD" in
    *git*) ;;
    *) exit 0 ;;
  esac
  case "$CMD" in
    *commit*) ;;
    *) exit 0 ;;
  esac
  block "the lighthouse lib is missing ($LIB). Run 'lighthouse init --upgrade'."
fi

# Keep the streams apart: stderr of parser and git does not belong in the
# findings, which are read line by line as fields.
ERR_FILE=$(mktemp 2>/dev/null) || ERR_FILE="${TMPDIR:-/tmp}/commit-test-gate.$$"
# One trap: a second `trap … EXIT` replaced the first. BT is the gate tmp dir,
# empty until there is one.
BT=""
trap 'rm -f "$ERR_FILE"; [ -z "$BT" ] || rm -rf "$BT"' EXIT

MSG=""
add_message() {
  if [ -n "$MSG" ]; then
    MSG="$MSG"$'\n'"$1"
  else
    MSG="$1"
  fi
}

# additionalContext reaches the model, systemMessage the user: a silent gate
# must not look like a satisfied gate. $1: the hook event the JSON answers
# (PreToolUse, PostToolUse or PostToolUseFailure).
emit_message_and_exit() {
  if [ -n "$MSG" ]; then
    jq -cn --arg m "$MSG" --arg e "$1" '{systemMessage: $m, hookSpecificOutput: {hookEventName: $e, additionalContext: $m}}'
  fi
  exit 0
}

# One key from lighthouse.toml or host.toml into CONFIG_VALUE. Returns 0 (set)
# or 1 (not set); a configuration that cannot be read blocks. Called directly,
# never in a command substitution, so `block` ends the hook.
read_config() {
  CONFIG_VALUE=$(LIGHTHOUSE_REPO_ROOT="$LIB_ROOT" python3 "$LIB/config.py" "$1" 2>"$ERR_FILE")
  local rc=$?
  case "$rc" in
    0|1) return "$rc" ;;
  esac
  block "the configuration cannot be read ($1): $(cat "$ERR_FILE" 2>/dev/null)"
}

slot_failed() {
  {
    echo "$HOOK_NAME: COULD NOT CHECK — cannot take a gate slot: $1"
    echo ""
    echo "Without a slot this run would go unchecked beside the other gates."
    echo "Not being able to test is not a green light."
    post_untested
  } >&2
  exit 2
}

# Value of key $1 in memory.events; 0 if the file or the key is missing.
event_count() {
  local key value
  if [ -r "$EVENTS" ]; then
    while read -r key value; do
      if [ "$key" = "$1" ]; then
        case "$value" in
          ''|*[!0-9]*) ;;
          *) printf '%s' "$value"; return 0 ;;
        esac
      fi
    done < "$EVENTS"
  fi
  printf '0'
}

# After minus before; a negative delta (slice recreated) counts as no delta.
delta() {
  local d=$(( $2 - $1 ))
  [ "$d" -gt 0 ] || d=0
  printf '%s' "$d"
}

# The gate itself, on the roots in $1, answering hook event $2: the
# configuration, the slot, the memory brake, then the test command per root.
# One root per line: `root` (pre) or `root<TAB>base<TAB>head` (post), and only
# a row with a base gives the command LIGHTHOUSE_GATE_BASE, _HEAD and _FILES.
# Ends the hook: exit 2 on a red or unchecked run, otherwise
# emit_message_and_exit.
run_gate_roots() {
  local roots="$1" event="$2" fail_line
  if [ "$MODE" = "post" ]; then
    fail_line="$HOOK_NAME: tests failed on the commit you just made. Fix them in a new commit."
  else
    fail_line="$HOOK_NAME: tests failed. Fix the failing tests before committing."
  fi
  # Not inherited from an outer gate run (a test suite of a hook inside a real
  # gate): without a row of its own the command tests everything.
  unset LIGHTHOUSE_GATE_BASE LIGHTHOUSE_GATE_HEAD LIGHTHOUSE_GATE_FILES

  # Not set, or `command = ""` (the opt-out, which `--upgrade` leaves alone): the
  # same note, nothing was tested.
  if ! read_config test_gate.command || [ -z "$CONFIG_VALUE" ]; then
    add_message "$HOOK_NAME: no test command configured, nothing was tested"
    emit_message_and_exit "$event"
  fi
  COMMAND="$CONFIG_VALUE"
  # Where the command comes from, and how to turn it off: a default command that does
  # not fit this project (no pytest) must not leave the agent guessing.
  # The same path as `config_path()` with the LIGHTHOUSE_REPO_ROOT of read_config.
  CONFIG_FILE="${LIGHTHOUSE_TOML:-$LIB_ROOT/lighthouse.toml}"
  COMMAND_HINT="The command is [test_gate] command in $CONFIG_FILE: $COMMAND. Set command = \"\" there to turn the test gate off."
  read_config test_gate.deadline_seconds
  DEADLINE_SECONDS="$CONFIG_VALUE"
  read_config test_gate.slots
  SLOTS="$CONFIG_VALUE"
  read_config test_gate.slot_wait_seconds
  SLOT_WAIT="$CONFIG_VALUE"
  read_config test_gate.unchecked_exit_codes
  # One code per line; spaces around them for the membership test below.
  UNCHECKED=" $(printf '%s' "$CONFIG_VALUE" | tr '\n' ' ') "
  SLICE=""
  if read_config host.gate_slice; then
    SLICE="$CONFIG_VALUE"
  fi

  # The config reader checked the types; the ranges are this hook's.
  if [ "$DEADLINE_SECONDS" -lt 1 ] || [ "$DEADLINE_SECONDS" -gt "$MAX_DEADLINE" ]; then
    block "test_gate.deadline_seconds = $DEADLINE_SECONDS; it must be between 1 and $MAX_DEADLINE (hook timeout 300 s)."
  fi
  if [ "$SLOTS" -lt 1 ] || [ "$SLOTS" -gt "$MAX_SLOTS" ]; then
    block "test_gate.slots = $SLOTS; it must be between 1 and $MAX_SLOTS."
  fi
  if [ "$SLOT_WAIT" -lt 0 ]; then
    block "test_gate.slot_wait_seconds = $SLOT_WAIT; it must not be negative."
  fi

  # One `date` call, before the slot: the wait counts towards the deadline.
  DEADLINE=$(( $(date +%s) + DEADLINE_SECONDS ))
  # So it never runs past it either: a wait beyond the hook timeout would let the
  # commit through untested.
  [ "$SLOT_WAIT" -le "$DEADLINE_SECONDS" ] || SLOT_WAIT="$DEADLINE_SECONDS"

  if [ -n "${LIGHTHOUSE_GATE_SLOT_DIR:-}" ]; then
    SLOT_DIR="$LIGHTHOUSE_GATE_SLOT_DIR"
  elif [ -d "/run/user/$UID" ]; then
    SLOT_DIR="/run/user/$UID/lighthouse-gate-slots"
  else
    SLOT_DIR="/tmp/lighthouse-gate-slots-$UID"
  fi
  # SC2174: only the lock dir itself must be 700; the parent (/run/user/$UID or /tmp) exists already.
  # shellcheck disable=SC2174
  if ! MKDIR_OUT=$(mkdir -p -m 700 "$SLOT_DIR" 2>&1); then
    slot_failed "lock dir $SLOT_DIR cannot be created ($MKDIR_OUT)"
  fi

  # A failed `exec` redirect does not stop a non-interactive bash (measured); the
  # `if` sees it. Slot n is fd n + 2: see the header on bash 3.2.
  SLOT_ARGS=()
  N=1
  while [ "$N" -le "$SLOTS" ]; do
    if ! eval "exec $((N + 2))>>\"\$SLOT_DIR/slot-$N\""; then
      slot_failed "lock files in $SLOT_DIR cannot be opened"
    fi
    SLOT_ARGS+=(--fd "$((N + 2))")
    N=$((N + 1))
  done

  SLOT=$(python3 "$LIB/gate_slot.py" "${SLOT_ARGS[@]}" --wait "$SLOT_WAIT" 2>"$ERR_FILE")
  SLOT_STATUS=$?
  if [ "$SLOT_STATUS" -eq 75 ]; then
    {
      echo "$HOOK_NAME: COULD NOT CHECK — all gate slots busy after $SLOT_WAIT s (slots: $SLOTS)"
      echo ""
      if [ "$MODE" = "post" ]; then
        echo "Other gates are running already. Not being able to test is not a green light."
        post_untested
      else
        echo "Other gates are running already. Try the commit again shortly."
        echo "Not being able to test is not a green light."
      fi
    } >&2
    exit 2
  fi
  if [ "$SLOT_STATUS" -ne 0 ]; then
    slot_failed "gate_slot.py gave exit $SLOT_STATUS: $(cat "$ERR_FILE" 2>/dev/null)"
  fi
  case "$SLOT" in
    ''|*[!0-9]*) slot_failed "gate_slot.py gave exit 0 without a valid slot number: '$SLOT'" ;;
  esac
  if [ "$SLOT" -lt 1 ] || [ "$SLOT" -gt "$SLOTS" ]; then
    slot_failed "gate_slot.py gave exit 0 without a valid slot number: '$SLOT'"
  fi
  # Close the fds of the slots not taken; the one taken stays open until the
  # hook ends, and `deadline.py` inherits it.
  N=1
  while [ "$N" -le "$SLOTS" ]; do
    [ "$N" -eq "$SLOT" ] || eval "exec $((N + 2))>&-"
    N=$((N + 1))
  done

  if ! BT=$(mktemp -d 2>"$ERR_FILE"); then
    BT=""
    {
      echo "$HOOK_NAME: COULD NOT CHECK — no gate tmp dir: $(cat "$ERR_FILE" 2>/dev/null)"
      echo ""
      echo "Not being able to test is not a green light."
      post_untested
    } >&2
    exit 2
  fi

  # VS Code `claude` runs in the logind scope without XDG_RUNTIME_DIR and
  # sometimes without DBUS_SESSION_BUS_ADDRESS; without a bus `systemctl --user`
  # fails and the brake drops out silently on exactly the path that needs it.
  if [ -z "${XDG_RUNTIME_DIR:-}" ] && [ -z "${DBUS_SESSION_BUS_ADDRESS:-}" ] \
    && [ -d "/run/user/$UID" ]; then
    export XDG_RUNTIME_DIR="/run/user/$UID"
  fi

  # Brake available? The first hit is the reason. `show` reports KEY=VALUE lines
  # in systemd's order, not in that of the `-p` arguments.
  REM_REASON=""
  if [ -z "$SLICE" ]; then
    REM_REASON="no gate_slice in host.toml"
    add_message "$HOOK_NAME: no gate_slice in host.toml, tests run without memory brake"
  elif ! command -v systemctl >/dev/null 2>&1; then
    REM_REASON="no systemctl"
  elif ! command -v systemd-run >/dev/null 2>&1; then
    REM_REASON="no systemd-run"
  elif ! command -v choom >/dev/null 2>&1; then
    REM_REASON="no choom"
  elif ! SHOW=$(systemctl --user show "$SLICE" -p FragmentPath -p MemoryMax 2>"$ERR_FILE"); then
    REM_REASON="systemctl --user failed"
  else
    FRAGMENT=""
    MEMORY_MAX=""
    while IFS='=' read -r KEY VALUE; do
      case "$KEY" in
        FragmentPath) FRAGMENT="$VALUE" ;;
        MemoryMax) MEMORY_MAX="$VALUE" ;;
      esac
    done <<< "$SHOW"
    if [ -z "$FRAGMENT" ]; then
      REM_REASON="$SLICE not installed"
    elif [ -z "$MEMORY_MAX" ] || [ "$MEMORY_MAX" = "infinity" ]; then
      REM_REASON="$SLICE without MemoryMax"
    fi
  fi

  WRAP=()
  if [ -n "$SLICE" ] && [ -n "$REM_REASON" ]; then
    add_message "$HOOK_NAME: memory brake not available: $REM_REASON"
  elif [ -z "$REM_REASON" ]; then
    WRAP=(systemd-run --user --scope --slice="$SLICE" --quiet -- choom -n 800 --)
  fi

  # The counters of the slice. LIGHTHOUSE_GATE_CGROUP_ROOT: for tests only.
  EVENTS="${LIGHTHOUSE_GATE_CGROUP_ROOT:-/sys/fs/cgroup/user.slice/user-$UID.slice/user@$UID.service}/$SLICE/memory.events"

  RUN=0
  while IFS=$'\t' read -r ROOT GATE_BASE GATE_HEAD; do
    [ -n "$ROOT" ] || continue
    # An empty LIGHTHOUSE_GATE_TMP per run.
    RUN=$((RUN + 1))
    GATE_TMP="$BT/run-$RUN"
    if ! mkdir "$GATE_TMP" 2>"$ERR_FILE"; then
      block "no gate tmp dir for $ROOT: $(cat "$ERR_FILE" 2>/dev/null)"
    fi
    # The files of the commit, beside GATE_TMP and not in it: `pytest
    # --basetemp` empties that directory, and a selector later in the command
    # must still read the list.
    GATE_FILES=""
    if [ -n "$GATE_BASE" ]; then
      [ -n "$GATE_HEAD" ] || block "the recorded row for $ROOT has no new HEAD."
      GATE_FILES="$BT/run-$RUN.files"
      if ! python3 "$LIB/commit_base.py" files "$ROOT" "$GATE_BASE" "$GATE_HEAD" "$GATE_FILES" 2>"$ERR_FILE"; then
        block "could not list the files of the commit in $ROOT: $(cat "$ERR_FILE" 2>/dev/null)"
      fi
    fi
    OOM_DELTA=0
    HIGH_DELTA=0
    if [ -z "$REM_REASON" ]; then
      OOM_BEFORE=$(event_count oom_kill)
      HIGH_BEFORE=$(event_count high)
    fi
    # "$BASH": the interpreter of this hook, not whatever `bash` PATH holds.
    # The exports stay in the subshell of the command substitution.
    TEST_OUTPUT=$(cd "$ROOT" \
      && if [ -n "$GATE_FILES" ]; then
        export LIGHTHOUSE_GATE_BASE="$GATE_BASE" LIGHTHOUSE_GATE_HEAD="$GATE_HEAD" \
          LIGHTHOUSE_GATE_FILES="$GATE_FILES"
      fi \
      && LIGHTHOUSE_GATE_TMP="$GATE_TMP" python3 "$LIB/deadline.py" "$DEADLINE" -- \
      ${WRAP[@]+"${WRAP[@]}"} "$BASH" -c "$COMMAND" 2>&1)
    STATUS=$?
    if [ -z "$REM_REASON" ]; then
      OOM_DELTA=$(delta "$OOM_BEFORE" "$(event_count oom_kill)")
      HIGH_DELTA=$(delta "$HIGH_BEFORE" "$(event_count high)")
    fi
    # The rows hold from top to bottom, the first hit wins. A green run stays
    # green, also with a delta from a neighbouring gate.
    if [ "$STATUS" -eq 0 ]; then
      :
    elif [ "$OOM_DELTA" -gt 0 ]; then
      {
        echo "$HOOK_NAME: COULD NOT CHECK — gate throttled (oom-kill in $SLICE during this run)."
        echo ""
        echo "Root: $ROOT. Not being able to test is not a green light."
        post_untested
        echo ""
        echo "$TEST_OUTPUT"
      } >&2
      exit 2
    elif [ -z "$REM_REASON" ] && {
      [[ "$TEST_OUTPUT" == *"Failed to start transient scope unit"* ]] \
        || [[ "$TEST_OUTPUT" == *"Failed to connect to bus"* ]] \
        || [[ "$TEST_OUTPUT" == *"Failed to create bus connection"* ]]
    }; then
      {
        echo "$HOOK_NAME: COULD NOT CHECK — systemd-run did not start."
        echo ""
        echo "Root: $ROOT. Not being able to test is not a green light."
        post_untested
        echo ""
        echo "$TEST_OUTPUT"
      } >&2
      exit 2
    fi
    if [ "$STATUS" -eq 124 ]; then
      {
        echo "$HOOK_NAME: COULD NOT CHECK — the test command ran past the deadline of $DEADLINE_SECONDS s."
        if [ "$HIGH_DELTA" -gt 0 ]; then
          echo "$SLICE was under memory pressure"
        fi
        echo ""
        echo "Root: $ROOT. An aborted run is not a green light."
        post_untested
        echo ""
        echo "$TEST_OUTPUT"
      } >&2
      exit 2
    fi
    if [ "$STATUS" -eq 127 ]; then
      # The test command did not start (usually: a program in it is not on
      # PATH). Then nothing was tested — something else than "tests failed".
      {
        echo "$HOOK_NAME: COULD NOT CHECK — the test command could not start (command not found?)."
        echo ""
        echo "Root: $ROOT. Not being able to test is not a green light."
        echo "$COMMAND_HINT"
        post_untested
        echo ""
        echo "$TEST_OUTPUT"
      } >&2
      exit 2
    fi
    if [ "$STATUS" -ge 128 ]; then
      # A SIGKILL on a child of bash gives 137, on the command via deadline.py
      # 247 (sys.exit(-9)): n = exit - 128, or 256 - exit above 192.
      if [ "$STATUS" -le 192 ]; then
        SIGNAL_NR=$(( STATUS - 128 ))
      else
        SIGNAL_NR=$(( 256 - STATUS ))
      fi
      # Without a brake there is no slice and so no systemd-oomd in it.
      if [ -z "$SLICE" ]; then
        KILL_HINT="$REM_REASON"
      elif [ -n "$REM_REASON" ]; then
        KILL_HINT="memory brake not available: $REM_REASON"
      else
        KILL_HINT="systemd-oomd in $SLICE?"
      fi
      {
        echo "$HOOK_NAME: COULD NOT CHECK — gate killed by signal $SIGNAL_NR ($KILL_HINT)."
        echo ""
        echo "Root: $ROOT. Not being able to test is not a green light."
        post_untested
        echo ""
        echo "$TEST_OUTPUT"
      } >&2
      exit 2
    fi
    case "$UNCHECKED" in
      *" $STATUS "*)
        # An exit code the project names as "nothing was tested" (for pytest:
        # 3 internal error, 4 usage error, 5 nothing collected). Block, but not
        # as "tests failed" — that sends the agent looking for a broken test
        # that is not there.
        {
          echo "$HOOK_NAME: COULD NOT CHECK — the test command stopped with exit code $STATUS, listed in test_gate.unchecked_exit_codes: nothing was tested."
          echo ""
          echo "Root: $ROOT. Not being able to test is not a green light."
          echo "$COMMAND_HINT"
          post_untested
          echo ""
          echo "$TEST_OUTPUT"
        } >&2
        exit 2
        ;;
    esac
    if [ "$STATUS" -ne 0 ]; then
      {
        echo "$fail_line"
        echo "$COMMAND_HINT"
        echo ""
        echo "$TEST_OUTPUT"
      } >&2
      exit 2
    fi
    # The summary lines of the selectors (`gate-<name>:` or `gate:`), in order,
    # so a `gate-go: skipped — …` stays visible when more runs after it; without
    # one, the last non-empty line.
    LAST=""
    SUMMARY=0
    while IFS= read -r LINE; do
      [ -n "${LINE//[[:space:]]/}" ] || continue
      LAST="$LINE"
      if [[ "$LINE" =~ $SELECTOR_LINE ]]; then
        add_message "$HOOK_NAME: $ROOT — $LINE"
        SUMMARY=1
      fi
    done <<< "$TEST_OUTPUT"
    [ "$SUMMARY" -eq 1 ] || add_message "$HOOK_NAME: $ROOT — $LAST"
    ROWS_DONE=$((ROWS_DONE + 1))
  done <<< "$roots"

  emit_message_and_exit "$event"
}

# `post`: the roots whose HEAD moved since `pre` recorded it. No row (nothing
# recorded, or nothing committed): silent, before configuration, slot or brake.
# `moved` reads and removes the record, so a repeated `post` tests nothing.
if [ "$MODE" = "post" ]; then
  if ! ROWS=$(printf '%s' "$INPUT" | python3 "$LIB/commit_base.py" moved 2>"$ERR_FILE"); then
    # The two-step rule: without `git` and `commit` in the raw text `pre` recorded
    # nothing, so a state dir that cannot be read does not block every Bash call.
    # Only on this failure path: `git "com"mit` lacks the word but is a commit.
    case "$CMD" in
      *git*) ;;
      *) exit 0 ;;
    esac
    case "$CMD" in
      *commit*) ;;
      *) exit 0 ;;
    esac
    block "could not read the recorded HEAD: $(cat "$ERR_FILE" 2>/dev/null)"
  fi
  if [ -z "$ROWS" ]; then
    exit 0
  fi
  POST_ROWS="$ROWS"
  # PostToolUseFailure too: `git commit -m x && false` fails the call, but the
  # commit is there.
  EVENT=$(printf '%s' "$INPUT" | jq -r '.hook_event_name // empty')
  [ "$EVENT" = "PostToolUseFailure" ] || EVENT="PostToolUse"
  run_gate_roots "$ROWS"$'\n' "$EVENT"
fi

FINDINGS=$(printf '%s' "$CMD" | python3 "$LIB/git_invocations.py" --report-new-dirs "$CWD" 2>"$ERR_FILE")
STATUS=$?

if [ "$STATUS" -eq 3 ]; then
  # The guard's two-step rule, here on `git` and `commit` (push is not this
  # hook's business): without them in the raw text there is nothing to test,
  # and one stray apostrophe locks nothing up.
  case "$CMD" in
    *git*) ;;
    *) exit 0 ;;
  esac
  case "$CMD" in
    *commit*) ;;
    *) exit 0 ;;
  esac
  block "the command cannot be parsed (open quote). Split the command."
fi
if [ "$STATUS" -ne 0 ]; then
  {
    echo "$HOOK_NAME: COULD NOT CHECK — the command parser failed."
    echo ""
    cat "$ERR_FILE" 2>/dev/null
  } >&2
  exit 2
fi

COMMITS=""
while IFS=$'\t' read -r ACTION TARGET TO_MAIN RAW; do
  if [ "$ACTION" = "commit" ]; then
    COMMITS="$COMMITS$ACTION"$'\t'"$TARGET"$'\t'"$TO_MAIN"$'\t'"$RAW"$'\n'
  fi
done <<< "$FINDINGS"
if [ -z "$COMMITS" ]; then
  exit 0
fi

printf '%s' "$CMD" | python3 "$LIB/no_verify.py" 2>"$ERR_FILE"
STATUS=$?
if [ "$STATUS" -ne 0 ] && [ "$STATUS" -ne 2 ]; then
  {
    echo "$HOOK_NAME: COULD NOT CHECK — the --no-verify check failed."
    echo ""
    cat "$ERR_FILE" 2>/dev/null
  } >&2
  exit 2
fi
if [ "$STATUS" -eq 2 ]; then
  echo "$HOOK_NAME: --no-verify is not allowed in this repo." >&2
  echo "" >&2
  echo "The test gate is a Claude Code PreToolUse hook, not a git hook, so the" >&2
  echo "flag skips nothing today — but it will as soon as git hooks are added." >&2
  echo "If something fails: fix it. If it really has to be skipped, ask." >&2
  exit 2
fi

# The repo root per commit. Unique roots, separated by a newline.
ROOTS=""
while IFS=$'\t' read -r ACTION TARGET TO_MAIN RAW; do
  [ -n "$ACTION" ] || continue
  if [ "$TARGET" = "$UNRESOLVED" ]; then
    block "the path '$RAW' cannot be resolved. Spell the path out."
  fi
  if [ "$TARGET" = "$NEW_DIR" ]; then
    block "'$RAW' does not exist yet. Commit in a separate call, once the directory exists."
  fi

  # LC_ALL=C: git translates the text matched below.
  ROOT=$(LC_ALL=C git -C "$TARGET" rev-parse --show-toplevel 2>"$ERR_FILE") || ROOT=""
  GIT_MESSAGE=$(cat "$ERR_FILE" 2>/dev/null)
  if [ -z "$ROOT" ]; then
    # '$TARGET is not a repo' is a real outcome: nothing to test. A git that
    # breaks (dubious ownership, broken installation) is not.
    case "$GIT_MESSAGE" in
      *"not a git repository"*|*"Not a git repository"*|*"No such file or directory"*|*"cannot change to"*)
        continue
        ;;
      *)
        block "git could not read '$TARGET': ${GIT_MESSAGE:-no message}."
        ;;
    esac
  fi

  case $'\n'"$ROOTS" in
    *$'\n'"$ROOT"$'\n'*) ;;
    *) ROOTS="$ROOTS$ROOT"$'\n' ;;
  esac
done <<< "$COMMITS"

if [ -z "$ROOTS" ]; then
  exit 0
fi

# `--git-common-dir`, absolute; the same error contract for reference and target.
common_dir() {
  local dir="$1" out message
  out=$(git -C "$dir" rev-parse --path-format=absolute --git-common-dir 2>"$ERR_FILE") || out=""
  message=$(cat "$ERR_FILE" 2>/dev/null)
  if [ -z "$out" ]; then
    block "git could not determine the common git dir of '$dir': ${message:-no message}."
  fi
  printf '%s' "$out"
}

REF_DIR="${CLAUDE_PROJECT_DIR:-$LIB_ROOT}"
# In a command substitution `block` only ends the subshell; the empty output
# catches that, and the message is on stderr by then.
REF_COMMON=$(common_dir "$REF_DIR") || exit 2

PROJECT_ROOTS=""
while IFS= read -r ROOT; do
  [ -n "$ROOT" ] || continue
  COMMON=$(common_dir "$ROOT") || exit 2
  if [ "$COMMON" = "$REF_COMMON" ]; then
    PROJECT_ROOTS="$PROJECT_ROOTS$ROOT"$'\n'
    continue
  fi
  # Another common dir: a worktree of a separate clone (forge) with the same
  # origin is still this project (#36).
  ORIGIN_REASON=$(python3 "$LIB/repo_identity.py" "$REF_DIR" "$ROOT" 2>"$ERR_FILE")
  case $? in
    0) PROJECT_ROOTS="$PROJECT_ROOTS$ROOT"$'\n' ;;
    1) add_message "$HOOK_NAME: $ROOT skipped — other project (git-common-dir $COMMON vs $REF_COMMON; $ORIGIN_REASON)" ;;
    *) block "git could not read the origin of '$ROOT': $(cat "$ERR_FILE" 2>/dev/null)" ;;
  esac
done <<< "$ROOTS"

# No project root (only another project): nothing to test or record, so no
# configuration, no slot and no wait.
if [ -z "$PROJECT_ROOTS" ]; then
  emit_message_and_exit PreToolUse
fi

# Which roots go to the default branch: the branch main-commit-gate.sh
# protects. `TO_MAIN` of git_invocations.py is always `no` for a commit and does
# not count here. A detached HEAD is not the default branch.
if ! read_config forge.default_branch || [ -z "$CONFIG_VALUE" ]; then
  block "the default branch cannot be read (forge.default_branch is not set)."
fi
DEFAULT_BRANCH="$CONFIG_VALUE"
MAIN_ROOTS=""
OTHER_ROOTS=()
while IFS= read -r ROOT; do
  [ -n "$ROOT" ] || continue
  if ! BRANCH=$(git -C "$ROOT" branch --show-current 2>"$ERR_FILE"); then
    block "git could not read the branch of '$ROOT': $(cat "$ERR_FILE" 2>/dev/null)"
  fi
  if [ -n "$BRANCH" ] && [ "$BRANCH" = "$DEFAULT_BRANCH" ]; then
    MAIN_ROOTS="$MAIN_ROOTS$ROOT"$'\n'
  else
    OTHER_ROOTS+=("$ROOT")
  fi
done <<< "$PROJECT_ROOTS"

# Every other root: record its HEAD for `post`, no tests here. A background call
# returns before the commit lands, so `post` would see nothing: no record, but a
# visible notice instead of a silent pass.
if [ "${#OTHER_ROOTS[@]}" -gt 0 ]; then
  BACKGROUND=$(printf '%s' "$INPUT" | jq -r '.tool_input.run_in_background // false')
  if [ "$BACKGROUND" = "true" ]; then
    for ROOT in "${OTHER_ROOTS[@]}"; do
      add_message "$HOOK_NAME: $ROOT — commit in a background call is not tested; commit in the foreground."
    done
  elif ! printf '%s' "$INPUT" | python3 "$LIB/commit_base.py" record "${OTHER_ROOTS[@]}" 2>"$ERR_FILE"; then
    block "could not record HEAD: $(cat "$ERR_FILE" 2>/dev/null)"
  fi
fi

# A commit to the default branch: test it before the commit, as before #80.
if [ -n "$MAIN_ROOTS" ]; then
  run_gate_roots "$MAIN_ROOTS" PreToolUse
fi
emit_message_and_exit PreToolUse

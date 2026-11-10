#!/usr/bin/env bash
# lighthouse: shell@v0.8.1
# Run shellcheck on the shell of this repo, or on the files you pass.
#
# Without arguments it runs the *strict set*: the shell in `shell.strict_dirs`
# from lighthouse.toml (default `.claude/hooks`), the shell that itself guards
# gates. That set stands at zero findings at the lowest severity (style) - that is
# KPI row 1 of the shell-standard skill.
#
# Exit codes, and this is the whole point of this script:
#   0  checked, nothing found
#   1  checked, findings
#   2  called wrongly
#   4  NOT checked (shellcheck is missing or fails) - neither green nor red
#
# Three outcomes, not two. "Could not look" is not "found nothing".
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib/shellcheck-bin.sh
. "$HERE/lib/shellcheck-bin.sh"

usage() {
  echo "usage: $0 [--severity error|warning|info|style] [file ...]" >&2
  echo "  without files: the strict set (shell.strict_dirs in lighthouse.toml)" >&2
}

severity=style
files=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --severity)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      case "$2" in
        error|warning|info|style) severity="$2" ;;
        *) usage; exit 2 ;;
      esac
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    --) shift; files+=("$@"); break ;;
    -*) usage; exit 2 ;;
    *) files+=("$1"); shift ;;
  esac
done

SHELLCHECK="$(resolve_shellcheck)" || exit "$SHELLCHECK_UNAVAILABLE"

if [ "${#files[@]}" -eq 0 ]; then
  REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
    echo "shellcheck-repo: not in a git repository and no files given" >&2
    exit 2
  }
  cd "$REPO_ROOT" || exit 2
  # The directories where the bar is highest, because a bug in a gate is quieter
  # than a bug in ordinary code: it reports green.
  LIB_ROOT="${LIGHTHOUSE_REPO_ROOT:-$REPO_ROOT}"
  if ! dirs_out="$(LIGHTHOUSE_REPO_ROOT="$LIB_ROOT" python3 "$LIB_ROOT/.lighthouse/lib/lighthouse_lib/config.py" shell.strict_dirs 2>&1)"; then
    echo "shellcheck-repo: shell.strict_dirs could not be read: $dirs_out" >&2
    exit "$SHELLCHECK_UNAVAILABLE"
  fi
  specs=()
  while IFS= read -r dir; do
    [ -n "$dir" ] && specs+=("${dir%/}/*.sh")
  done <<EOF
$dirs_out
EOF
  # git ls-files instead of a glob: no empty expansion that gets passed on as a literal
  # file name, and recursive into subdirectories.
  # --others --exclude-standard is not optional: without those two the gate
  # does not see a brand-new, not yet tracked hook at all and reports green
  # about a set that the new file was not in.
  if [ "${#specs[@]}" -gt 0 ]; then
    while IFS= read -r f; do
      files+=("$f")
    done < <(git ls-files --cached --others --exclude-standard -- "${specs[@]}")
  fi
  if [ "${#files[@]}" -eq 0 ]; then
    echo "shellcheck-repo: no .sh found in the strict set - this is not green" >&2
    exit 2
  fi
fi

# -x: follow `source`/`.` into the sourced file. Without it shellcheck reports
# SC1091 ("not following") on every script that loads lib/ — a finding about the
# check itself instead of about the code.
# The rc of shellcheck itself: 0 clean, 1 findings. Any other rc (2 a file that
# it could not read, 3 and 4 wrong options, or a crash) is not checked.
"$SHELLCHECK" -x -S "$severity" "${files[@]}"
rc=$?
case "$rc" in
  0|1) exit "$rc" ;;
esac
echo "shellcheck-repo: shellcheck exited with rc=$rc, so nothing was checked" >&2
exit "$SHELLCHECK_UNAVAILABLE"

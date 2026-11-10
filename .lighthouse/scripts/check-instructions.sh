#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# The generic deterministic checks over the instruction layer of a repo:
# 1, 2, 3, 4, 5, 7, 8, 10, 11 and 14. The numbers come from the source; 6, 9, 12 and 13
# are project-specific and are not in here.
#
# Layer 2 of the gate spec (commit time): no network, no LLM, no state.
# It makes the instruction layer itself measurable -- the difference between a
# norm and a wish.
#
# Output contract, three outcomes and not two:
#   ok       the check measured and found nothing
#   FAIL     the check measured and found something -> exit 1
#   SKIPPED  the check could not measure here. Exit 0, but loud: the
#            absence of a signal is not a negative result.
# If the script as a whole cannot measure (no repo root, broken
# lighthouse.toml), then `check-instructions: COULD NOT CHECK — <reason>` on
# stderr and exit 2.
#
# Run:  .lighthouse/scripts/check-instructions.sh
set -uo pipefail

could_not_check() {
  printf 'check-instructions: COULD NOT CHECK — %s\n' "$1" >&2
  exit 2
}

# The repo root: `LIGHTHOUSE_REPO_ROOT`, otherwise git. Not the directory of this script:
# that is installed in `.lighthouse/scripts/`, and the config reader picks
# the same root.
ROOT="${LIGHTHOUSE_REPO_ROOT:-}"
if [ -z "$ROOT" ]; then
  ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" ||
    could_not_check "no repository root: LIGHTHOUSE_REPO_ROOT is not set and git rev-parse --show-toplevel failed"
fi
[ -d "$ROOT" ] || could_not_check "repository root '$ROOT' is not a directory"
CLAUDE_SKILLS="$ROOT/.claude/skills"
AGENTS_SKILLS="$ROOT/.agents/skills"
PERSONAL_SKILLS="${CHECK_INSTRUCTIONS_PERSONAL:-$HOME/.claude/skills}"
CONFIG_PY="$(dirname "${BASH_SOURCE[0]}")/../lib/lighthouse_lib/config.py"

findings=0

fail() {
  printf 'FAIL  %s\n' "$1"
  findings=$((findings + 1))
}

skip() { printf 'SKIPPED  %s\n' "$1"; }
ok() { printf 'ok    %s\n' "$1"; }

# sha256sum does not exist on macOS. A repo is edited on Linux AND on a Mac,
# and a gate that silently does nothing on one of the two is not a gate.
if command -v sha256sum >/dev/null 2>&1; then
  SHA_CMD="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  SHA_CMD="shasum -a 256"
else
  printf 'FAIL  no sha256sum and no shasum on this machine; the duplicate check cannot run\n'
  exit 1
fi

# python3 is rarely missing, but check 8 (pairing) parses settings.json with it
# and cannot measure without python3 whether a hook path is registered; checks 10
# and 11 read lighthouse.toml with it. Unlike sha256sum/shasum this touches only
# a few checks -- hence SKIPPED on those checks instead of a script-wide FAIL.
if command -v python3 >/dev/null 2>&1; then
  HAVE_PYTHON3=1
else
  HAVE_PYTHON3=0
fi

# 'wc' is part of coreutils and is rarely missing, but without it check 10 (size) cannot
# measure whether the instructions stay within the ceiling. Same treatment
# as python3 above: this touches only one check, so SKIPPED on that one
# check instead of a script-wide FAIL.
if command -v wc >/dev/null 2>&1; then
  HAVE_WC=1
else
  HAVE_WC=0
fi

# A key from lighthouse.toml via the config reader of this installation.
# Sets CONFIG_VALUE and returns the rc of the reader: 0 value, 1 not set. A
# broken config (rc 2) stops the whole script: every outcome after it would rest
# on a guess. Without python3 it returns 3; the check that needs the value
# then reports SKIPPED.
config_value() {
  local out rc
  [ "$HAVE_PYTHON3" -eq 1 ] || return 3
  out="$(LIGHTHOUSE_REPO_ROOT="$ROOT" python3 "$CONFIG_PY" "$1" 2>&1)"
  rc=$?
  case "$rc" in
    0) CONFIG_VALUE="$out" ;;
    1) CONFIG_VALUE="" ;;
    *) could_not_check "$out" ;;
  esac
  return "$rc"
}

# Word count above which the always-loaded instructions (CLAUDE.md plus the
# AGENTS.md it imports) turn into wallpaper again: `[project]
# instructions_word_ceiling`, default 3000. Empty without python3.
CEILING=""
config_value project.instructions_word_ceiling && CEILING="$CONFIG_VALUE"

# The file with the recorded differences of check 11: `[paths]
# mirror_exceptions`, relative to the repo root. Optional: not set means
# no exceptions. MIRROR_CONFIG is 0 without python3 (cannot be read).
MIRROR_EXCEPTIONS=""
MIRROR_CONFIG=1
config_value paths.mirror_exceptions
case "$?" in
  0)
    case "$CONFIG_VALUE" in
      /*) MIRROR_EXCEPTIONS="$CONFIG_VALUE" ;;
      *) MIRROR_EXCEPTIONS="$ROOT/$CONFIG_VALUE" ;;
    esac
    ;;
  3) MIRROR_CONFIG=0 ;;
esac

# A frontmatter field from a SKILL.md. Empty if the field is missing OR carries
# no value: `upstream:` without a value must count as "missing", otherwise
# the provenance header can be bypassed with an empty line.
#
# Also carries a YAML folded (`>`) or literal (`|`) block scalar: without this
# branch `index($0, f ": ") == 1` on `description: >` reads only the literal
# ">" of that same line, and the real text on the indented continuation lines
# stays unread -- check 4 (overlap) then compares against ">" instead of
# the description and never finds shared trigger words.
#
# `frontmatter_field` works on a file path; `skill_field` is the wrapper
# for a skill directory.
frontmatter_field() {
  local file="$1" field="$2"
  [ -f "$file" ] || return 0
  awk -v f="$field" '
    NR == 1 && $0 == "---" { inside = 1; next }
    inside && collecting {
      if ($0 == "---" || ($0 !~ /^[ \t]/ && $0 != "")) {
        collecting = 0
        print buf
        exit
      }
      line = $0
      sub(/^[ \t]+/, "", line)
      if (line == "") {
        buf = buf "\n"
      } else if (buf == "" || buf ~ /\n$/) {
        buf = buf line
      } else if (fold) {
        buf = buf " " line
      } else {
        buf = buf "\n" line
      }
      next
    }
    inside && $0 == "---" { exit }
    inside && index($0, f ": ") == 1 {
      value = substr($0, length(f) + 3)
      if (value ~ /^[>|][-+0-9]*[ \t]*$/) {
        collecting = 1
        fold = (substr(value, 1, 1) == ">")
        buf = ""
        next
      }
      print value
      exit
    }
    END { if (collecting) print buf }
  ' "$file"
}

skill_field() { frontmatter_field "$1/SKILL.md" "$2"; }

# A key under the `metadata:` block of a SKILL.md. The Agent Skills spec
# allows only six top-level fields (name, description, license, compatibility,
# metadata, allowed-tools); everything else belongs below it. Empty if the key
# is missing OR carries no value: `upstream:` without a value must count as
# "missing", otherwise the provenance header can be bypassed with an empty line.
skill_meta() {
  local file="$1/SKILL.md" field="$2"
  [ -f "$file" ] || return 0
  awk -v f="$field" '
    NR == 1 && $0 == "---" { inside = 1; next }
    inside && $0 == "---" { exit }
    inside && $0 ~ /^metadata:[ \t]*$/ { in_meta = 1; next }
    in_meta && NF && $0 !~ /^[ \t]/ { in_meta = 0 }
    in_meta && match($0, "^[ \t]+" f ": ") { print substr($0, RLENGTH + 1); exit }
  ' "$file"
}

# Directory names (not paths) of the skills under a root, sorted. `-printf` is
# GNU-only, hence the basename loop. A root that does not exist yields nothing
# and is not an error: that judgement belongs to the check, not to the helper.
# NOTE: every `comm` this feeds must ALSO run with LC_ALL=C. If the
# helper sorts on C and comm compares under nl_NL, comm complains on stderr
# ("file 1 is not in sorted order") and the outcome cannot be trusted.
skill_dirs() {
  [ -d "$1" ] || return 0
  find "$1" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | LC_ALL=C sort
}

# Content hash over a skill directory: relative path names plus file content, in
# a fixed order. The relative path is in there on purpose -- then a renamed
# file also counts as a difference. `sort -z` is GNU-only, so the loop reads plain
# lines; a skill directory with a newline in a file name does not exist and would
# already violate the spec.
dir_hash() {
  (
    cd "$1" || exit 1
    find . -type f | LC_ALL=C sort | while IFS= read -r f; do
      printf '%s\n' "$f"
      cat "$f"
    done
  ) | $SHA_CMD | awk '{ print $1 }'
}

# --- Check 1: name claimed twice -------------------------------------------
# Two directories that carry the same `name:` in their frontmatter. Claude
# then loads one and stays silent about the other.
check_duplicate_name() {
  if [ ! -d "$CLAUDE_SKILLS" ]; then
    skip "duplicate names: no .claude/skills, so there is nothing to measure here"
    return 0
  fi
  local name dir duplicate found=0
  local lines=""
  while IFS= read -r dir; do
    name="$(skill_field "$CLAUDE_SKILLS/$dir" name)"
    [ -n "$name" ] && lines+="$name"$'\t'"$dir"$'\n'
  done < <(skill_dirs "$CLAUDE_SKILLS")

  while IFS= read -r duplicate; do
    [ -z "$duplicate" ] && continue
    found=1
    fail "duplicate name '$duplicate' claimed by: $(printf '%s' "$lines" |
      awk -F'\t' -v n="$duplicate" '$1 == n { printf "%s ", $2 }')"
  done < <(printf '%s' "$lines" | cut -f1 | LC_ALL=C sort | uniq -d)

  [ "$found" -eq 0 ] && ok "no names claimed twice"
  return 0
}

# --- Check 2: byte-identical pair ------------------------------------------
# Two skill directories with identical content. The `generate-slides-retro` /
# `generate_slides_retro` case: same md5, and the second costs a full
# description in every session.
check_byte_identical() {
  if [ ! -d "$CLAUDE_SKILLS" ]; then
    skip "byte-identical: no .claude/skills, so there is nothing to measure here"
    return 0
  fi
  local skill_root dir hash duplicate lines found=0
  # Per root, NEVER across the mirror. `.claude/skills/x` and
  # `.agents/skills/x` are a pair by design; for a vendored external
  # skill the mirror is moreover verbatim and thus literally identical. Lumping
  # those together would report every external skill as a duplicate.
  for skill_root in "$CLAUDE_SKILLS" "$AGENTS_SKILLS"; do
    lines=""
    while IFS= read -r dir; do
      hash="$(dir_hash "$skill_root/$dir")"
      lines+="$hash"$'\t'"$dir"$'\n'
    done < <(skill_dirs "$skill_root")

    while IFS= read -r duplicate; do
      [ -z "$duplicate" ] && continue
      found=1
      fail "byte-identical pair in $(basename "$(dirname "$skill_root")")/skills: $(printf '%s' "$lines" |
        awk -F'\t' -v h="$duplicate" '$1 == h { printf "%s ", $2 }')"
    done < <(printf '%s' "$lines" | cut -f1 | LC_ALL=C sort | uniq -d)
  done

  [ "$found" -eq 0 ] && ok "no byte-identical pairs within a skill root"
  return 0
}

# --- Check 3: mirror break --------------------------------------------------
# `.claude/skills/` and `.agents/skills/` should carry the same names. The
# mirror moves in the same reviewed change; this check makes that enforceable.
check_mirror() {
  local missing found=0
  while IFS= read -r missing; do
    [ -z "$missing" ] && continue
    found=1
    fail "mirror missing: '$missing' is not in both .claude/skills/ and .agents/skills/"
  done < <(LC_ALL=C comm -3 <(skill_dirs "$CLAUDE_SKILLS") <(skill_dirs "$AGENTS_SKILLS") | tr -d '\t')

  [ "$found" -eq 0 ] && ok "mirror .claude/skills <-> .agents/skills matches"
  return 0
}

# --- Check 11: mirror content (#981) ---------------------------------------
# Check 3 sees only names; the content of a pair drifted apart unnoticed. This
# check compares every file of every pair. What deliberately differs per harness
# ("Claude Code" versus "Codex", an OpenCode paragraph) is in
# MIRROR_EXCEPTIONS, in the same form as mirror_differences() prints.
# No line numbers in that form: an equal edit on both sides shifts
# the differences but does not change them, and so must not turn red.
#
# A skill directory of which a `SKILL.md` carries the lighthouse marker
# (`metadata.lighthouse`) skips the check: that one belongs to the package, which manages
# its two variants itself, and `lighthouse init` lays them down.

# Prints a header and the deviating lines per differing file (`<` = .claude,
# `>` = .agents). Exit 4 if diff could not compare a pair: that is "could not
# measure", never "no difference".
mirror_differences() {
  local name rel left right out rc left_list right_list
  while IFS= read -r name; do
    [ -z "$name" ] && continue
    if [ -n "$(skill_meta "$CLAUDE_SKILLS/$name" lighthouse)" ] ||
      [ -n "$(skill_meta "$AGENTS_SKILLS/$name" lighthouse)" ]; then
      continue
    fi
    # Outside the loop and with an rc check: a directory that cd or find cannot read
    # otherwise yields an empty list, and that reads as "no difference".
    left_list="$(cd "$CLAUDE_SKILLS/$name" && find . -type f)" || return 4
    right_list="$(cd "$AGENTS_SKILLS/$name" && find . -type f)" || return 4
    while IFS= read -r rel; do
      [ -z "$rel" ] && continue
      left="$CLAUDE_SKILLS/$name/$rel"
      right="$AGENTS_SKILLS/$name/$rel"
      [ -f "$left" ] || left=/dev/null
      [ -f "$right" ] || right=/dev/null
      # `git diff`, not `diff`: GNU diff (Linux) and the FreeBSD diff of macOS
      # line up the same hunks differently, and then this output differs per machine
      # from the recorded file. Git with a fixed algorithm is the same on both.
      out="$(git diff --no-index --no-color --no-ext-diff --no-textconv -U0 \
        --diff-algorithm=myers --no-indent-heuristic -- "$left" "$right")"
      rc=$?
      case "$rc" in
        0) continue ;;
        1) ;;
        *) return 4 ;;
      esac
      printf '=== %s/%s\n' "$name" "$rel"
      # Unified to the `<`/`>` form; the header before the first `@@` is dropped.
      # A binary difference yields no hunks; then show the message itself.
      printf '%s\n' "$out" | awk '
        /^@@/ { hunk = 1; next }
        !hunk { next }
        /^-/  { print "< " substr($0, 2); n++; next }
        /^\+/ { print "> " substr($0, 2); n++ }
        END   { exit n ? 0 : 1 }
      ' || printf '%s\n' "$out"
    done < <(printf '%s\n%s\n' "$left_list" "$right_list" | sed 's|^\./||' | LC_ALL=C sort -u)
  done < <(LC_ALL=C comm -12 <(skill_dirs "$CLAUDE_SKILLS") <(skill_dirs "$AGENTS_SKILLS"))
  return 0
}

check_mirror_content() {
  local current expected="" origin rel
  if [ "$MIRROR_CONFIG" -eq 0 ]; then
    skip "mirror-content: python3 is missing on this machine, so [paths] mirror_exceptions cannot be read"
    return 0
  fi
  if ! current="$(mirror_differences)"; then
    skip "mirror-content: diff could not compare a skill pair, so the content was not measured"
    return 0
  fi
  [ -n "$MIRROR_EXCEPTIONS" ] && [ -f "$MIRROR_EXCEPTIONS" ] &&
    expected="$(cat "$MIRROR_EXCEPTIONS")"

  if [ "$current" = "$expected" ]; then
    ok "mirror-content matches, apart from the recorded exceptions"
    return 0
  fi

  if [ -n "$MIRROR_EXCEPTIONS" ]; then
    rel="${MIRROR_EXCEPTIONS#"$ROOT"/}"
    origin="$rel"
  else
    rel="<file>"
    origin="no recorded exceptions ([paths] mirror_exceptions is not set)"
  fi
  fail "mirror-content differs from $origin (- recorded, + now; < .claude, > .agents):"
  diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$current") | tail -n +3 | sed 's/^/      /'
  printf '      Bring the other side in line. If the difference is deliberate per harness, record it with\n'
  if [ -z "$MIRROR_EXCEPTIONS" ]; then
    printf '      [paths] mirror_exceptions = "<file>" in lighthouse.toml, and then\n'
  fi
  printf '      .lighthouse/scripts/check-instructions.sh --print-mirror-exceptions > %s\n' "$rel"
  return 0
}

# Words that appear in almost every description and so distinguish nothing.
STOPWORDS=" gebruik wanneer welke wordt worden binnen deze dit die daar want
 zodat alleen altijd nooit iemand iedere elke andere anders about after before
 which where their there these those would could should using while whenever
 without within always never asks user users something anything wants needs "

# Trigger words of a description: lowercase, >= 5 characters, not a
# stopword, unique and sorted.
# The list goes through the environment and not through `-v`: the BSD awk that macOS
# ships refuses a newline in a `-v` value ("awk: newline in string")
# and then died, after which this check ran on without a result. `tr` makes it
# one line -- which also repairs a silent second error, because a
# stopword at the end of a line was never found by `index(stop, " " $0 " ")`.
trigger_tokens() {
  printf '%s\n' "$1" |
    tr '[:upper:]' '[:lower:]' |
    tr -cs 'a-z0-9' '\n' |
    LIGHTHOUSE_STOPWORDS="$(printf '%s' "$STOPWORDS" | tr '\n' ' ')" awk '
      BEGIN { stop = ENVIRON["LIGHTHOUSE_STOPWORDS"] }
      length($0) >= 5 && index(stop, " " $0 " ") == 0 { print }
    ' |
    LC_ALL=C sort -u
}

# Does this description carry a bounding clause? Two requirements at once: a
# scope word AS a separate word, and another skill by name. Either one alone
# is exactly the trap CEDA's validator tripped on: a bare scope word fires inside
# a longer word, and a widening phrase enlarges the trigger instead of bounding it.
# lighthouse: allow-language — the Dutch examples: `buiten` inside "buitenwereld", "gebruik dan deze skill"
has_exclusion_clause() {
  local desc own word scope=0
  desc="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  own="$2"
  for word in "let op" "distinct from" "in plaats van" "gebruik dan" \
    "niet de skill" "niet voor" "nooit voor" "instead" "not for"; do
    if printf '%s' "$desc" |
      grep -qE "(^|[^a-z])${word// /[[:space:]]}([^a-z]|\$)"; then
      scope=1
      break
    fi
  done
  [ "$scope" -eq 1 ] || return 1

  # A hyphenated token of >= 6 characters that is not the own name.
  printf '%s' "$desc" |
    grep -oE '[a-z0-9]+(-[a-z0-9]+)+' |
    awk -v own="$own" '
      length($0) >= 6 && $0 != own { found = 1 }
      END { exit found ? 0 : 1 }'
}

# --- Check 4: overlap without a bounding clause ----------------------------
# Two descriptions that share the same trigger words without either one
# bounding. The model then picks at random; the fix is a clause in that
# description, not a higher threshold.
OVERLAP_THRESHOLD=5

check_overlap() {
  local -a names=() descs=()
  local dir i j shared found=0
  while IFS= read -r dir; do
    names+=("$dir")
    descs+=("$(skill_field "$CLAUDE_SKILLS/$dir" description)")
  done < <(skill_dirs "$CLAUDE_SKILLS")

  for ((i = 0; i < ${#names[@]}; i++)); do
    for ((j = i + 1; j < ${#names[@]}; j++)); do
      shared="$(LC_ALL=C comm -12 \
        <(trigger_tokens "${descs[i]}") \
        <(trigger_tokens "${descs[j]}") | wc -l)"
      [ "$shared" -lt "$OVERLAP_THRESHOLD" ] && continue
      has_exclusion_clause "${descs[i]}" "${names[i]}" && continue
      has_exclusion_clause "${descs[j]}" "${names[j]}" && continue
      found=1
      fail "overlap without a bounding clause: '${names[i]}' and '${names[j]}' share $shared trigger words"
    done
  done

  [ "$found" -eq 0 ] && ok "no unbounded overlap between descriptions"
  return 0
}

# --- Check 5: shadowing personal skill --------------------------------------
# Personal skills OVERSHADOW project skills of the same name -- the reverse of
# settings.json. A name that is in both means the project version
# never loads, and that happens silently.
check_shadow() {
  if [ ! -d "$PERSONAL_SKILLS" ]; then
    skip "shadow: '$PERSONAL_SKILLS' does not exist on this machine, so there is nothing to measure here"
    return 0
  fi

  local name found=0
  while IFS= read -r name; do
    [ -z "$name" ] && continue
    found=1
    fail "shadow: '$name' is in $PERSONAL_SKILLS and in .claude/skills/; the personal version wins"
  done < <(LC_ALL=C comm -12 <(skill_dirs "$PERSONAL_SKILLS") <(skill_dirs "$CLAUDE_SKILLS"))

  [ "$found" -eq 0 ] && ok "no personal skill shadows a project skill"
  return 0
}

# --- Check 7: every rule is path-scoped --------------------------------------
# A rule without `paths:` loads every session and so costs the same as
# CLAUDE.md. The gain of rules lies entirely in the scoping; without that rule it
# is CLAUDE.md with extra steps.
check_scoping() {
  local rules="$ROOT/.claude/rules" file found=0

  if [ ! -d "$rules" ]; then
    skip "scoping: '$rules' does not exist, so there is no rule to check here"
    return 0
  fi

  while IFS= read -r file; do
    [ -z "$file" ] && continue
    # `paths:` must be in the frontmatter (not somewhere in the body) AND carry a
    # non-empty value -- either inline (`paths: src/**`), or as a
    # list on the next line. A bare `paths:` or `paths: []`
    # matches no path at all and so counts as "missing", just like elsewhere in this
    # script (skill_field()/skill_meta() also treat an empty field as
    # absent).
    if ! awk '
      NR == 1 && $0 == "---" { inside = 1; next }
      inside && $0 == "---" { exit }
      inside && index($0, "paths:") == 1 {
        rest = substr($0, 7)
        gsub(/^[ \t]+/, "", rest)
        gsub(/[ \t]+$/, "", rest)
        if (rest != "" && rest != "[]") { found = 1; exit }
        # Empty lines and YAML comment lines between `paths:` and the list
        # are valid YAML and must not make the check fail -- hence a
        # loop that skips them instead of looking at exactly one next line.
        # The edge case remains: the closing "---" is neither empty nor
        # comment, so it stops the loop without setting found.
        while ((getline nextline) > 0) {
          trimmed = nextline
          gsub(/^[ \t]+/, "", trimmed)
          gsub(/[ \t]+$/, "", trimmed)
          if (trimmed == "" || trimmed ~ /^#/) { continue }
          # "- " (hyphen + space), not a bare "-": otherwise the closing
          # frontmatter delimiter "---" also reads as a list item.
          if (substr(trimmed, 1, 2) == "- ") { found = 1 }
          break
        }
        exit
      }
      END { exit found ? 0 : 1 }
    ' "$file"; then
      found=1
      fail "scoping: '$(basename "$file")' has no paths: block with a non-empty value in its frontmatter"
    fi
  done < <(find "$rules" -maxdepth 1 -type f -name '*.md' | LC_ALL=C sort)

  [ "$found" -eq 0 ] && ok "every rule is path-scoped"
  return 0
}

# Is this hook path registered in settings.json -- specifically in the
# `hooks` block (never in `permissions.deny` or elsewhere), and as a whole
# path segment in a `command` field, not as an accidental substring of a
# longer file name (`streng.sh.disabled` must not count for
# `streng.sh`). `grep -qF` on the whole file proved only "occurs as a
# substring", not "is registered" -- hence the JSON parse.
#
# Three exit codes, not two: 0 found, 1 not found (settings.json was
# valid JSON and an object), 2 settings.json is broken or not an object.
# check_pairing() uses exit code 2 to give its own, distinguishing message
# -- the cause is then settings.json, not the skill body.
hook_registered() {
  local settings="$1" hook="$2"
  python3 -c '
import json, re, sys

settings_path, hook = sys.argv[1], sys.argv[2]

# Path boundaries on both sides: the character right before and right after the match must
# not be a path/file name character. That rejects both a longer prefix
# (".claude/hooks/other-streng.sh") and a longer suffix
# (".claude/hooks/streng.sh.disabled").
pattern = re.compile(r"(?<![A-Za-z0-9_.-])" + re.escape(hook) + r"(?![A-Za-z0-9_.-])")


def contains_match(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "command" and isinstance(value, str) and pattern.search(value):
                return True
            if contains_match(value):
                return True
        return False
    if isinstance(node, list):
        return any(contains_match(item) for item in node)
    return False


try:
    with open(settings_path, encoding="utf-8") as f:
        data = json.load(f)
    found = contains_match(data.get("hooks", {}))
except (OSError, json.JSONDecodeError, AttributeError):
    # AttributeError: settings.json is valid JSON but not an object (a
    # list or a bare string, for example) -- .get() then does not exist.
    # Exit code 2, not 1: this is a different finding from "hook not found".
    sys.exit(2)

sys.exit(0 if found else 1)
' "$settings" "$hook"
}

# --- Check 8: pairing --------------------------------------------------------
# Hook and skill/rule are one rule at two moments. `binding: hard` is the
# claim that an enforcement exists; this check makes that claim testable.
# No fourth frontmatter field: the skill names its hook in the body, and that
# hook must exist AND be registered in settings.json. A hook script that is
# registered nowhere never runs. The path may be in a subdirectory of
# `.claude/hooks/`: `lighthouse init` lays down its own in `.claude/hooks/lighthouse/`.
check_pairing() {
  local settings="$ROOT/.claude/settings.json"
  local dir binding hooks hook covered found=0 hard=0 skipped=0
  local skip_reason="" settings_exists=1 settings_broken rc

  [ -f "$settings" ] || settings_exists=0

  while IFS= read -r dir; do
    binding="$(skill_meta "$CLAUDE_SKILLS/$dir" binding)"
    [ "$binding" = "hard" ] || continue
    hard=$((hard + 1))

    hooks="$(grep -oE '\.claude/hooks/([A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.sh' \
      "$CLAUDE_SKILLS/$dir/SKILL.md" 2>/dev/null | LC_ALL=C sort -u)"

    # No hook path named at all in the body: that is a measured finding,
    # independent of settings.json or python3. Only when a path
    # IS named does registration become the remaining question.
    if [ -z "$hooks" ]; then
      found=1
      fail "pairing: '$dir' carries 'binding: hard' but names no counterpart that exists and is registered in settings.json"
      continue
    fi

    # `continue`, not `return 0`: a missing precondition makes only the
    # registration of THIS skill unmeasurable. A `return 0` here used to break off
    # the whole loop and left every alphabetically later hard skill unmeasured --
    # including a skill without any hook path, which IS a measured FAIL.
    if [ "$settings_exists" -eq 0 ]; then
      skipped=1
      skip_reason="pairing: '$settings' does not exist, so registration cannot be checked here"
      continue
    fi

    if [ "$HAVE_PYTHON3" -eq 0 ]; then
      skipped=1
      skip_reason="pairing: python3 is missing on this machine, so registration cannot be checked here"
      continue
    fi

    covered=0
    settings_broken=0
    while IFS= read -r hook; do
      [ -z "$hook" ] && continue
      [ -f "$ROOT/$hook" ] || continue
      hook_registered "$settings" "$hook"
      rc=$?
      if [ "$rc" -eq 0 ]; then
        covered=1
        break
      elif [ "$rc" -eq 2 ]; then
        settings_broken=1
      fi
    done <<<"$hooks"

    if [ "$covered" -eq 0 ]; then
      found=1
      # A broken/not-an-object settings.json is a FAIL, not SKIPPED: the
      # file exists and is readable, so this IS measurable -- and the cause
      # is settings.json, not the skill body. Without this branch the reader got
      # the same "names no counterpart" message and went to repair the skill
      # body instead of settings.json.
      if [ "$settings_broken" -eq 1 ]; then
        fail "pairing: '$settings' is not valid JSON (or not an object), so '$dir' cannot be checked for a registered hook"
      else
        fail "pairing: '$dir' carries 'binding: hard' but names no counterpart that exists and is registered in settings.json"
      fi
    fi
  done < <(skill_dirs "$CLAUDE_SKILLS")

  if [ "$hard" -eq 0 ]; then
    ok "no skill claims 'binding: hard'"
    return 0
  fi

  # Unconditional: an unmeasurable skill must always be reported
  # loudly, also next to a FAIL elsewhere. An `elif` on found used to
  # suppress this line as soon as even one FAIL fell.
  [ "$skipped" -eq 1 ] && skip "$skip_reason"
  [ "$found" -eq 0 ] && [ "$skipped" -eq 0 ] && ok "every binding: hard has a registered hook"
  return 0
}

# --- Check 14: hook names -----------------------------------------------------
# A lighthouse hook is named after what it does. `-gate` can block, so it sits on
# PreToolUse, Stop or SubagentStop; `-check` reports after the fact and never
# sits on Stop or SubagentStop. Only commands that carry `.claude/hooks/lighthouse/`
# count: a hook of the project itself, or `npx ...`, is not ours to name.
# A broken or non-object settings.json makes the python part exit non-zero.
check_hook_names() {
  local settings="$ROOT/.claude/settings.json"
  local out rc line found=0

  if [ ! -f "$settings" ]; then
    skip "hook-names: '$settings' does not exist, so hook names cannot be checked here"
    return 0
  fi
  if [ "$HAVE_PYTHON3" -eq 0 ]; then
    skip "hook-names: python3 is missing on this machine, so hook names cannot be checked here"
    return 0
  fi

  out="$(python3 -c '
import json, re, sys

with open(sys.argv[1], encoding="utf-8") as f:
    data = json.load(f)
hooks = data.get("hooks", {})
if not isinstance(hooks, dict):
    raise AttributeError
path = re.compile(r"\.claude/hooks/lighthouse/(?:[A-Za-z0-9_-]+/)*([A-Za-z0-9_-]+)\.sh")
events = {}


def walk(node, event):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "command" and isinstance(value, str):
                match = path.search(value)
                if match:
                    events.setdefault(match.group(1), set()).add(event)
            else:
                walk(value, event)
    elif isinstance(node, list):
        for item in node:
            walk(item, event)


for event, entries in hooks.items():
    walk(entries, event)

for name in sorted(events):
    seen = events[name]
    if name.endswith("-gate"):
        if not seen & {"PreToolUse", "Stop", "SubagentStop"}:
            print("gate " + name)
    elif name.endswith("-check"):
        if seen & {"Stop", "SubagentStop"}:
            print("check " + name)
    else:
        print("suffix " + name)
' "$settings" 2>/dev/null)"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    fail "hook-names: '$settings' is not valid JSON (or not an object)"
    return 0
  fi

  while IFS= read -r line; do
    [ -z "$line" ] && continue
    found=1
    case "$line" in
      suffix\ *) fail "hook-names: '${line#* }' ends on neither -gate nor -check" ;;
      gate\ *) fail "hook-names: '${line#* }' is a gate but no registration is on PreToolUse, Stop or SubagentStop" ;;
      check\ *) fail "hook-names: '${line#* }' is a check but is registered on Stop or SubagentStop" ;;
    esac
  done <<<"$out"

  [ "$found" -eq 0 ] && ok "every lighthouse hook is named after what it does"
  return 0
}

# --- Check 10: size of CLAUDE.md + AGENTS.md --------------------------------
# Without a counter the word count just creeps up again. CLAUDE.md is an
# `@AGENTS.md` import plus the Claude remainder: counting only CLAUDE.md would be a
# placebo, so the check counts what Claude Code actually loads -- both.
check_size() {
  local claude="$ROOT/CLAUDE.md" agents="$ROOT/AGENTS.md"
  local label="CLAUDE.md" words=0 n file
  local -a files=("$claude")

  if [ "$HAVE_WC" -eq 0 ]; then
    skip "size: 'wc' is missing on this machine, so CLAUDE.md cannot be counted here"
    return 0
  fi

  if [ ! -f "$claude" ]; then
    skip "size: '$claude' does not exist, so there is nothing to count here"
    return 0
  fi

  if [ -z "$CEILING" ]; then
    skip "size: python3 is missing on this machine, so [project] instructions_word_ceiling cannot be read"
    return 0
  fi

  if [ -f "$agents" ]; then
    files+=("$agents")
    label="CLAUDE.md + AGENTS.md"
  fi

  for file in "${files[@]}"; do
    if [ ! -r "$file" ]; then
      skip "size: '$file' is not readable, so there is nothing to count here"
      return 0
    fi

    # `wc -w` on an unreadable file yields an empty stdout: without this
    # validation `[ "" -gt "$CEILING" ]` throws "integer expression expected" on
    # stderr, the if branch silently falls through to ok(), and the gate reports "0 words,
    # within the ceiling" about a file it did not measure.
    n="$(wc -w <"$file" 2>/dev/null | tr -d '[:space:]')"
    if ! [[ "$n" =~ ^[0-9]+$ ]]; then
      skip "size: 'wc' could not count '$file', so there is nothing to count here"
      return 0
    fi
    words=$((words + n))
  done

  if [ "$words" -gt "$CEILING" ]; then
    fail "size: $label counts $words words, ceiling is $CEILING"
    return 0
  fi

  ok "$label counts $words words, within the ceiling of $CEILING"
  return 0
}

if [ "${1:-}" = "--print-mirror-exceptions" ]; then
  mirror_differences || {
    printf 'SKIPPED  diff could not compare a skill pair\n' >&2
    exit 4
  }
  exit 0
fi

check_duplicate_name
check_byte_identical
check_mirror
check_mirror_content
check_overlap
check_shadow
check_scoping
check_pairing
check_hook_names
check_size

if [ "$findings" -gt 0 ]; then
  printf '\n%d finding(s).\n' "$findings"
  exit 1
fi
exit 0

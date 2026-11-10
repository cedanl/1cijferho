#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# Run one adversarial review round through an external Codex session.
#
#   review.sh [--prompt F] [--schema F] [--context PATH]... <DOC> <ROUND> <OUT> [PRIOR] [LEVEL]
#
#     DOC    path to the document under review (spec or plan), relative to the repo root
#     ROUND  round number (1, 2, 3)
#     OUT    path where the JSON result lands (.events and .err land next to it)
#     PRIOR  path to the previous round, or "none" (default none). If a sidecar
#            `<name without .json>.decisions.json` lies next to it, the prior block names
#            it; without a sidecar the block is the original one, byte for byte.
#     LEVEL  reasoning effort; empty = from config.toml. Use `max` only when a missed
#            blocker is expensive. Never `xhigh`: measured more expensive than `high`
#            without one unique finding.
#
#     --prompt   prompt template; default reviewer-prompt.md next to this script
#     --schema   output schema; default findings.schema.json next to this script
#     --context  extra context path for {{CONTEXT_PATHS}}, repeatable; repo-relative
#                or absolute, must exist
#
# Flags may stand anywhere. Without flags and without a sidecar the prompt is the
# original one, byte for byte: the spec review notices nothing of the plan review.
#
# One fixed command shape, so it can be allowlisted (`permissions.json`) and a round can
# run in the background without an approval prompt in between.
#
# The repo root comes from LIGHTHOUSE_REPO_ROOT, otherwise from git; never from this
# file's own path.
#
# Before Codex the runner runs `.lighthouse/scripts/codex_usage.py budget` and writes the
# decision to `<OUT>.budget.json`. If the budget is exhausted beforehand (decision
# `fallback`), the runner stops with exit 6 and stderr `CODEX_BUDGET: fallback — <reason>`,
# without `codex exec`. `LIGHTHOUSE_CODEX_BUDGET_CMD` replaces that command (test seam,
# split on spaces). A check that fails or gives no JSON object counts as not measurable:
# the round goes on.
#
# Exit: 0 ok | 2 usage error | 3 codex missing | 4 not logged in | 5 run failed | 6 budget exhausted beforehand
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROMPT_FILE="$SKILL_DIR/reviewer-prompt.md"
SCHEMA_FILE="$SKILL_DIR/findings.schema.json"
CONTEXTS=""
POS=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --prompt | --schema | --context)
      [ "$#" -ge 2 ] || { echo "$1 needs a value" >&2; exit 2; }
      case "$1" in
        --prompt) PROMPT_FILE="$2" ;;
        --schema) SCHEMA_FILE="$2" ;;
        --context) CONTEXTS="${CONTEXTS}${2}"$'\n' ;;
      esac
      shift 2
      ;;
    --*) echo "unknown flag: $1" >&2; exit 2 ;;
    *) POS+=("$1"); shift ;;
  esac
done
# `${POS[@]+...}`: bash 3.2 (the macOS default) fails under `set -u` on an empty
# array expansion.
set -- ${POS[@]+"${POS[@]}"}

SPEC="${1:-}"
ROUND="${2:-}"
OUT="${3:-}"
PRIOR="${4:-none}"
LEVEL="${5:-}"

[ -n "$SPEC" ] && [ -n "$ROUND" ] && [ -n "$OUT" ] || {
  echo "usage: review.sh [--prompt F] [--schema F] [--context PATH]... <DOC> <ROUND> <OUT> [PRIOR] [LEVEL]" >&2; exit 2; }
[ -f "$PROMPT_FILE" ] || { echo "prompt template not found: $PROMPT_FILE" >&2; exit 2; }
[ -f "$SCHEMA_FILE" ] || { echo "schema not found: $SCHEMA_FILE" >&2; exit 2; }

command -v codex >/dev/null || { echo "CODEX_MISSING: no codex in PATH" >&2; exit 3; }

REPO="${LIGHTHOUSE_REPO_ROOT:-$(git rev-parse --show-toplevel)}"
[ -f "$REPO/$SPEC" ] || { echo "document not found under the repo root: $REPO/$SPEC" >&2; exit 2; }
while IFS= read -r ctx; do
  [ -z "$ctx" ] && continue
  case "$ctx" in
    /*) [ -f "$ctx" ] || { echo "context path not found: $ctx" >&2; exit 2; } ;;
    *) [ -f "$REPO/$ctx" ] || { echo "context path not found under the repo root: $REPO/$ctx" >&2; exit 2; } ;;
  esac
done <<<"$CONTEXTS"

# Clean CODEX_HOME: the reviewer inherits no marketplaces, plugins, MCP servers,
# hooks or agents. Those cost ~8k tokens and undermine the clean context this skill
# rests on. `--ignore-user-config` is not enough: it skips only config.toml and still
# loads $CODEX_HOME/agents/.
CH="${CODEX_SPECREVIEW_HOME:-$HOME/.codex-specreview}"
mkdir -p "$CH"
[ -f "$HOME/.codex/auth.json" ] || { echo "CODEX_UNAUTHENTICATED: ~/.codex/auth.json missing" >&2; exit 4; }
cp -f "$HOME/.codex/auth.json" "$CH/auth.json"   # takes a refreshed token along
[ -s "$CH/config.toml" ] || printf 'model = "gpt-5.6-sol"\nmodel_reasoning_effort = "high"\n' > "$CH/config.toml"

mkdir -p "$(dirname "$OUT")"

# Budget beforehand. No heredoc inside `$(...)`: bash 3.2 (macOS) parses that wrongly.
set +e
if [ -n "${LIGHTHOUSE_CODEX_BUDGET_CMD:-}" ]; then
  read -r -a BUDGET_ARGV <<<"$LIGHTHOUSE_CODEX_BUDGET_CMD"
  "${BUDGET_ARGV[@]}" < /dev/null > "$OUT.budget.json"
else
  python3 "$REPO/.lighthouse/scripts/codex_usage.py" budget \
    < /dev/null > "$OUT.budget.json"
fi
BUDGET_CODE=$?
python3 - "$OUT.budget.json" "$BUDGET_CODE" <<'PY'
import json
import pathlib
import sys

path, code = pathlib.Path(sys.argv[1]), int(sys.argv[2])
try:
    doc = json.loads(path.read_text(encoding="utf-8"))
except (OSError, ValueError):
    doc = None
if code in (0, 6) and isinstance(doc, dict):
    if code == 6:
        reason = doc.get("reason", "")
        sys.stderr.buffer.write(f"CODEX_BUDGET: fallback — {reason}\n".encode())
    raise SystemExit(code)
# The fields of codex_usage.py's budget answer, in its JSON order.
keys = (
    "decision", "reason", "window", "reset_at", "session_pct",
    "week_pct", "source", "lower_bound", "thresholds",
)
synth = dict.fromkeys(keys)
synth["decision"] = "not_measurable"
synth["reason"] = f"budget check failed: exit {code}"
path.write_text(json.dumps(synth, ensure_ascii=False) + "\n", encoding="utf-8")
PY
BUDGET_DECISION=$?
set -e
if [ "$BUDGET_DECISION" -eq 6 ]; then
  exit 6
fi

PROMPT_TMP="$(mktemp)"
trap 'rm -f "$PROMPT_TMP"' EXIT

python3 - "$PROMPT_FILE" "$SPEC" "$REPO" "$ROUND" "$PRIOR" "$CONTEXTS" > "$PROMPT_TMP" <<'PY'
import pathlib
import sys

tmpl, spec, repo, round_no, prior, contexts = sys.argv[1:7]
text = pathlib.Path(tmpl).read_text()
body = text.split("---BEGIN PROMPT---", 1)[1].split("---END PROMPT---", 1)[0].strip()
context_paths = [c for c in contexts.split("\n") if c]

prior_block = ""
if prior != "none":
    prior_path = pathlib.Path(prior)
    sidecar = prior_path.with_name(prior_path.name.removesuffix(".json") + ".decisions.json")
    if sidecar.is_file():
        prior_block = (
            f"Previous review: `{prior}`. The author's decisions are in `{sidecar}`:\n"
            "one entry per finding, keyed by `finding_id` = `r<round>-f<index>` (index =\n"
            "1-based position in that review's `findings` array), with `decision` and\n"
            "`reason`. Check specifically which accepted findings were answered in prose\n"
            "but not actually resolved in the document. Do not re-raise a finding the\n"
            "author rejected with a stated reason — instead say whether that reasoning is\n"
            "wrong, and why."
        )
    else:
        prior_block = (
            f"Previous review: `{prior}`. The author's responses are recorded there.\n"
            "Check specifically which findings were answered in prose but not actually\n"
            "resolved in the spec. Do not re-raise a finding the author rejected with a\n"
            "stated reason — instead say whether that reasoning is wrong, and why."
        )

if context_paths and "{{CONTEXT_PATHS}}" not in body:
    raise SystemExit("--context given, but the template has no {{CONTEXT_PATHS}}")
context_block = "\n".join(f"- {c}" for c in context_paths) or "(none)"

body = (
    body.replace("{{SPEC_PATH}}", spec)
    .replace("{{DOC_PATH}}", spec)
    .replace("{{CONTEXT_PATHS}}", context_block)
    .replace("{{REPO}}", repo)
    .replace("{{ROUND}}", round_no)
    .replace("{{PRIOR_REVIEW_BLOCK}}", prior_block)
)
if "{{" in body:
    raise SystemExit("unsubstituted placeholder in the prompt template")
print(body)
PY

# The level flag through positional parameters, not an array: bash 3.2 (the macOS
# default) fails under `set -u` on an empty array expansion.
if [ -n "$LEVEL" ]; then
  set -- -c "model_reasoning_effort=$LEVEL"
else
  set --
fi

set +e
CODEX_HOME="$CH" codex exec \
  --sandbox read-only \
  "$@" \
  --cd "$REPO" \
  --output-schema "$SCHEMA_FILE" \
  -o "$OUT" \
  --json \
  "$(cat "$PROMPT_TMP")" \
  < /dev/null > "$OUT.events" 2> "$OUT.err"
CODE=$?
set -e

if [ "$CODE" -ne 0 ] || [ ! -s "$OUT" ]; then
  echo "CODEX_FAILED (exit $CODE). Last lines of $OUT.err:" >&2
  tail -15 "$OUT.err" >&2
  exit 5
fi

python3 - "$OUT" "$CH/config.toml" <<'PY'
import json
import pathlib
import sys

out, config = sys.argv[1], sys.argv[2]
doc = json.loads(pathlib.Path(out).read_text())
missing = {"review_markdown", "status", "findings"} - set(doc)
if missing:
    raise SystemExit(f"incomplete result, missing {sorted(missing)}")
model = next(
    (line.split('"')[1] for line in pathlib.Path(config).read_text().splitlines()
     if line.startswith("model =")),
    "unknown",
)
sev = {s: sum(1 for f in doc["findings"] if f["severity"] == s) for s in ("BLOCKER", "MAJOR", "MINOR")}
print(f"ok | reviewer: {model} | status: {doc['status']} | "
      f"{len(doc['findings'])} findings ({sev['BLOCKER']}B/{sev['MAJOR']}M/{sev['MINOR']}m) | raw: {out}")
PY

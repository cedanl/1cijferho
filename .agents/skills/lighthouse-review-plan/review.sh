#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# Run one adversarial plan review round: the runner of the spec review, with the
# plan prompt and the plan schema of this directory.
#
#   review.sh <PLAN> <ROUND> <OUT> [PRIOR] [LEVEL] [--context <SPEC>]...
#
# One runner, no second copy of the Codex call. The relative path to the sibling skill
# works in .claude/skills/ and in .agents/skills/.
# Exit codes: those of ../lighthouse-review-spec/review.sh.
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SKILL_DIR/../lighthouse-review-spec/review.sh" \
  --prompt "$SKILL_DIR/reviewer-prompt.md" \
  --schema "$SKILL_DIR/findings.schema.json" \
  "$@"

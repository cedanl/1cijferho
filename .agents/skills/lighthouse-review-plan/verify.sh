#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# Run one verification round: the same runner as review.sh, with the verifier prompt
# and the verification schema of this directory (from round 2 on).
#
#   verify.sh <PLAN> <ROUND> <OUT> [PRIOR] [LEVEL] [--context <SPEC-or-diff>]...
#
# One runner, no second copy of the Codex call. The relative path to the sibling skill
# works in .claude/skills/ and in .agents/skills/.
# Exit codes: those of ../lighthouse-review-spec/review.sh.
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SKILL_DIR/../lighthouse-review-spec/review.sh" \
  --prompt "$SKILL_DIR/verifier-prompt.md" \
  --schema "$SKILL_DIR/verification.schema.json" \
  "$@"

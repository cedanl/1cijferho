#!/usr/bin/env bash
# lighthouse: core@v0.8.1
# Run one verification round: the same runner as review.sh, with the verifier prompt
# and the verification schema of this directory (from round 2 on).
#
#   verify.sh <SPEC> <ROUND> <OUT> [PRIOR] [LEVEL] [--context <diff>]...
#
# One runner, no second copy of the Codex call.
# Exit codes: those of review.sh (in this directory).
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SKILL_DIR/review.sh" \
  --prompt "$SKILL_DIR/verifier-prompt.md" \
  --schema "$SKILL_DIR/verification.schema.json" \
  "$@"

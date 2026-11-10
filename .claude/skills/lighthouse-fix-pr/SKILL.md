---
name: lighthouse-fix-pr
description: 'Use when an existing pull request in this repo is red or has merge conflicts, or when a green PR carries an unprocessed roborev review comment (findings without a `roborev-verwerkt` marker) — "fix pr 596", "fix 595", "612 has conflicts, fix it", "627 pr has errors", "PR 585 is red". Opens with one fixed diagnosis (status, checks, failure log, worktree) and works in the worktree of the head branch. NOTE — this skill stops as soon as the PR is green (and a roborev review is processed): closing the issue is lighthouse-close-issue, a review of the diff on content is caveman-review, and starting new work on an issue is lighthouse-start-issue. Not for a PR that only needs merging.'
metadata:
  lighthouse: "core@v0.8.1"
  binding: default
  origin: own
---

# Fix PR Skill

From "fix 596" to a green PR. The skill adds no skill — it replaces the run-up, which in eight
measured turns asked the same four questions in five to eight separate calls.

## Domain

In scope: an **existing** PR in this repo (`forge.repo`) that is red or conflicts — diagnosis, the
worktree of the head branch, the fix, getting it green.

Second entry: a **green** PR with an unprocessed roborev review (section 6, only if present) —
the user starts it: "process the roborev review on 1407". The dispatcher (controller run, if
present) starts this skill only on a red PR after a finished run, never for a roborev review on
a green PR.

Not here: closing the issue (`/lighthouse-close-issue`), a review of the diff on content (a review
skill such as `caveman:caveman-review`), starting new work on an issue (`/lighthouse-start-issue`),
opening a PR (`/lighthouse-write-pr`), and the merge itself — that is a human decision.

## 1. Diagnosis in one block

One Bash call, read-only, `dangerouslyDisableSandbox: true`. Replace `<nr>`.

```bash
set -uo pipefail
N=<nr>
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2

PR=$(gh pr view "$N" --repo "$R" --json number,title,state,isDraft,mergeable,mergeStateStatus,headRefName,url 2>&1)
RC=$?
if [ "$RC" -ne 0 ]; then
  echo "no PR #$N — is it an issue? gh said: $PR"
  exit 1
fi
printf '%s' "$PR" | jq -r '"PR #\(.number) [\(.state)] draft=\(.isDraft) merge=\(.mergeable)/\(.mergeStateStatus)\nbranch \(.headRefName)\n\(.url)\n\(.title)"'
B=$(printf '%s' "$PR" | jq -r .headRefName)

echo "=== checks ==="
gh pr checks "$N" --repo "$R" || true

if [ -z "$B" ] || [ "$B" = "null" ]; then
  echo "no headRefName in the PR JSON — stop here"
  exit 1
fi

echo "=== worktree ==="
git worktree list | grep -F -- "[$B]" || echo "NO worktree on $B"

echo "=== failure log ==="
RUNS=$(gh pr checks "$N" --repo "$R" --json bucket,link --jq '.[] | select(.bucket=="fail") | .link' 2>/dev/null | grep -oE 'runs/[0-9]+' | cut -d/ -f2 | sort -u)
if [ -z "$RUNS" ]; then
  echo "no fail check — red is then a conflict, a pending check or a run that did not start"
fi
for r in $RUNS; do
  echo "--- run $r ---"
  gh run view "$r" --repo "$R" --log-failed 2>&1 \
    | grep -aE '##\[error\]|FAILED | E {2,}|Process completed with exit code|Error: ' \
    | tail -15 || echo "no failure line found — see 'A red job without a failed test'"
done

echo "=== roborev ==="
if test -e .roborev.toml; then
  RB=$(gh api "repos/$R/issues/$N/comments" --paginate --jq '.[] | select((.user.login == "github-actions[bot]" or .user.login == "github-actions") and (.body | contains("roborev-pr-comment"))) | .body | @json' 2>/dev/null) \
    && RB=$(printf '%s\n' "$RB" | tail -1 | jq -r '. // empty' 2>/dev/null) || RB=""
  VW=$(gh api "repos/$R/issues/$N/comments" --paginate --jq '.[] | .body | split("\n")[] | select(contains("<!-- roborev-verwerkt:"))' 2>/dev/null) || VW=""
  [ -n "$RB" ] && printf '%s\n' "$RB" || echo "no roborev comment"
  [ -n "$VW" ] && printf '%s\n' "$VW" || echo "no roborev-verwerkt marker"
else
  echo "roborev: skipped — no .roborev.toml"
fi
```

The `=== roborev ===` part runs only if present (a `.roborev.toml` in the repo root) and shows the
**full** body of the last roborev comment, not only heading and verdict: section 6 works per
finding (path, line, severity), and a controller run (if present) gets only the PR number. Every
comment goes as one `@json` line through `--paginate`, so `tail -1` takes the last one across page
boundaries.

Four things in this block are measured, not taste (the `=== roborev ===` part follows the same `|| echo` rule):

- **A guard right after `gh pr view`.** If that call fails — `<nr>` was an issue, not a PR — the
  block stops with a message instead of reading on with an empty `$B`: without a guard an empty
  `grep -F -- ""` afterwards matches every line, and the failure-log part wrongly reports "no fail
  check".
- **`|| true` after `gh pr checks`.** That gives exit 1 on failing and exit 8 on pending checks.
  Without the guard the block breaks off in exactly the case it is meant for.
- **`grep` on the error markers, not `tail -60`.** Measured on one run: the log was 8,284 lines,
  the failed test was on line 2,141, and the last 60 lines were `Post job cleanup`. The grep gave
  four lines on that same log, with the cause in them.
- **Every searching step has an `|| echo` branch.** A grep that finds nothing gives exit 1, and
  with `pipefail` "no output" then reads as "crashed".

## 2. Work in the worktree of the head branch

Never in the main checkout, and not in a fresh worktree from the default branch: the work belongs
on the head branch of the PR. `EnterWorktree` and `superpowers:using-git-worktrees` make a **new**
branch from `origin/<default branch>` and are therefore wrong here.

```bash
B=<head-branch>
ROOT=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")
WT="$ROOT/.claude/worktrees/$(printf '%s' "$B" | tr / -)"
if git worktree list --porcelain | grep -qxF "branch refs/heads/$B"; then
  git worktree list --porcelain | grep -B2 -xF "branch refs/heads/$B" | head -1   # exists already
elif git rev-parse --verify --quiet "$B" >/dev/null; then
  git worktree add "$WT" "$B"
else
  git fetch origin "$B" && git worktree add "$WT" --track -b "$B" "origin/$B" \
    || echo "could not find $B on origin — does the branch still exist on the remote?"
fi
```

`ROOT` is the main checkout, also when this block runs inside another worktree: `--git-common-dir`
always points at the shared `.git`, never at the worktree's own. Without that anchoring
`git worktree add` makes a nested worktree under the current worktree, where cleanup rules do not
look. The `-x` on both `grep`s matches the whole line, not a substring: `refs/heads/issue-664`
otherwise also matches `refs/heads/issue-664-fix-pr-skill`.

Then prefix **every** command with `cd <worktree>` or `git -C <worktree>`. The cwd of the
persistent shell drifts back to the main checkout in the middle of a session, and a `sed -i` then
hits the wrong tree without warning.

## 3. The gates, and how to reproduce them locally

The red checks are steps of the CI workflows in `.github/workflows/`; the step name says which
gate failed.

| Red check | What fails | Reproduce locally |
|---|---|---|
| Tests | the test command of the repo | `python3 .lighthouse/lib/lighthouse_lib/config.py test_gate.command` prints it; run it with `bash -c` (rc 1: no test command configured, read it from the workflow) |
| Quality ratchet (ruff) | a ruff counter in the baseline went up; pack/python, only if present | `test -e .lighthouse/scripts/quality/ratchet.py && uv run python .lighthouse/scripts/quality/ratchet.py ruff` |
| Expo lockfiles | patch drift in the Expo lockfiles; pack/expo, only if present | `/lighthouse-refresh-expo-lockfiles` |
| Any other step | lint, type check, scan, licence | the command of that step in `.github/workflows/`; a step that cannot run locally (a signature) the contributor fixes themselves |

Gates before CI break the commit or the push, not the PR: `.claude/hooks/lighthouse/bash-write-check.sh`
(per edit, with the gates of the language packs that are installed),
`.claude/hooks/lighthouse/commit-test-gate.sh` (after each commit, on its files) and a pre-push hook, if present
(`test -x "$(git rev-parse --git-path hooks/pre-push)"`).
`.claude/hooks/lighthouse/main-commit-gate.sh` blocks a commit that lands on the default branch.

**With pack/python (if present): a `ty` zero without extras is no zero.** A module `ty` cannot
resolve makes *every* symbol from it `Unknown`, and on `Unknown` no type error is possible. Always
report with which extras you measure.

## 4. A red job without a failed test

If a host-wide OOM kills a self-hosted runner, the job fails without a single test failing.
Recognition: in
`R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api "repos/$R/actions/runs/<id>/jobs"` the
test step is at `"completed_at": null` while the job is `failure`, and the log is empty or 404s.
Confirm:

```bash
journalctl -k --since <time> | grep -i "Killed process"
```

You look for `Killing process <pid> (Runner.Worker)` and `Out of memory: Killed process <pid> (pytest)`.
Then run the CI selection locally; green there plus a step without `completed_at` means OOM. The
fix is `gh run rerun <id> --failed`, not a code change.

## 5. Push once, and stop at green

Keep working on the branch and push only when it is done. Every push to an open PR starts a full
new round of checks, so "a quick update in between" costs a whole runner cycle and makes the check
history unreadable. Exception: a fix on a *red* check — that one does belong on it right away.

As soon as `gh pr checks <nr>` is green and `mergeable` is `MERGEABLE`: **report and stop**.
Do not merge, do not review, do not close the issue. That "stop at green" does not hold while
section 6 is open: a green PR with an unprocessed roborev review is not done yet.

## 6. Process a roborev review (only if present)

Only if the repo has a `.roborev.toml`, the checks are green, the verdict in `=== roborev ===` is
`Changes require fixes` and there is no `roborev-verwerkt` line for that SHA. One round, no more:

1. Work in the worktree of the head branch (section 2).
2. Follow `superpowers:receiving-code-review` per finding: fix and commit, or refute with
   a reason. Read the severity critically: in one calibration the user found 3 of 4 tested
   `high` findings not `high`. Take high and medium seriously. Fix a low only if that is trivial.
   Every fix is a commit of its own whose body has the line `roborev: <path>:<line> (<severity>)`.
3. Push once (section 5) — only if there is something to push. If you refute *all* findings,
   there is nothing to push and you skip this step.
4. Post **always** one answer comment, also if you pushed nothing and also if you refuted every
   finding: the marker is the signal that the round is done; without it a later fix-pr run
   processes the same review again. The dispatcher (controller run, if present) does not read the
   marker: it starts fix-pr only on a red PR after a finished run, at most three times per PR.
   The comment comes from the renderer `lighthouse-write-pr` uses too, so both routes leave the
   same log. `<scratch>` is the scratchpad or `$TMPDIR`. Save the roborev comment, the same one
   the diagnosis shows:

   ```bash
   R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api "repos/$R/issues/<nr>/comments" --paginate --jq '.[] | select((.user.login == "github-actions[bot]" or .user.login == "github-actions") and (.body | contains("roborev-pr-comment"))) | .body | @json' | tail -1 | jq -r . > <scratch>/roborev-ci.md
   python3 .lighthouse/scripts/roborev_gate.py classify <scratch>/roborev-ci.md
   ```

   If `classify` prints `verdict` `skip` (for instance `review faalde: bevindingen onleesbaar`),
   there is no `sha` to record: render and post nothing, name the reason in your answer and stop
   this section.
   Otherwise `classify` prints `sha` (literally from the heading `` ## roborev: Review Complete (`<sha>`) ``,
   or `Combined Review`) and the `findings`. Write the decisions to `<scratch>/decisions.json`: a
   list with one object per finding, keys `path`, `line`, `severity` (as `classify` printed them),
   `uitkomst` (`fixed`, `refuted` or `left`), `commit` (the fix commit, else `null`) and `reden`
   (the reason, `null` with `fixed`; for a low that is not trivial: `low, not trivial`). Then:

   ```bash
   python3 .lighthouse/scripts/roborev_gate.py record --done <sha> --decisions <scratch>/decisions.json --out <scratch>/roborev.json --raw <scratch>/roborev-ci.md
   python3 .lighthouse/scripts/roborev_comment.py render --pr <nr> --bron ci --state <scratch>/roborev.json --raw <scratch>/roborev-ci.md > <scratch>/comment.md
   R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh pr comment <nr> --repo "$R" --body-file <scratch>/comment.md
   ```

   `record` exits 2 when the decisions do not match the findings one to one; the message says
   which, so fix the file and run it again. The renderer puts the marker
   `<!-- roborev-verwerkt: <sha> -->` at the top, then a one-line `roborev-log` block for scripts
   and a table with the outcome per finding: `fixed in <commit>`, `refuted: <reason>` or
   `left: <reason>`.
5. Stop — the marker is there then, also for a round without a push. Request no new review
   (no close and reopen) and no `roborev fix` or `roborev refine`. The user requests a new review
   themselves.

## Non-negotiable

- ✅ The diagnosis is one Bash call · ❌ four separate exploring calls
- ✅ Read-only diagnosis, mutation only after it · ❌ `git rebase` in the same block as the diagnosis
- ✅ Work in the worktree of the head branch · ❌ in the main checkout, or in a fresh worktree from the default branch
- ✅ `git worktree add <path> --track -b <b> origin/<b>` · ❌ `EnterWorktree` (makes a new branch from the default branch)
- ✅ `gh` with `dangerouslyDisableSandbox: true` · ❌ without — Go's TLS is out then
- ✅ Grep on the error markers · ❌ `--log-failed | tail -60` (misses the error)
- ✅ Reproduce the gate locally before you push · ❌ push and see what CI says
- ✅ Stop as soon as the PR is green · ❌ go on into review, merge or `close-issue`
- ✅ If a gate fails on code: fix the code · ❌ loosen the config or `--no-verify`
- ✅ One processing round with always a marker comment · ❌ request a re-review or `roborev fix`/`roborev refine`

## KPIs

| Metric | Target | Measurement |
|---|---:|---|
| Tool calls between fix prompt and the first mutating call | ≤ 2 | `python3 .lighthouse/scripts/measure_fix_pr_kpi.py leadin` |
| Read-only `gh`/`git` diagnosis calls before that mutation | ≤ 1 | `python3 .lighthouse/scripts/measure_fix_pr_kpi.py diagnose` |
| Turns in which this skill loaded without a fix request in the prompt | 0 | `python3 .lighthouse/scripts/measure_fix_pr_kpi.py mistrigger` |
| Turns in which the skill ran on into `lighthouse-close-issue` or `caveman-review` | 0 | `python3 .lighthouse/scripts/measure_fix_pr_kpi.py scope` |

The script is the source of the numbers, not this text.

Row 3 is the bill for the ambient choice. Without that row "a sharp negative boundary" is a claim;
with that row it is a number that rises at the next measurement or does not.

Row 4 sees only a `Skill` call; a review done by hand in the same turn does not count.

## Known pitfalls

- **`gh pr checks` gives exit 8 on pending checks**, not 0. A block with `set -e` falls over on it.
- **`gh issue view <nr>` and `gh pr view <nr>` do not share the number space.** A "fix 596" can mean
  an issue or a PR; `gh pr view` is the test. In the eight measured turns it was always a PR.
- **`gh issue list --json` breaks under the rtk proxy.** Use `gh api`.
- **A merge conflict gives no fail check.** `mergeStateStatus: DIRTY` is the signal; the
  failure-log part of the diagnosis block stays empty then and that is correct.
- **Links: absolute in chat, relative in files.** In a chat answer a link to a file that exists
  only on the branch must carry the full absolute worktree path: the IDE resolves chat links
  against the main checkout. In a `.md` file, relative instead;
  `.claude/hooks/lighthouse/md-link-check.sh` blocks an absolute path there.
- **In auto mode the permission classifier refuses `git worktree unlock` and `git worktree remove`
  on a locked worktree.** Do not clean up then; report it.

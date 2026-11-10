---
name: lighthouse-review-plan
description: 'Has an implementation plan reviewed adversarially by an agent from a different model family (Codex, read-only, clean context) against the spec and the repo — round 1 an executability review, after that only scoped verification of the fixes. Use after committing a plan and before subagent-driven-development. Triggers - /lighthouse-review-plan, "review this plan", "second pair of eyes on the plan". NOTE — not for a spec (use lighthouse-review-spec instead) or a diff (code-review).'
metadata:
  lighthouse: "core@v0.8.1"
---

# Review Plan

A second pair of eyes on a plan before SDD executes it without you. You stay the operator: the
reviewer delivers findings, *you* decide what is real; the user only where the spec is at
stake (step 6).

## Why this skill exists

A measurement of leaks along the chain found that most of them should have been seen in the
plan. Since superpowers 6.3.0 SDD decides conflicts itself and logs them as
`Ruling:` in the ledger; a bad plan then becomes a stack of rulings the user sees only at the
PR. This skill sits between `writing-plans` and SDD.

Discovery and verification long shared one mechanism: every round was a full review, so every
round also found new criticism instead of converging. Since then only round 1 is discovery;
round 2 and later only verify whether the fixes of the previous round hold.

## When

A fixed seam: plan committed in the plan directory
(`python3 .lighthouse/lib/lighthouse_lib/config.py paths.plans`) → this skill →
`/superpowers:subagent-driven-development`. For *every* plan, also a plan without a separate
spec ("issue #N is the spec"), unless the user chose to skip at the plan seam
(`lighthouse-write-handoff`).

Not for: a spec (`/lighthouse-review-spec`), a diff (`/code-review`).

Controller runs exist only in Claude Code (the controller, if present, starts `claude -p`);
here *report and stop* holds headless too.

## What it shares with the spec review

One runner. `review.sh` here is a wrapper around
`../lighthouse-review-spec/review.sh`, with the plan prompt (`reviewer-prompt.md`)
and the plan schema (`findings.schema.json`, with a required `cat` field). The clean
`CODEX_HOME` (`~/.codex-specreview/`), `--sandbox read-only`, exit codes 2/3/4/5 and the choice
of level (`high`; `max` per run when a missed blocker is expensive, never `xhigh`) are in
`lighthouse-review-spec`, *Prerequisites*. Read that when Codex stalls.

From round 2 on a second wrapper runs on the same runner: `verify.sh`, with the verification
prompt (`verifier-prompt.md`) and the verification schema (`verification.schema.json`). It does
not search for problems again; it only verifies whether the fixes of the previous round hold
(see *Workflow*, step 5).

The review prompt (round 1) sets seven categories, each a `cat`:

| `cat` | When |
|---|---|
| `spec_mismatch` | the plan departs materially from the spec |
| `dependency_order` | the task order or a dependency makes execution impossible or wrong |
| `repo_falsehood` | the plan assumes something about the current codebase that is not true |
| `missing_artifact` | a file, interface, schema or function does not exist yet at the point where the plan uses it |
| `verification_gap` | the tests or verification steps do not prove what the plan claims |
| `unverified_external_assumption` | execution depends on an external assumption the plan never checks |
| `likely_runtime_failure` | the plan contains a concrete failure path that likely yields wrong behaviour |

Every BLOCKER or MAJOR carries three parts: the concrete step or omission, evidence from spec,
plan or repo, and the concrete failure scenario if the plan runs unchanged — without a failure
scenario it is at most a MINOR. A verification round adds one eighth `cat`:
`fix_incomplete` (see steps 5 and 6).

## Workflow

Shell variables do not survive the next shell call: note the paths a step prints and fill them
in literally.

### 1. Preflight

```bash
command -v codex || echo "codex CLI missing"
codex login status   # expected: "Logged in using ChatGPT"
```

If Codex is missing, report it loudly and stop: there is no fallback outside Claude Code (see
*Fallback without Codex*). Do not go on silently.

If the Codex budget is exhausted beforehand, you notice at the round: the runner checks it
before every round and stops with exit 6 (`CODEX_BUDGET: fallback — <reason>`), without
`codex exec`. Report `reason` and `reset_at` from `<OUT>.budget.json` (a paired round 1 does
not exist here: outside Claude Code there is no pair) and stop **before** the round; the
convergence outcome is `aborted`, as with the limit.

### 2. Pick the plan

Argument → that path, repo-relative. No argument → the newest file in the plan directory (not
`index.md`); confirm which.

### 3. Start an attempt

```bash
python3 .lighthouse/scripts/plan_review_record.py start <plan>
```

Prints the attempt directory `${TMPDIR:-/tmp}/plan-review/<chain-key>/<attempt>/` (below
`<ATTEMPT>`) and records the commit round 1 reads; on the always-on host (`host.toml`) the
attempt is also in the data directory at once, so that a review that never reaches step 9 is
still the newest. Exit 2 for a plan with uncommitted changes: commit first. Every new review of
the same plan, also after a return through `spec`, starts here again. You never reuse an old
attempt directory.

### 4. Pick the spec(s) — you, not the reviewer

From the `**Spec:**` line of the plan:

| The line names | You do |
|---|---|
| a repo-relative path | pass it on as is |
| an absolute worktree path | reduce it to repo-relative: everything after `.claude/worktrees/<name>/` |
| several specs | pass them all on |
| "issue #N is the spec" | fetch the body (below), pass that file on |
| nothing | report and ask; do not guess |

Issue as spec — the reviewer has no GitHub, so the body goes along as a file:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/issues/<N>" --jq .body > <ATTEMPT>/issue-<N>.md
date -u +%Y-%m-%dT%H:%M:%SZ
```

`gh` runs outside the sandbox of your harness. Note the time: it goes as
`--issue-fetched-at` into the record (step 9).

### 5. Run a round — directly, in the foreground

**Paired round 1: Claude Code only.** There Codex and an Opus subagent run side by side in
round 1 by default, and Opus verifies rounds 2 and 3. Here that subagent does not exist; every
round runs through Codex, as below.

Here you call the runner yourself. Before *every* round a snapshot of the plan:

```bash
cp <plan> <ATTEMPT>/round-<n>.snapshot.md
```

**Only round 1 is a review:**

```bash
.agents/skills/lighthouse-review-plan/review.sh <plan> 1 <ATTEMPT>/round-1.json \
  none [max] --context <spec path> [--context <spec path 2>]
```

**Round n ≥ 2 runs only if round n-1 had a BLOCKER/MAJOR with `accept` or `spec`.**
No fixes, no round. Build the fixes diff first:

```bash
git diff --no-index <ATTEMPT>/round-<n-1>.snapshot.md <plan> > <ATTEMPT>/round-<n>.fixes.diff
```

`git diff --no-index` exits 1 when there is a difference; that is no error. If a finding got
`spec` in the previous round, fetch the issue body again to `<ATTEMPT>/issue-<N>.md` before the
round (the same `gh api` line as step 4). Then run:

```bash
.agents/skills/lighthouse-review-plan/verify.sh <plan> <n> <ATTEMPT>/round-<n>.json \
  <ATTEMPT>/round-<n-1>.json [max] --context <spec path> [--context <spec path 2>] \
  --context <ATTEMPT>/round-<n>.fixes.diff
```

`max` only when a missed blocker is expensive (default: leave it out). A round costs ~7 minutes
on `high` (~18 on `max`). Run the runner and the record script of step 9 outside the sandbox of
your harness (or ask for escalation): the runner writes under `~/.codex-specreview/` and starts
a `codex exec` with network, the record script writes to the data directory
(`~/.local/share/lighthouse/` by default). Set the timeout of the shell call to at least 20
minutes. Then show the findings sorted by severity, with the ID computed **before** sorting
(`r<n>-f<index>`, the position in `findings[]`):

```bash
jq -r --arg round <n> '
  .findings | to_entries
  | map(.value + {id: "r\($round)-f\(.key + 1)"})
  | sort_by({"BLOCKER": 0, "MAJOR": 1, "MINOR": 2}[.severity])
  | .[]
  | "| \(.id) | \(.severity) | \(.cat) | \(.verifies // "") | \(.plan_section) | \(.problem) | \(.failure_scenario) | \(.evidence) | \(.proposed_fix) |"
' <ATTEMPT>/round-<n>.json
```

Header: `| ID | Sev | Cat | Verifies | Section | Problem | Failure | Evidence | Proposed fix |`.
Directly below it, for a verification round:

```bash
jq -r '.verified // empty | "VERIFIED: " + (if length == 0 then "none" else join(", ") end)' \
  <ATTEMPT>/round-<n>.json
```

Summarise nothing; read `review_markdown` only when a finding is unclear.

**If you run inside Codex yourself**, the reviewer is the same model family as the author. Put
the banner from *Fallback without Codex* above the table, and give the record in step 9
`--review-mode codex_same_family` instead of `codex`.

### 6. Gate — you follow your recommendation, the spec is the boundary

**Required sub-skill:** `superpowers:receiving-code-review`. Do not fix blindly.

Per finding you form a recommendation and carry it out, **without asking the user**. In
practice the user kept choosing the recommended option; a question per finding cost only time.

Ask the user **only** when your recommendation contradicts the spec:

- the fix makes the plan depart from what the spec prescribes or excludes, or
- the decision is `spec` and the change is more than a clarification (then the spec goes back
  to `/lighthouse-review-spec` anyway; not in round 3: see the `spec` row below).

A fix that brings the plan *into* line with the spec is no contradiction: just do it. If you
doubt whether something contradicts the spec, you ask.

After every round show one table `ID | Decision | Reason`, so that the user can step in before
the next round. The decisions:

| Decision | Meaning |
|---|---|
| `accept` | valid, adjust the plan |
| `reject` | invalid, **with a reason**; that reason goes along to the next round |
| `spec` | valid, but the error is in the spec: adjust the spec plus one line in the plan. If the change is more than a clarification, stop and report that the spec must go back to `/lighthouse-review-spec`. Exception: in round 3 a `spec` decision does not stop the chain; the point goes through `accept-open` like any other open point |

Check every `file:line` of a BLOCKER before `accept`: a reviewer that misreads the repo produces
convincing nonsense. A `reject` is just as much a recommendation, with a reason. You decide
MINORs yourself too, per MINOR `minor_yes` or `minor_no`, without a batch question.

In a verification round one extra check holds:
Every accepted ID is in `verified` or in `verifies` of a finding (accepted = `accept` or
`spec`). If one is missing, run *that* verification round again (same `n`, same `.fixes.diff`,
`round-<n>.json` overwritten). If the ID is missing again, the outcome is `aborted` and the
handoff names that ID as an open point — never `converged`. MINORs never start a round.

After **every** round, the last one too, write the decisions to the sidecar
`<ATTEMPT>/round-<n>.decisions.json`, **never** into the round JSON (it must stay valid against
the schema). One line per finding, MINORs included: at `converged` and `round_limit` step 9
refuses a record with a finding without a decision (exit 4); only `aborted` may have gaps.

```json
[
  {"finding_id": "r2-f1", "decision": "accept", "reason": "Task 3 named the wrong function"},
  {"finding_id": "r2-f2", "decision": "reject", "reason": "the cron does run on the always-on host, see Task 5", "same_as": "r1-f4"},
  {"finding_id": "r2-f3", "decision": "minor_yes", "reason": "small, own choice"}
]
```

- `finding_id` only from the `ID` column, each exactly once. Never count in the sorted table
  yourself: the ID is the position before sorting. An ID that does not exist in that round or
  appears twice is a validation error in step 9.
- `decision`: `accept`, `reject`, `spec`, `accepted_open` (only at `round_limit`, set by
  `accept-open` in step 8, never by hand), `minor_yes`, `minor_no`.
- `same_as`: the ID of the same finding in an earlier round; otherwise leave it out.

`review.sh` names the sidecar in the prior block of the next round.

### 7. Fix

Only the `accept`s in the plan and the `spec` clarifications in the spec. No code, no adjacent
files. Commit after every round: `docs(plan): process plan review round <n> (#<issue>)`, in the
artefact language of the project. No fixes = no commit; never make an empty commit. That commit
goes before the snapshot of the next round (step 5).

### 8. Convergence — three outcomes

| `outcome` | When | Handoff |
|---|---|---|
| `converged` | no open BLOCKER/MAJOR in the last round (see *Open* below); if that round is a verification, all accepted fixes are verified with it; step 9 refuses otherwise (exit 4) | `/superpowers:subagent-driven-development <plan>` |
| `round_limit` | round 3 reached with open BLOCKERs/MAJORs. A third round only if a fix from round 2 does not hold | `/superpowers:subagent-driven-development <plan>` — go on, never a question: first record `accepted_open` (see *Open*) |
| `aborted` | the user says stop, or an accepted ID is missing also after a repeated verification round (step 6) | *no* SDD; the handoff names the open points |

If the same finding (through the `verifies` chain) carries `fix_incomplete` in round 2 *and* 3
→ solve it structurally, no round 4.

**Open** = a BLOCKER/MAJOR from the last round without `reject` — also one that got `accept` and
was fixed in step 7, because no reviewer saw that fix any more. `converged` means: after the
last round nothing is open. At `round_limit` you record the open points and go on, in this
order:

1. `python3 .lighthouse/scripts/plan_review_record.py accept-open <ATTEMPT> --doc <plan>`
   (outside the sandbox, as with `start` and `write`). This rewrites `round-3.decisions.json`
   (every open point becomes `accepted_open`) and puts the block `heading:open_after_review` at
   the bottom of the plan; an existing block is replaced, never doubled. Exit 2 with on stderr
   `no open BLOCKER/MAJOR in the last round: nothing to accept` = nothing is open: the outcome
   is then `converged`, not `round_limit`. Exit 2 without that message is a call error: repair
   the call. Exit 4 (`ACCEPT-OPEN FAILED: …`): report the message literally, repair the cause
   (path, sidecar) and run again. If it still fails, put the open points in the handoff and go
   on to the next phase; never edit the sidecar or the document by hand, never a question.
2. Commit the plan if `git diff --quiet -- <plan>` reports a difference:
   `docs(plan): record open review points (#<issue>)`. No difference (resumed run) = no commit.
3. Only then step 9 (`write --outcome round_limit`).

A second line for the same ID is a validation error in step 9. Ask the user nothing:
`round_limit` is no question, also not when `spec` decisions are open. The record (step 9)
always comes first.

### 9. Record — only on the always-on host (`host.toml`)

After the last fix commit, at every outcome:

```bash
python3 .lighthouse/scripts/plan_review_record.py write <ATTEMPT> \
  --outcome <converged|round_limit|aborted> --review-mode codex \
  --spec <spec path> [--spec <spec path 2>] [--level max] \
  --skill-dir .agents/skills/lighthouse-review-plan
```

Issue as spec: `--issue <N> --issue-body <ATTEMPT>/issue-<N>.md --issue-fetched-at <time>`
instead of `--spec`. From inside Codex itself: `--review-mode codex_same_family` (step 5).

| exit | Meaning | You do |
|---|---|---|
| 0 | record written; the path is on stdout | report the path |
| 3 | not on the always-on host (`host.toml`): no record | report loudly that this chain falls outside the measurement, on to step 10 |
| 4 | write or validation error: only the staging directory `.<attempt>.tmp/` stays | pass the message on literally; if it is a missing or wrong decision, repair the sidecar and run `write` again; otherwise on to step 10 |

The record is measurement material for a later measurement, the review itself is the product: a
failed record never blocks the handoff.

### 10. Close

Report: plan path, number of rounds, count of `accept`/`reject`/`spec`, the final verdict of the
last round, `outcome`, and the record path or the message from step 9. End with
`/lighthouse-write-handoff` following the table in step 8.

## Fallback without Codex

Outside Claude Code there is no `general-purpose` subagent to fall back on. If Codex is
missing, or at its limit (exit 5, `grep -o 'try again at [^"]*' <OUT>.events`), or its budget
is exhausted beforehand (exit 6, `reason` and `reset_at` from `<OUT>.budget.json`), report that
with the reset time and stop **before** the round: a review by yourself is no second pair of
eyes.

The banner for a review within one model family (see step 5):

> ⚠️ SAME-FAMILY — same model family as the author of the plan.
> Catches ground errors (code that does not exist), catches *no* shared blind spots.

## Known pitfalls

- **Counting the ID after sorting.** Then every decision points at the wrong finding and the
  record is wrong. Take the ID from the table.
- **Writing decisions into the round JSON.** Makes it invalid against the schema; always the
  sidecar.
- **Resuming an old attempt.** A new review is a new `start`; the measurement counts only the
  newest attempt per chain.
- **Running a verification round with `review.sh`.** That starts discovery again, and that is
  exactly the loop the verification rounds closed.

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

## Controller run (only if present)

The controller stays outside lighthouse. It is present when `lighthouse.toml` has a
`[controller]` table (`grep -q '^\[controller\]' lighthouse.toml`); otherwise this section is
skipped. Recognition: the prompt ends on `(issue #<nr>, controller-run)`, or
`LIGHTHOUSE_CONTROLLER_RUN` is set in the environment; the second counts after a resume with an
answer, because that prompt does not carry the suffix. Then, above the steps below:

1. No `codex`: *Fallback without Codex* at once, without a question.
2. Step 2: the plan comes as an argument; the confirmation drops.
3. Each of the three spec questions of step 6 becomes the same `🤖 heading:kind_question`
   instead of an interactive one: a fix with which the plan departs from the spec, a `spec`
   decision that is more than a clarification, and doubt whether something contradicts the
   spec. The question follows the controller's run instructions and gives the point with two
   options. *Adjust the spec*: the resumed run adjusts the spec according to the answer,
   commits, and posts the handoff
   `next: /lighthouse-review-spec <absolute spec path>` (otherwise
   `lighthouse-write-handoff` sends the unreviewed plan back to plan review and the chain loops).
   *Carry on with the point open*: the review goes on and the point goes into the handoff to
   SDD as a note. In the sidecar the point gets `"decision": "reject"` with
   `"reason": "open on request (heading:kind_question)"`: plan review knows no `defer`,
   `accept`/`spec` makes the next verification round expect a fix, and a point without a
   decision makes `write --outcome converged` fail (exit 4), so that the record is lost.
   The run never changes the spec materially by itself.
4. At `aborted` (step 8): first the record (`--outcome aborted`, step 9), then a
   `🤖 heading:kind_question` with the open points and two options: *On to SDD with the open
   points* (handoff `next: /superpowers:subagent-driven-development <absolute plan path>`, open
   points as a note, no second `write`, no `accept-open`) or *Continue interactively* (handoff
   `next: /superpowers:executing-plans <absolute plan path>`: the controller returns the
   issue). No option *another round*.
5. After the record at `round_limit` the run instruction follows
   (`controller.bij_rondegrens` in `lighthouse.toml`).
6. Background tasks: the paragraph **Controller run (if present):** in *Round 1: Codex and
   Opus*.

## What it shares with the spec review

One runner. `review.sh` here is a wrapper around
`../lighthouse-review-spec/review.sh`, with the plan prompt (`reviewer-prompt.md`)
and the plan schema (`findings.schema.json`, with a required `cat` field). The clean
`CODEX_HOME` (`~/.codex-specreview/`), `--sandbox read-only`, exit codes 2/3/4/5 and the choice
of level (`high`; `max` per run when a missed blocker is expensive, never `xhigh`) are in
`lighthouse-review-spec`, *Prerequisites*. Read that when Codex stalls.

From round 2 on an Opus subagent verifies, with the verification prompt
(`verifier-prompt.md`) and the verification schema (`verification.schema.json`). It does not
search for problems again; it only verifies whether the fixes of the previous round hold (see
*Workflow*, step 5).

So Codex runs only in round 1, and there always next to Opus: the two reviewers complement each
other in round 1, and the verification rounds on Opus spare the Codex quota. Outside Claude Code
there is no Opus subagent; there `verify.sh` (the same runner) verifies through Codex.

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

Shell variables do not survive the next Bash call: note the paths a step prints and fill them
in literally.

### 1. Preflight

```bash
command -v codex || echo "codex CLI missing"
codex login status   # expected: "Logged in using ChatGPT"
```

If Codex is missing, report it loudly and ask whether the fallback may run (at the bottom); not
in a controller run (if present). Do not go on silently.

If the Codex budget is exhausted beforehand, you notice only at round 1: the runner checks it
before the round, writes the decision to `<OUT>.budget.json` and stops with **exit 6** without
`codex exec`. That is no preflight error to report and stop on: round 1 then runs Opus-only,
see *Fallback without Codex*.

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

`gh` always with `dangerouslyDisableSandbox: true`. Note the time: it goes as
`--issue-fetched-at` into the record (step 9).

### 5. Dispatch a round — in the background

Before *every* dispatch a snapshot of the plan: `cp <plan> <ATTEMPT>/round-<n>.snapshot.md`.

**Only round 1 is a review**, and it is always paired: Codex and Opus side by side (*Round 1:
Codex and Opus*, below). Only the retry from the table there runs Codex alone. Do it yourself, as
a Bash call with `run_in_background: true` and `dangerouslyDisableSandbox: true`; the command
stays on one line, and `review.sh` is called by exactly this repo-relative path, so the allowlist
rule in `.claude/settings.json` (from `lighthouse init`) matches on that prefix:

```bash
.claude/skills/lighthouse-review-plan/review.sh <plan> 1 <ATTEMPT>/round-1.json none [max] --context <spec path> [--context <spec path 2>] > <ATTEMPT>/round-1.retry.log 2>&1; echo "exit=$?"
```

`[max]` is the level slot: leave it out, or write `max` if you want it. The `--context` paths are
the spec(s) from step 4. Read only the exit code, never `round-1.retry.log`. On success make the
`ID` table with the jq fragment from *Opus dispatch*, on `round-1.json`, without a banner. On a
non-zero exit:

| exit | meaning |
|---|---|
| 2 | wrong arguments, the plan is not under the repo root, or a `--context` path does not exist |
| 3 | `CODEX_MISSING`: no codex in PATH |
| 4 | `CODEX_UNAUTHENTICATED`: no `~/.codex/auth.json` |
| 5 | `CODEX_FAILED`: the run failed or produced nothing. Read the reset time with `grep -o 'try again at [^"]*' <ATTEMPT>/round-1.json.events \| tail -1` |
| 6 | `CODEX_BUDGET`: budget exhausted before the round; `reason` and `reset_at` are in `<ATTEMPT>/round-1.json.budget.json` |

Never retry the retry: exit 6 is never retried, nor is any other exit. After "Both fail" the
retry always runs once, also when the Codex side of the pair gave exit 6. If the retry fails too, whatever the exit, stop with `--outcome aborted` and report both
errors. Never fall back to reviewing the plan yourself: that defeats the cross-family check.

**Round n ≥ 2 runs only if round n-1 had a BLOCKER/MAJOR with `accept` or `spec`.**
No fixes, no round. Build the fixes diff first:

```bash
git diff --no-index <ATTEMPT>/round-<n-1>.snapshot.md <plan> > <ATTEMPT>/round-<n>.fixes.diff
```

`git diff --no-index` exits 1 when there is a difference; that is no error. If a finding got
`spec` in the previous round, fetch the issue body again to `<ATTEMPT>/issue-<N>.md` before the
dispatch (the same `gh api` line as step 4) — otherwise the verifier does not see the spec side
of the fix.

Then dispatch the verifier: the subagent from *Opus dispatch* (at the bottom), with
`verifier-prompt.md`, `PRIOR` = `<ATTEMPT>/round-<n-1>.json`, as context the spec(s) from step 4
plus `<ATTEMPT>/round-<n>.fixes.diff`, and `OUT` = `<ATTEMPT>/round-<n>.json`. Always Opus, also
when Codex still has budget: Codex is spared for round 1. No banner: this is the rule, no
fallback.

Let it run in the background (the default of the Agent tool). You make the ID table yourself
with the jq fragment from *Opus dispatch*: per finding an `ID` (`r<n>-f<index>`), a `Cat` and a
`Verifies`.

#### Round 1: Codex and Opus

In round 1 Codex and an Opus reviewer run side by side, on the same commit; you decide blind.
Started as a temporary measurement, it is the default for plan and spec alike: in paired plan
reviews each reviewer found valid BLOCKERs and MAJORs the other missed, more than the two
shared.

First check that `git status --porcelain` is empty. If it is not, commit or clean up first:
`start` checks only the plan, but `blind` refuses a work tree that is not clean, and then the
pair is lost. Then dispatch two background reviewers in **one message**, on the same commit and
after the snapshot.

**Controller run (if present):** end no answer while a reviewer runs, also not the verifier
from round 2 on or the retry; wait for every notification. What still runs after the last
answer dies with the session.

1. **Codex**, through a Bash call with `run_in_background: true` and
   `dangerouslyDisableSandbox: true`:

   ```bash
   .claude/skills/lighthouse-review-plan/review.sh <plan> 1 <ATTEMPT>/round-1.codex.json \
     none [max] --context <spec path> [--context <spec path 2>] > <ATTEMPT>/round-1.codex.log 2>&1; echo "exit=$?"
   ```

   `[max]` is the level slot: leave it out, or write `max` if you want it, like in the retry
   above. Then the record in step 9 gets `--level max`.

   No ID table for this run: it would show the findings, and then deciding is no longer blind. The
   redirect is required for the same reason. On success `review.sh` prints the number of
   findings and the severity split (the `ok | reviewer: …` line). That count next to the A/B
   table betrays which letter Codex is. Read only the exit code, never `round-1.codex.log`.
2. **Opus**: the subagent from *Opus dispatch*, with `OUT` = `<ATTEMPT>/round-1.fallback.json`.
   The prompt asks it to return only `DONE <path> model=<exact model ID from your own system
   prompt>`: no findings, no count. `model: "opus"` is an alias. So take the reported ID over as
   `--shadow-model` (step 9) and never fill it in yourself. Run no jq table for this dispatch
   and put no banner before `blind`: the jq table from *Opus dispatch* reads the raw file, and
   then deciding is no longer blind. `blind` makes the table.

Between the dispatch and `blind` you write nothing in the worktree: no edit, no commit. Both
reviewers read the live work tree. `blind` refuses when `HEAD` moved or the work tree is not
clean, untracked files included. A change you revert before `blind` it does not see. So the rule
holds also when you put something back "for a moment".

When both are done, run `python3 .lighthouse/scripts/review_pair.py blind <ATTEMPT>`
(`dangerouslyDisableSandbox: true`) and decide on the A/B table. Do not read `pair.json`, the
raw files or `round-1.codex.log`. Step 6 holds unchanged, with the blind IDs.

If the pair fails, there is no pair: no `merge`, no `--shadow-model`. The ID table then comes
from the jq fragment in *Opus dispatch*, on `round-1.json`; with the banner from *Fallback
without Codex* when `round-1.json` comes from Opus, without it when it comes from Codex. A
budget exhausted beforehand you recognise by exit 6 and
`<ATTEMPT>/round-1.codex.json.budget.json` (`reason`, `reset_at`); a limit in the middle of the
round by exit 5 plus `grep -o 'try again at [^"]*' <ATTEMPT>/round-1.codex.json.events` (not
`round-1.json.events`).

| Situation | `round-1.json` becomes | `--review-mode` |
|---|---|---|
| Codex exit 6 (budget exhausted beforehand) | `mv <ATTEMPT>/round-1.fallback.json <ATTEMPT>/round-1.json` | `fallback --reviewer-model <reported model>` |
| Codex exit 5 with a reset time | `mv <ATTEMPT>/round-1.fallback.json <ATTEMPT>/round-1.json` | `fallback --reviewer-model <reported model>` |
| Codex fails otherwise (exit 2/3/4, or 5 without a reset time) | `mv <ATTEMPT>/round-1.fallback.json <ATTEMPT>/round-1.json` | `fallback --reviewer-model <reported model>` |
| The Opus subagent fails or writes an invalid file | `mv <ATTEMPT>/round-1.codex.json <ATTEMPT>/round-1.json` | `codex` |
| Both fail, or both raw files are invalid | none; run the retry from step 5 once | following that retry; if it fails too, stop with `--outcome aborted` and report both errors |
| `blind` gives exit 4 on a broken raw file | the other, valid file, as in the two rows above | the same |
| `blind` gives exit 4 with `work tree changed since start or not clean` | `mv <ATTEMPT>/round-1.codex.json <ATTEMPT>/round-1.json`; the Opus output drops | `codex` |
| `pair.json` or a raw file read before `merge` | `mv <ATTEMPT>/round-1.codex.json <ATTEMPT>/round-1.json`; the blind decisions drop, decide again on the r1 IDs | `codex` |

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

**Paired round 1** (step 5, *Round 1: Codex and Opus*):

- Write `<ATTEMPT>/round-1.blind-decisions.json` instead of `round-1.decisions.json`, with the
  blind IDs (`A-f<i>`, `B-f<i>`).
- A duplicate finding (the same failure in the same place, by both reviewers) is one row with
  `also`: `{"finding_id": "A-f2", "also": "B-f1", "decision": "accept", "reason": "…"}`.
- Then run `python3 .lighthouse/scripts/review_pair.py merge <ATTEMPT>`
  (`dangerouslyDisableSandbox: true`). It writes `round-1.json`, `round-1.decisions.json` and
  `round-1.sources.json`, and shows the unblinded table with the `r1-f<i>` IDs.
- Exit 4 on a decision: repair the sidecar and run `merge` again. Exit 4 on a broken raw file:
  no pair (the table in step 5).
- Round 2 reads `round-1.json` as `PRIOR`, as always.

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
always comes first; for a controller run (if present) see *Controller run*. Interactively
`round_limit` is never a question.

### 9. Record — only on the always-on host (`host.toml`)

After the last fix commit, at every outcome:

```bash
python3 .lighthouse/scripts/plan_review_record.py write <ATTEMPT> \
  --outcome <converged|round_limit|aborted> --review-mode codex \
  --spec <spec path> [--spec <spec path 2>] [--level max]
```

`start` (step 3) and `write` write outside the working directory (`${TMPDIR:-/tmp}/plan-review/`
and the data directory, `~/.local/share/lighthouse/` by default); under an active sandbox run
them with `dangerouslyDisableSandbox: true`, otherwise `write` ends in exit 4 and the record is
lost.

Issue as spec: `--issue <N> --issue-body <ATTEMPT>/issue-<N>.md --issue-fetched-at <time>`
instead of `--spec`.

`--review-mode` says who reviewed round 1:

| Round 1 | Flags |
|---|---|
| paired (there is a `round-1.sources.json`) | `--review-mode codex --shadow-model <model Opus reported in round 1>` |
| only Codex (Opus failed, or `blind` refused) | `--review-mode codex` |
| only Opus (Codex failed, *Fallback without Codex*) | `--review-mode fallback --reviewer-model <reported model>` |

The verification rounds on Opus do not change `--review-mode`: they are the rule, no fallback,
and `round_kinds` in the record marks them. Without a pair no `--shadow-model`; `write` refuses
both wrong sides with exit 4.

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

Only round 1 runs on Codex, so only round 1 has a fallback. Three triggers: no `codex` (ask
first, step 1; not in a controller run, if present), the budget exhausted beforehand (exit 6),
or a Codex limit in the middle of the round (exit 5 + `try again at`). Round 1 then runs
Opus-only: the Opus side of the pair becomes `round-1.json` (the table in step 5). If `codex`
was missing at the preflight already, no pair was dispatched: one *Opus dispatch* with `OUT` =
`<ATTEMPT>/round-1.json`.

- **Exit 6** you recognise by `CODEX_BUDGET: fallback — <reason>` on stderr and the decision in
  `<ATTEMPT>/round-1.codex.json.budget.json`. **Do not ask and do not wait.** Report one line
  with `reason` and `reset_at` from that file.
- **Exit 5** you recognise by a reset time in the events
  (`grep -o 'try again at [^"]*' <ATTEMPT>/round-1.codex.json.events`; `.err` shows only
  `Reading additional input from stdin...`). Then too **do not ask and do not wait**. Report the
  reset time in one line.

Name in the final report "before the round (exit 6)" or "in the middle of the round (exit 5)".

Put above the table of an Opus-only round 1:

> ⚠️ SAME-FAMILY FALLBACK — same model family as the author of the plan.
> Catches ground errors (code that does not exist), catches *no* shared blind spots.

The record gets `--review-mode fallback --reviewer-model <model name of the subagent>` (step 9).

## Opus dispatch

One dispatch, three roles: the Opus side of the pair in round 1, the fallback when Codex does
not make round 1, and the verifier in every round ≥ 2.

Dispatch a `general-purpose` subagent with `model: "opus"` and the text between
`---BEGIN PROMPT---` and `---END PROMPT---` from `reviewer-prompt.md` (round 1) or
`verifier-prompt.md` (round ≥ 2), the placeholders filled in by hand: `{{CONTEXT_PATHS}}` as
lines `- <path>`, and `{{PRIOR_REVIEW_BLOCK}}` empty in round 1 and from round 2 on the paragraph
that `review.sh` builds as `prior_block`, with `PRIOR` and its `.decisions.json` sidecar filled
in. Instruct it to write the JSON answer following `findings.schema.json` or
`verification.schema.json` to `OUT`. Compute the
`ID` column yourself, **before** sorting. In a paired round 1 you skip this table
(step 5): `blind` makes the table.

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

## Known pitfalls

- **Counting the ID after sorting.** Then every decision points at the wrong finding and the
  record is wrong. Take the ID from the table.
- **Writing decisions into the round JSON.** Makes it invalid against the schema; always the
  sidecar.
- **Making the `review.sh` path absolute.** The allowlist matches on the repo-relative prefix; an
  absolute path makes the background round wait for an approval.
- **Resuming an old attempt.** A new review is a new `start`; the measurement counts only the
  newest attempt per chain.
- **Dispatching a verification round with `reviewer-prompt.md` or `review.sh`.** That starts
  discovery again, and that is exactly the loop the verification rounds closed.
- **Sending a verification round to Codex.** Codex is for round 1; the verification runs on
  Opus, also when the Codex budget is still ample.
- **Reading `pair.json` or a raw file before `merge`.** Then you no longer decide blind and the
  pair is worthless; follow the last row of the table in step 5.
- **Pulling the output of the Codex call into the conversation.** It belongs in
  `round-1.codex.log`; the count in the `ok` line betrays the letter.

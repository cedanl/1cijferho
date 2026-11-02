---
name: lighthouse-review-spec
description: 'Has a spec reviewed adversarially by an agent from a different model family (Codex, read-only, clean context) against the repo — round 1 a review, after that only scoped verification of the fixes. Use after writing a spec and before writing-plans. Triggers - /lighthouse-review-spec, "review this spec", "second pair of eyes on the spec", "adversarial review". NOTE — not for a plan (use lighthouse-review-plan instead) or a diff (code-review).'
metadata:
  lighthouse: "core@v0.8.1"
---

# Review Spec

A second pair of eyes on a spec before a plan comes out of it. You stay the operator: the
reviewer delivers findings, *you* decide what is real.

## Why this skill exists

Superpowers' `brainstorming` (7.0) dispatches a **builder check** before the handover
(`skills/brainstorming/builder-check-prompt.md`): a fresh subagent reads the design as the one
who has to build it and reports only the questions where a wrong guess matters. It asks *where
do I have to guess?*, while the user is still there, and gets only the document: it checks
nothing against the codebase.

A first sieve, no check against the code. This skill is the second sieve: it asks *does this
match the real code?*, and runs after the commit.

| Layer | Who | Catches |
|---|---|---|
| 1 | superpowers builder check, fresh subagent, before the handover | spots to guess that only the user can decide |
| 2 | this skill: Codex and Opus side by side in round 1, clean context, read-only | does the spec match the real code, what breaks on implementation |

We do not replace layer 1: it runs in brainstorming anyway and catches something else.

Discovery and verification long shared one mechanism: every round was a full review, so every
round also found new criticism instead of converging. Since then only round 1 is discovery;
round 2 and later only verify whether the fixes of the previous round hold.

Round 1 always runs paired: Codex and Opus side by side, decided blind. The two reviewers
complement each other in round 1: each finds valid BLOCKERs and MAJORs the other misses,
security among them. The verification rounds run on Opus, so that Codex is spared for round 1.

## When

- Spec written (in `python3 .lighthouse/lib/lighthouse_lib/config.py paths.specs`), before
  `superpowers:writing-plans`.
- Also fine: a spec that comes from someone else and that you have to build on.

Not for: reviewing code or a diff (that is `/code-review`), or reviewing a plan (that is
`/lighthouse-review-plan`).

## Controller run (only if present)

The controller stays outside lighthouse. It is present when `lighthouse.toml` has a
`[controller]` table (`grep -q '^\[controller\]' lighthouse.toml`); otherwise this section is
skipped. Recognition: the prompt ends on `(issue #<nr>, controller-run)`, or
`LIGHTHOUSE_CONTROLLER_RUN` is set in the environment; the second counts after a resume with an
answer, because that prompt does not carry the suffix. Then, above the steps below:

1. No `codex`: *Fallback without Codex* at once, without a question (exit 5 and 6 do that
   already).
2. Step 1: the path comes as an argument; the confirmation drops.
3. Step 3: you decide the table and the piles yourself, also every MINOR, and report the choices.
   A finding outside the scope becomes a line under *Open for the plan* or a proposal for an
   issue of its own in the handoff, not a question.
4. A question of substance that no tenet or area block decides: a numbered list as the last
   text, following the controller's run instructions; the controller posts it as
   `🤖 heading:kind_question` and resumes the session with the answer.
5. At `converged` or `round_limit`: `/lighthouse-write-handoff` with
   `next: /superpowers:writing-plans <absolute spec path>`. After the record follows the run
   instruction (`controller.bij_rondegrens` in `lighthouse.toml`).
6. At `aborted`: first the record (`--outcome aborted`, step 5), then a `🤖 heading:kind_question`
   with the open points and two options. *On to writing-plans with the open points*: handoff
   `next: /superpowers:writing-plans <absolute spec path>`, open points under the handoff's note
   field. No second `write`, no `accept-open`. *Continue interactively*: handoff
   `next: /superpowers:executing-plans <absolute spec path>` (the existing return path: the
   controller posts `🤖 heading:kind_returned`, the Status stays `Controller`, the user moves it
   off). Only `next:` carries executing-plans, as the return signal; the `First step:` of the
   block is then `/superpowers:writing-plans <absolute spec path>` (or an interactive spec
   review), because there is no plan to execute yet. *Another round* is no option: that clashes
   with the round limit, and you never reuse an old attempt.
7. Background tasks: the paragraph **Controller run (if present):** in
   `lighthouse-review-plan`, *Round 1: Codex and Opus*.

## Prerequisites

```bash
command -v codex || echo "codex CLI missing"
codex login status   # expected: "Logged in using ChatGPT"
```

**Install it on the always-on host (`host.toml`)**, where unattended rounds run:
`@openai/codex` globally through npm (the binary on `PATH`), logged in through a **ChatGPT**
subscription. Bare `codex login` opens a `localhost:1455` callback that you finish on that
machine's screen. The auth lands in `~/.codex/auth.json` (access and refresh token).

That a ChatGPT subscription runs the reviewer is no accident but the intent: using a Claude
*subscription* token for automated agent review falls outside what Anthropic sanctions, and it
is moreover the same model family — exactly what this skill wants to avoid.

The reviewer runs in an **own, clean `CODEX_HOME`** (`~/.codex-specreview/`), not in your usual
`~/.codex`. Two reasons, and the second weighs most:

- **Context.** Your usual Codex home loads marketplaces, plugins, MCP servers, hooks and agent
  definitions. Measured on the same trivial prompt: roughly twice the tokens from `~/.codex`
  as from the clean home — overhead before the spec is even in view.
- **A clean context is the claim of this skill.** A reviewer that starts with your skill
  catalogue and a handful of workflow guards in its prompt does not judge without bias.
  `--ignore-user-config` is *not* enough: it ignores only `config.toml` and leaves
  `~/.codex/agents/` in place.

Model and reasoning effort are **not** in this skill — they belong to your Codex subscription.
They are in `~/.codex-specreview/config.toml`:

```toml
model = "gpt-5.6-sol"
model_reasoning_effort = "high"
```

### Why `high`, and when `max`

Measured, not guessed — 27 runs over three specs:

| level | reasoning cost against `high` | unique reproducible claims |
|---|---|---|
| `high` | 1× | the baseline |
| `xhigh` | 1.3× | **zero**, on none of the three specs |
| `max` | 2.6× (and ~2.5× the run time) | two over three specs, one standing after judgement |

`xhigh` is dominated: more expensive than `high` and finds nothing extra. Do not use it.

`max` can do something `high` cannot, but too little to pay for it on every review. Use it per
run when a missed blocker is expensive — infrastructure you cannot easily roll back,
migrations, something that goes live:

```bash
CODEX_HOME=~/.codex-specreview codex exec -c model_reasoning_effort=max ...
```

Do not touch `config.toml` for that; the flag overrules the profile per call.

### Structured output

`findings.schema.json` (next to this file) enforces the final answer through
`codex exec --output-schema`. The schema starts with a free `review_markdown` field and only
then the validated fields — that order is functional, not cosmetic: structured generation
follows the schema order, so the reviewer first reasons freely and structures afterwards.
Format restrictions otherwise lower reasoning ability (arXiv:2408.02442), and for an
adversarial reviewer that is exactly the wrong thing to economise on.

Consequence for the agent: compressing becomes sorting. No second LLM judgement comes between
the reviewer and you any more.

The runner creates that home when it is missing and copies `auth.json` from `~/.codex` at every
round, so that a refreshed token comes along. Switching models = editing only that file.

> Do not put a `[profiles.specreview]` block in `~/.codex/config.toml`: in Codex ≥ 0.148 that
> is a hard start error. The v2 form (a separate `~/.codex/<name>.config.toml` + `--profile`)
> does work, but inherits the pollution of the base config — hence the separate home instead
> of a profile.

## Workflow

### 0. Preflight

No `codex` found? Report it loudly and ask whether you may go on in **fallback mode** (see
below); not in a controller run (if present). Do not go on silently — then you lose exactly the
property this skill is about.

Codex runs only in round 1, so only round 1 can hit a Codex limit.

**Codex budget exhausted beforehand = exit 6, fallback at once.** The runner checks the Codex
budget before the round and writes the decision to `<OUT>.budget.json`. If the budget is
exhausted (`CODEX_BUDGET: fallback — <reason>` on stderr), it stops with exit 6 without
`codex exec`. Round 1 then runs Opus-only (see *Fallback without Codex*): no question to the
user, no waiting. Report one line with `reason` and `reset_at` from `<OUT>.budget.json`. A
check that is not measurable lets the round go on to Codex.

**Codex limit in the middle of the round = fallback at once too, no waiting.** If round 1 fails
with exit 5, look in the events: `grep -o 'try again at [^"]*' <OUT>.events`. If a reset time
is there, round 1 runs Opus-only: no question to the user, no timer until the reset. The
ChatGPT limit often falls in the middle of a review loop; waiting costs hours, the fallback
minutes. Report the reset time in one line, and in the final report "before the round (exit 6)"
or "in the middle of the round (exit 5)". (`.err` shows only
`Reading additional input from stdin...`; the real error is in `.events`.)

### 1. Pick the spec

Argument given → that path. No argument → the newest file in the spec directory
(`python3 .lighthouse/lib/lighthouse_lib/config.py paths.specs`), and confirm which one you
take.

Then start an attempt (`dangerouslyDisableSandbox: true`, the script writes outside the working
directory):

```bash
python3 .lighthouse/scripts/plan_review_record.py start --kind spec <spec>
```

It prints the attempt directory (below `<ATTEMPT>`) and records the commit that round 1 reads.
Exit 2 for a spec with uncommitted changes: commit first. Shell variables do not survive the
next Bash call, so fill in the path literally. Every new review starts here again; you never
reuse an old attempt.

Rounds land in `<ATTEMPT>/round-<n>.json`, from round 2 on with `round-<n-1>.snapshot.md` and
`round-<n>.fixes.diff` (step 2). Not in the repo: the spec is the artefact, the review is a
tool. Between rounds the spec need not be committed — the snapshot compares the working tree,
not git.

### 2. Run a round — in the background

Before *every* dispatch a snapshot of the spec:

```bash
cp <spec> <ATTEMPT>/round-<n>.snapshot.md
```

**Only round 1 is a review**, and it is always paired (*Round 1: Codex and Opus*, below). Only
the retry from the table there runs Codex alone. Do it yourself, as a Bash call with
`run_in_background: true` and `dangerouslyDisableSandbox: true`, on one line, without `--context`:

```bash
.claude/skills/lighthouse-review-spec/review.sh <spec> 1 <ATTEMPT>/round-1.json none [max] > <ATTEMPT>/round-1.retry.log 2>&1; echo "exit=$?"
```

Read only the exit code, never `round-1.retry.log`. On success make the `ID` table with the jq
fragment from *Opus dispatch*, on `round-1.json`. For the exit codes see the exit table in the
plan skill, step 5.

**Round n ≥ 2 runs only if round n-1 had a BLOCKER/MAJOR with `accept`.** `defer` does not
count. No fixes, no round. Build the fixes diff first:

```bash
git diff --no-index <ATTEMPT>/round-<n-1>.snapshot.md <spec> > <ATTEMPT>/round-<n>.fixes.diff
```

`git diff --no-index` exits 1 when there is a difference; that is no error.

Then dispatch the verifier: the subagent from *Opus dispatch* (at the bottom), with
`verifier-prompt.md`, `PRIOR` = `<ATTEMPT>/round-<n-1>.json`, as context
`<ATTEMPT>/round-<n>.fixes.diff`, and `OUT` = `<ATTEMPT>/round-<n>.json`. Always Opus, also when
Codex still has budget: Codex is spared for round 1. No banner: this is the rule, no fallback.
The verifier does not search for problems again; it only verifies whether the fixes of the
previous round hold.

**Let every reviewer run in the background** (that is the default of the Agent tool): a Codex
round costs ~7 minutes on `high` and ~18 on `max`. You get a notification as soon as it is
done; keep working in the meantime.

The table carries an `ID` per finding: `r<round>-f<index>`, the position in `findings[]` before
sorting. Decisions refer only to that ID (step 3); a verification round also carries a
`Verifies`.

Codex runs through `review.sh`: one fixed command shape that handles the prompt substitution,
the clean `CODEX_HOME`, `--sandbox read-only` and `--output-schema`. The raw review stays in
`OUT`; read it only when a finding is unclear. `verify.sh` (next to this file) is a thin wrapper
around `review.sh` with the verification prompt and the verification schema; only the `.agents`
variant uses it, where no Opus subagent exists.

**The allowlist, or the background round stalls on a prompt.** Codex cannot run inside the
Claude sandbox (`failed to initialize in-process app-server client: Operation not permitted`),
so the call needs permission. Without an allowlist rule an unattended round waits for you
instead of running. `lighthouse init` puts these rules in `.claude/settings.json`:

```json
{ "permissions": { "allow": [
  "Bash(.claude/skills/lighthouse-review-spec/review.sh:*)",
  "Bash(.claude/skills/lighthouse-review-spec/verify.sh:*)"
] } }
```

#### Round 1: Codex and Opus

In round 1 Codex and an Opus reviewer run side by side; you decide blind. The mechanics are
written once, in the plan skill: `lighthouse-review-plan`, step 5, *Round 1: Codex
and Opus*. Follow that: two background reviewers in one message, then
`python3 .lighthouse/scripts/review_pair.py blind <ATTEMPT>` and decide on the A/B table
(step 3), and for a failed pair the table there. You run `blind`, like `merge` and `write`, with
`dangerouslyDisableSandbox: true`. Where that table says "run the retry from step 5 once", that
is the retry in step 2 here. Only this differs here:

- **Codex** (Bash, `run_in_background: true`, `dangerouslyDisableSandbox: true`), without
  `--context`:

  ```bash
  .claude/skills/lighthouse-review-spec/review.sh <spec> 1 <ATTEMPT>/round-1.codex.json \
    none [max] > <ATTEMPT>/round-1.codex.log 2>&1; echo "exit=$?"
  ```

  `[max]` is the level slot: leave it out, or write `max` if you want it. Then the record in
  step 5 gets `--level max`. A limit is in
  `<ATTEMPT>/round-1.codex.json.events`, not in `round-1.json.events`.

- **Opus** is the subagent from *Opus dispatch* below, with
  `OUT` = `<ATTEMPT>/round-1.fallback.json`.

Miss one of these three rules and the pair is lost:

- `git status --porcelain` is empty before the dispatch. Otherwise `blind` refuses. Between the
  dispatch and `blind` you write nothing in the worktree: no edit, no commit.
- The prompt asks the Opus subagent to return only `DONE <path> model=<exact model ID from your
  own system prompt>`: no findings, no count. Run no jq table and put no banner before `blind`.
  The reported ID goes unchanged as `--shadow-model` into the record (step 5).
- Do not read `pair.json`, the raw files or `round-1.codex.log` before `merge`. Of the Codex
  call read only the exit code.

### 3. Gate — you judge, not the reviewer

**Required sub-skill:** `superpowers:receiving-code-review`. Do not fix blindly.

Show the table and per BLOCKER/MAJOR a proposal:

| Decision | Meaning |
|---|---|
| accept | valid, adjust the spec |
| reject | invalid — **with a reason**, which goes along to the next round |
| defer | valid but belongs in the plan, not in the spec |

Spot-check every `file:line` a BLOCKER carries before you accept it. A reviewer that misreads
the codebase produces convincing nonsense. What you take over (`accept` on a BLOCKER/MAJOR,
`minor_yes` on a MINOR) you sort into three piles, after the builder check of superpowers
(`brainstorming/SKILL.md`):

- **Already answered** by spec or repo: the answer goes into the spec.
- **Small**: a reasonable builder decides it without anything else being built. Decide it
  yourself and mark it in the spec with `*(own choice after spec review, <ID>)*`.
- **The user's**: the answer changes what gets built. Only this pile becomes a question,
  bundled in one message: at most four, the most important first, each with a recommendation.
  In Claude Code one `AskUserQuestion`; in a controller run (if present) a numbered list as the
  last text (*Controller run*).

You decide MINORs yourself (`minor_yes`/`minor_no`); in the sidecar each MINOR gets its own line.
`reject` and `defer` get no pile.

In a verification round one extra check holds:
Every accepted ID is in `verified` or in `verifies` of a finding (accepted = `accept`). If one is missing, run *that* verification round
again (same `n`, same `.fixes.diff`, `round-<n>.json` overwritten). If the ID is missing again,
stop with `aborted` (step 5) and report that ID as an open point. MINORs never start a round.

After **every** round, the last one too, write the decisions to the sidecar
`<ATTEMPT>/round-<n>.decisions.json`, and **never** at the bottom of `OUT` itself: that makes
the JSON invalid. One object per finding, MINORs included. At `converged` and `round_limit`,
`write` (step 5) refuses a record with a finding without a decision (exit 4); only `aborted`
may have gaps.

```json
[
  {"finding_id": "r1-f2", "decision": "reject", "reason": "the migration is already in B3"},
  {"finding_id": "r1-f3", "decision": "minor_yes", "reason": "small, own choice"}
]
```

- `finding_id` only from the `ID` column, each exactly once.
- `decision`: `accept`, `reject`, `defer`, `accepted_open` (only at `round_limit`, set by
  `accept-open` in step 5, never by hand), `minor_yes` or `minor_no`.
- `same_as`: the ID of the same finding in an earlier round; otherwise leave it out.

`review.sh` names the sidecar in the prior block of round N+1, so that it sees what was
rejected and why.

**Paired round 1** (step 2, *Round 1: Codex and Opus*): you decide blind, as in step 6 of the
plan skill.

- Write `<ATTEMPT>/round-1.blind-decisions.json` instead of `round-1.decisions.json`, with the
  blind IDs (`A-f<i>`, `B-f<i>`).
- A duplicate finding (the same failure in the same place, by both reviewers) is one row with
  `also`: `{"finding_id": "A-f2", "also": "B-f1", "decision": "accept", "reason": "…"}`.
- Then run `python3 .lighthouse/scripts/review_pair.py merge <ATTEMPT>`
  (`dangerouslyDisableSandbox: true`). It writes `round-1.json`, `round-1.decisions.json` and
  `round-1.sources.json`, and shows the unblinded table with the `r1-f<i>` IDs.
- Exit 4 on a decision: repair the sidecar and run `merge` again. Exit 4 on a broken raw file:
  no pair (the table in the plan skill).

### 4. Fix

Only the accepts, only in the spec. No code, no adjacent files. Deferred items: one line in the
spec under *Open for the plan*.

### 5. Convergence

Stop at the first thing that is true. Every stop rule has an `outcome` for the record:

| Stop rule | `outcome` |
|---|---|
| Round 1 without `accept` on a BLOCKER/MAJOR — there is nothing to fix, so nothing to verify | `converged` |
| A verification round without an open BLOCKER/MAJOR (see step 3) — all accepted fixes hold | `converged` |
| Round 3 reached with open BLOCKERs/MAJORs — record them with `accept-open` and go on to `writing-plans`, never a question | `round_limit` |
| You say stop, or an accepted ID is missing also after a repeated verification round (step 3) | `aborted` |

**Open** = a BLOCKER/MAJOR from the last round without `reject` or `defer`, also an `accept`
you fixed in step 4: no reviewer saw that fix any more. `write` refuses `converged` with an open
point (exit 4).

**Only round 1 is a review.** From round 2 on the reviewer only verifies whether the fixes of
the previous round hold — no new search for problems; that was exactly the loop that did not
converge (see *Why this skill exists*). **No fixes, no round**: without an `accept` on a
BLOCKER/MAJOR in the previous round you run no next round.
**A third round only if a fix from round 2 does not hold** — the same finding two rounds in a
row is a structural problem, no round-4 candidate.

**Commit the spec fixes first**, before `accept-open` and the record:
`docs(spec): process spec review (#<issue>)`, in the artefact language of the project. `write`
hashes the spec at `HEAD` as `spec_sha256_final`; an uncommitted fix falls outside the record.
No fixes = no commit; never make an empty commit.

**At `round_limit` you record the open points and go on**, in this order:

1. `python3 .lighthouse/scripts/plan_review_record.py accept-open <ATTEMPT> --doc <spec>`
   (outside the sandbox, as with `start` and `write`). This rewrites `round-3.decisions.json`
   (every open point becomes `accepted_open`) and puts the block `heading:open_after_review` at
   the bottom of the spec; an existing block is replaced, never doubled. Exit 2 with on stderr
   `no open BLOCKER/MAJOR in the last round: nothing to accept` = nothing is open: the outcome
   is then `converged`, not `round_limit`. Exit 2 without that message is a call error: repair
   the call. Exit 4 (`ACCEPT-OPEN FAILED: …`): report the message literally, repair the cause
   (path, sidecar) and run again. If it still fails, put the open points in the handoff and go
   on to the next phase; never edit the sidecar or the document by hand, never a question.
2. Commit the spec if `git diff --quiet -- <spec>` reports a difference:
   `docs(spec): record open review points (#<issue>)`. No difference (resumed run) = no commit.
3. Only then the record (below, `--outcome round_limit`). Ask the user nothing: `round_limit`
   is no question. The record always comes first. After the record, in a controller run (if
   present), the run instruction follows (`controller.bij_rondegrens` in `lighthouse.toml`).

Then the record, at every outcome (`dangerouslyDisableSandbox: true`, otherwise `write` ends in
exit 4 and the record is lost):

```bash
python3 .lighthouse/scripts/plan_review_record.py write <ATTEMPT> \
  --outcome <converged|round_limit|aborted> --review-mode <codex|fallback> \
  [--reviewer-model <model>] [--shadow-model <model>] [--level max]
```

No `--spec` or `--issue`: for a spec review that is exit 2.

`--review-mode` says who reviewed round 1, as in step 9 of the plan skill:

- Paired round 1 (there is a `round-1.sources.json`): `--review-mode codex --shadow-model
  <model Opus reported>`.
- Only Codex made round 1: `--review-mode codex`.
- Only Opus made round 1 (*Fallback without Codex*): `--review-mode fallback
  --reviewer-model <reported model>`.

The verification rounds on Opus do not change `--review-mode`. Without a pair no
`--shadow-model`; `write` refuses both wrong sides with exit 4.

| exit | Meaning | You do |
|---|---|---|
| 0 | record written; the path is on stdout | report the path |
| 3 | not on the always-on host (`host.toml`): no record | report loudly that this review falls outside the measurement, on to closing |
| 4 | write or validation error: only the staging directory `.<attempt>.tmp/` stays | pass the message on literally; if it is a missing or wrong decision, repair the sidecar and run `write` again; otherwise on to closing |

The record is measurement material, the review itself is the product: a failed record never
blocks the handoff.

Close with: spec path, number of rounds, accepted/rejected/deferred, the final verdict of the
last round, `outcome`, and the record path or the message of `write`. End with
`/lighthouse-write-handoff`. Interactively, `superpowers:writing-plans` runs only in the next window
(a fixed seam); for a controller run (if present) see *Controller run*.

## Fallback without Codex

Only round 1 runs on Codex, so only round 1 has a fallback. Three triggers: no `codex` (ask
first, see *Preflight*; not in a controller run, if present), the budget exhausted beforehand
(exit 6), or a Codex limit in the middle of the round (exit 5 + `try again at`). For the last
two do not ask, act at once. Round 1 then runs Opus-only: the Opus side of the pair becomes
`round-1.json` (the table in the plan skill). If `codex` was missing at the preflight already,
no pair was dispatched: one *Opus dispatch* with `OUT` = `<ATTEMPT>/round-1.json`.

Put above the table of an Opus-only round 1:

> ⚠️ SAME-FAMILY FALLBACK — same model family as the author of the spec.
> Catches ground errors (code that does not exist), catches *no* shared blind spots.

A clean context still helps; other blind spots you do not get for free.

The record gets `--review-mode fallback --reviewer-model <model name of the subagent>` (step 5).

## Opus dispatch

One dispatch, three roles: the Opus side of the pair in round 1, the fallback when Codex does
not make round 1, and the verifier in every round ≥ 2.

Dispatch a `general-purpose` subagent with `model: "opus"` and the text between
`---BEGIN PROMPT---` and `---END PROMPT---` from `reviewer-prompt.md` (round 1) or
`verifier-prompt.md` (round ≥ 2), placeholders filled in by hand: `{{CONTEXT_PATHS}}` as lines
`- <path>`, and `{{PRIOR_REVIEW_BLOCK}}` empty in round 1 and from round 2 on the paragraph that
`review.sh` builds as `prior_block`, with `PRIOR` and its `.decisions.json` sidecar filled in.
Instruct it to write the JSON answer following `findings.schema.json` or
`verification.schema.json` to `OUT`. In a paired round 1 you skip the table below (step 2):
`blind` makes the table. Otherwise you make the `ID` column yourself, **before** sorting:

```bash
jq -r --arg round <n> '
  .findings | to_entries
  | map(.value + {id: "r\($round)-f\(.key + 1)"})
  | sort_by({"BLOCKER": 0, "MAJOR": 1, "MINOR": 2}[.severity])
  | .[]
  | "| \(.id) | \(.severity) | \(.spec_section) | \(.verifies // "") | \(.problem) | \(.failure_scenario) | \(.evidence) | \(.proposed_fix) |"
' <OUT>
```

Header: `| ID | Sev | Section | Verifies | Problem | Failure | Evidence | Proposed fix |`.
Directly below it, for a verification round:

```bash
jq -r '.verified // empty | "VERIFIED: " + (if length == 0 then "none" else join(", ") end)' \
  <OUT>
```

## Extensions

- Plans: `/lighthouse-review-plan`. Same runner, with its own prompt and schema
  through `--prompt`/`--schema` and the spec(s) through `--context` (see the head of
  `review.sh`).
- Verification: `verify.sh` (next to this file) is a second wrapper on the same runner, with
  `verifier-prompt.md` and `verification.schema.json` instead of the round-1 prompt and schema.
  Only the `.agents` variant uses it; in Claude Code Opus verifies (step 2).
- Tune the prompt in `reviewer-prompt.md` — this SKILL.md never reads the content, only the
  placeholders.

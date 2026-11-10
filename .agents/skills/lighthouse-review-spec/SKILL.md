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
| 2 | this skill: Codex, clean context, read-only | does the spec match the real code, what breaks on implementation |

We do not replace layer 1: it runs in brainstorming anyway and catches something else.

Discovery and verification long shared one mechanism: every round was a full review, so every
round also found new criticism instead of converging. Since then only round 1 is discovery;
round 2 and later only verify whether the fixes of the previous round hold.

## When

- Spec written (in `python3 .lighthouse/lib/lighthouse_lib/config.py paths.specs`), before
  `superpowers:writing-plans`.
- Also fine: a spec that comes from someone else and that you have to build on.

Not for: reviewing code or a diff (that is `/code-review`), or reviewing a plan (that is
`/lighthouse-review-plan`).

Controller runs exist only in Claude Code (the controller, if present, starts `claude -p`);
here *report and stop* holds headless too.

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

No `codex` found? Report it loudly and stop: there is no fallback outside Claude Code (see
*Fallback without Codex*). Do not go on silently — then you lose exactly the property this
skill is about.

**Codex limit: report and stop.** If a round fails with exit 5, look in the events:
`grep -o 'try again at [^"]*' <OUT>.events`. Outside Claude Code there is no subagent for a
fallback, and a review by yourself is no second pair of eyes: report the reset time and stop.
(`.err` shows only `Reading additional input from stdin...`; the real error is in `.events`.)

**Codex budget exhausted beforehand: report and stop.** The runner checks the budget before
every round and stops with exit 6 (`CODEX_BUDGET: fallback — <reason>`), without `codex exec`.
Report `reason` and `reset_at` from `<OUT>.budget.json` and stop **before** the round; the
outcome is `aborted`, as with the limit.

### 1. Pick the spec

Argument given → that path. No argument → the newest file in the spec directory
(`python3 .lighthouse/lib/lighthouse_lib/config.py paths.specs`), and confirm which one you
take.

Then start an attempt, outside the sandbox of your harness (or ask for escalation): the script
writes outside the working directory.

```bash
python3 .lighthouse/scripts/plan_review_record.py start --kind spec <spec>
```

It prints the attempt directory (below `<ATTEMPT>`) and records the commit that round 1 reads.
Exit 2 for a spec with uncommitted changes: commit first. Shell variables do not survive the
next shell call, so fill in the path literally. Every new review starts here again; you never
reuse an old attempt.

Rounds land in `<ATTEMPT>/round-<n>.json`, from round 2 on with `round-<n-1>.snapshot.md` and
`round-<n>.fixes.diff` (step 2). Not in the repo: the spec is the artefact, the review is a
tool. Between rounds the spec need not be committed — the snapshot compares the working tree,
not git.

### 2. Run a round — directly, in the foreground

**Paired round 1: Claude Code only.** There Codex and an Opus subagent run side by side in
round 1 by default, and Opus verifies rounds 2 and 3. Here that subagent does not exist; every
round runs through Codex, as below.

Here you call the runner yourself; the whole review then does come into your context. Before *every*
round a snapshot of the spec:

```bash
cp <spec> <ATTEMPT>/round-<n>.snapshot.md
```

**Only round 1 is a review:**

```bash
.agents/skills/lighthouse-review-spec/review.sh \
  <spec> 1 <ATTEMPT>/round-1.json none [max]
```

**Round n ≥ 2 runs only if round n-1 had a BLOCKER/MAJOR with `accept`.** `defer` does not
count. No fixes, no round. Build the fixes diff first:

```bash
git diff --no-index <ATTEMPT>/round-<n-1>.snapshot.md <spec> > <ATTEMPT>/round-<n>.fixes.diff
```

`git diff --no-index` exits 1 when there is a difference; that is no error. Then run:

```bash
.agents/skills/lighthouse-review-spec/verify.sh \
  <spec> <n> <ATTEMPT>/round-<n>.json \
  <ATTEMPT>/round-<n-1>.json [max] --context <ATTEMPT>/round-<n>.fixes.diff
```

`<PRIOR>` = the previous round, or `none` in round 1. Run the runner and the record script of
step 5 outside the sandbox of your harness (or ask for escalation): the runner writes under
`~/.codex-specreview/` and starts a `codex exec` with network, the record script writes to the
data directory (`~/.local/share/lighthouse/` by default). Timeout of the shell call: at least
20 minutes. Then show the findings sorted by severity, with the ID computed **before** sorting
(`r<n>-f<index>`):

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
jq -r '.verified // empty | "VERIFIED: " + (if length == 0 then "none" else join(", ") end)' <OUT>
```

Summarise nothing; read `review_markdown` only when a finding is unclear.

**If you run inside Codex yourself**, the reviewer is the same model family as the author. Put
the banner from *Fallback without Codex* above the table, and give the record in step 5
`--review-mode codex_same_family` instead of `codex`.

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
  Here: one message with a numbered list.

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
| You say stop, Codex stops on a limit (*Preflight*), or an accepted ID is missing also after a repeated verification round (step 3) | `aborted` |

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
   is no question. The record always comes first.

Then the record, at every outcome (outside the sandbox, see step 2):

```bash
python3 .lighthouse/scripts/plan_review_record.py write <ATTEMPT> \
  --outcome <converged|round_limit|aborted> --review-mode codex [--level max] \
  --skill-dir .agents/skills/lighthouse-review-spec
```

No `--spec` or `--issue`: for a spec review that is exit 2. From inside Codex itself:
`--review-mode codex_same_family` (step 2).

| exit | Meaning | You do |
|---|---|---|
| 0 | record written; the path is on stdout | report the path |
| 3 | not on the always-on host (`host.toml`): no record | report loudly that this review falls outside the measurement, on to closing |
| 4 | write or validation error: only the staging directory `.<attempt>.tmp/` stays | pass the message on literally; if it is a missing or wrong decision, repair the sidecar and run `write` again; otherwise on to closing |

The record is measurement material, the review itself is the product: a failed record never
blocks the handoff.

Close with: spec path, number of rounds, accepted/rejected/deferred, the final verdict of the
last round, `outcome`, and the record path or the message of `write`. End with
`/lighthouse-write-handoff` — `superpowers:writing-plans` runs only in the next window (a fixed
seam).

## Fallback without Codex

Only in Claude Code, where a `general-purpose` subagent exists. Here: report and stop (see
*Preflight*), both at a limit (exit 5) and at a budget exhausted beforehand (exit 6).

If you run inside Codex yourself (step 2, the same model family as the author), put above
*every* output:

> ⚠️ SAME-FAMILY FALLBACK — same model family as the author of the spec.
> Catches ground errors (code that does not exist), catches *no* shared blind spots.

A clean context still helps; other blind spots you do not get for free.

## Extensions

- Plans: `/lighthouse-review-plan`. Same runner, with its own prompt and schema
  through `--prompt`/`--schema` and the spec(s) through `--context` (see the head of
  `review.sh`).
- Verification: `verify.sh` (next to this file) is a second wrapper on the same runner, with
  `verifier-prompt.md` and `verification.schema.json` instead of the round-1 prompt and schema.
  From round 2 on it runs instead of `review.sh` (step 2).
- Tune the prompt in `reviewer-prompt.md` — this SKILL.md never reads the content, only the
  placeholders.

---
name: lighthouse-route-issue
description: 'Read-only triage of one or more issues in this repo — five gates (blocker, known recipe, bug, instruction layer, design depth), first hit wins, and per issue exactly one line with the route. Use for "which route for #610", "how would you pick this up", and when lighthouse-start-issue calls it. Never runs the route. NOTE — actually starting on an issue (worktree, board, execution) is lighthouse-start-issue.'
metadata:
  lighthouse: "core@v0.8.1"
  binding: default
  origin: own
---

# Route Issue Skill

From issue number to one line: which gate, which route, why. Read-only: this skill reads, weighs
and reports. It sets up no worktree, does not touch the board, sends out no reconnaissance and
calls no follow-up skill.

**`lighthouse-route-issue` never calls the route; it reports the line and stops. Only
`lighthouse-start-issue` executes.**

## Domain

In scope: deciding the route of an existing issue in this repo (`forge.repo`), for one number or a
list. Callers: `lighthouse-start-issue` (step 4, one number) and the backlog-stand skill, if present
(the split of *Ready*, many numbers).

Not here: starting on the issue or executing the route (`/lighthouse-start-issue`), writing or
updating an issue (`/lighthouse-write-issue`), auditing the board (the board-sweep skill, if
present).

## Labels

The label names come from `[labels]` in `lighthouse.toml`. Read each one with
`python3 .lighthouse/lib/lighthouse_lib/config.py labels.<key>`; below, `labels.<key>` stands for
the name that call prints:

- `config.py labels.blocked_issue`, `config.py labels.blocked_external`: waits on something
  outside the session;
- `config.py labels.needs_shaping`, `config.py labels.needs_decision`,
  `config.py labels.needs_hands`, `config.py labels.needs_testing`: what has to happen in the
  session.

## Input

One or more issue numbers. Per number:

- **The issue is already in context** — read in step 1 of `lighthouse-start-issue`, or in the list
  call of the backlog-stand skill, if present — then no new call.
- Otherwise one read call, with `dangerouslyDisableSandbox: true`:

  ```bash
  R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
  gh api "repos/$R/issues/<nr>"
  ```

  Only if `comments` > 0 follows
  `R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api "repos/$R/issues/<nr>/comments"`.
  Never `gh issue list --json` — that breaks under the rtk proxy.

## Output

Per issue exactly one line, in the order of the input:

```text
#<nr> heading:gate_line <n>[<a|b|c>]: <route> — <reason>
```

`heading:gate_line` is the gate word in the project language. At gate 4 always with the sub-letter:
`#610 heading:gate_line 4b: writing-plans — the AC settle the HOW`, never a bare
`heading:gate_line 4: ...`. If the triage fell through gate 3, the layer follows:
`… — <reason> · layer: <layer> (<file>)`. An accompanying skill from gate 1 goes in the reason.

The form is a contract. `.lighthouse/scripts/measure_start_issue_kpi.py` counts the gate line and
the sub-letter; the backlog-stand skill, if present, sorts *Ready* on the gate from this line.

## Triage: five gates, first hit wins

```
0. Does the issue carry labels.blocked_issue or labels.blocked_external, or does it have
   an open blocked_by without labels.needs_shaping/labels.needs_decision/labels.blocked_issue?
                                          → route: stop, reason: what it waits for (or: label missing)
1. Does the description of a skill match this issue,
   and does that skill deliver the end result of the issue?
                                          → route: that skill (closes)
2. Does the issue describe a bug or regression?
                                          → route: superpowers:systematic-debugging
3. Does the issue touch an instruction layer?
                                          → layer + file, fall through ↓
4. Is the HOW settled?                    → c / b / a
```

Only gate 3 falls through. The number in the line is the gate where the triage stopped; that makes
a wrongly chosen gate findable afterwards.

### Gate 0 — blocked

Two entries: the label, or a blocker without a label.

**With `labels.blocked_issue` or `labels.blocked_external`** the blocker comes from the relation,
not from the body:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/issues/<nr>/dependencies/blocked_by" \
  --jq '.[] | "#\(.number) \(.state)"'
```

If there is no open blocker, the reason reports the verdict — **lapsed** (only closed relations)
or **unlinked** (no relation) — with the repair command from `lighthouse-write-issue` 3e. The
route stays stop: removing the label or laying the relation is up to the user. If this GET fails,
or the output is unreadable, the verdict is **unknown**, with the read command added — the route
stays stop, and a failed or empty GET is never **unlinked** (that puts the user on the wrong
track of a `--remove-label`).

**Without that label, but with `issue_dependencies_summary.blocked_by > 0`** (already in the
payload of step 1, no extra call): if the issue carries none of `labels.needs_shaping`,
`labels.needs_decision` or `labels.blocked_issue`, this also stops at gate 0. Confirm with the same
list GET above and report **label missing** with the repair command from `lighthouse-write-issue`
3e (`gh issue edit <nr> --add-label <labels.blocked_issue>`). If that GET fails, report **unknown**
and stop — an unreadable blocker never falls silently through to gate 4.

If the issue does carry `labels.needs_shaping` or `labels.needs_decision` with an open
`blocked_by`, that is **not** a gate-0 stop: the lower label wins (invariant
`lighthouse-write-issue` 3e, "holds") and the route continues through that label — gate 4c.

**Controller run** (only if present: the prompt ends on `(issue #<nr>, controller-run)`): an open
blocker through `labels.blocked_issue` or a bare `blocked_by` does not stop then. The controller
allows only spec and plan and stacks the execution itself. Route on to gates 1–4 and put
`— blocker #<x> open` after the line. `labels.blocked_external` always stops, here too. The router
stays read-only: it only reports the route; not executing is in `lighthouse-start-issue`.

### Gate 1 — known recipe

The skill catalogue is in context from session start, descriptions and all. This gate therefore
costs **zero tool calls**: read the issue, hold it against those descriptions. Do not grep over
`.claude/skills/` — that is the tool call that hits the KPI ceiling before anything is routed.

**Scope**: only `lighthouse-*`, the project's own skills and `superpowers:*`. The catalogue can
also hold skills that target *another repo*; those never enter the route.

**Test on the outcome**: *does this skill deliver the end result of the issue, or only a norm for
work that still has to happen?* Only the first closes.

| | Example | Role |
|---|---|---|
| closing | `lighthouse-audit-worktrees`, a skill that refreshes lockfiles | the skill *does* the issue |
| accompanying | a skill with the writing norms of a language, a diagram skill | the skill carries a norm |

Those two rows are illustration, not a list to maintain. An accompanying skill closes nothing: it
goes along to gate 4 as context for the execution.

### Gate 2 — bug

The route is always `superpowers:systematic-debugging`. No sub-choice for a debugger with
persistent debug state: that does not weigh up against a bug that is done in five minutes. If the
state turns out to be needed during debugging, that is an in-session decision, not routing up
front.

Recognition is in the issue body, not in a label — there is no bug label in `[labels]` and there
will not be one: those labels mark what blocks, not what kind of work it is.

### Gate 3 — instruction layer

The issue asks to change a rule, norm, convention or skill. Apply the instruction-layer table of
the project (in `AGENTS.md`, if it has one), decide which layer and which file the change should
touch, and fall through to gate 4 — mostly a or b. No extra turn, no `AskUserQuestion`; the layer
choice travels along in the line.

### Gate 4 — is the HOW settled?

| Exit | When | Route |
|---|---|---|
| **c** | the issue allows several defensible designs | `superpowers:brainstorming` → spec |
| **b** | the AC settle the HOW; only order and execution remain | `superpowers:writing-plans` |
| **a** | one file, zero design choice, AC executable as written | execute directly |

Research issues ("find out whether X can") fall under **c**.

Also under **c** falls an issue that demonstrably touches an open question from the
`### heading:milestone_not_yet_decided` section of its milestone — the description is already in
context (`milestone.description` in the issue payload, or step 1 of the backlog-stand skill, if
present), no extra call. The same behaviour as `labels.needs_shaping`, but without a label. Open =
a bullet without `→ #<nr>`; only a question that demonstrably concerns the work of this issue
counts. A milestone without that heading, or with "None.", routes as usual. The reason names the
question: `— touches open question "<question>" from the milestone`. After the spec comes the
outflow from `lighthouse-write-issue`: a question that has become sharp enough becomes an issue of
its own and the bullet gets `→ #<nr>`.

There is no tie-break rule. The skill weighs and names one route; if it is off, the user corrects
it in one sentence. A rule that always leans to the expensive side when in doubt costs more than
that sentence.

## A blocking label steers the triage, it does not stop it

Gate 0 catches the two blocked labels. The other four are exactly the signal *which* route the
issue gets.

| Label | Route |
|---|---|
| `labels.needs_shaping` | **gate 4, c** — that *is* exactly the missing step |
| `labels.needs_decision` | **gate 4, c**, narrow: put the open decision as the first question |
| `labels.needs_hands` | **gate 4, b/a**, execute up to the hand movement, with an explicit instruction *where* human hands are needed |
| `labels.needs_testing` | **gate 4, b/a**, execute and let the human judge whether it does what was meant |
| `labels.blocked_issue` | **gate 0** — stop, report the blocker from the `blocked_by` relation, not from the body; in a controller run (if present): route on, see Gate 0 |
| `labels.blocked_external` | **gate 0** — stop, report what it waits for |

Only the two blocked labels are a hard stop (except `labels.blocked_issue` in a controller run,
see Gate 0, which applies only if present): those wait on something outside the session. The
other four describe what has to happen *in* the session.

## Non-negotiable

- ✅ Report the line and stop · ❌ call the route yourself, set up a worktree or update the board
- ✅ The gates in order, first hit wins · ❌ ask gate 4 as the first question
- ✅ Per issue one line `#<nr> heading:gate_line <n>[<a|b|c>]: <route> — <reason>`, at gate 4 with
  sub-letter · ❌ route silently, or a bare `heading:gate_line 4: ...` without sub-letter
- ✅ Choose one route and name it · ❌ propose two routes and let the user choose
- ✅ Gate 1 from the catalogue in context · ❌ grep over `.claude/skills/` (costs the KPI ceiling)
- ✅ `labels.blocked_*` stops (`labels.blocked_issue` not in a controller run, if present, see Gate 0) · ❌ treat a `labels.needs_*` label as a stop
- ✅ Do not fetch an issue that is already in context again · ❌ a second read call per number

## Known pitfalls

- **Treating an accompanying skill as gate 1.** A skill with the norms for shell scripts does
  *not* close an issue about a hook — it carries the norm you write it with. Gate 1 stops the
  routing; do that only when the skill delivers the end result.
- **Skills for another repo in the catalogue.** They target another repo and another board. They
  sometimes match an issue here convincingly; they never belong in gate 1.

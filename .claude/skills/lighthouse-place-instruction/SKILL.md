---
name: lighthouse-place-instruction
description: 'Decides on which layer a new instruction belongs — CLAUDE.md or AGENTS.md, a path-scoped .claude/rules/ rule, a skill or a hook — and enforces the pairing rule (no hook without a counterpart). Call with /lighthouse-place-instruction. NOTE — not for the craft of writing a skill itself (frontmatter syntax, examples, structure): use superpowers:writing-skills instead.'
disable-model-invocation: true
metadata:
  lighthouse: "core@v0.8.1"
  binding: default
  origin: own
---

# Place Instruction

From "this has to be recorded" to one layer, one file, one measurement command.

## Domain

In scope: choosing *where* an instruction lives, and putting the file down in the right form.

Not here: the craft of writing a skill — this skill delegates that to
`superpowers:writing-skills`. And not the guarding afterwards: that is
`.lighthouse/scripts/check-instructions.sh`.

## Step 1 — External first. Write nothing until this is done.

The cheapest skill is someone else's. Before you write anything, look:

```bash
npx skills find "<topic>"
npx skills add -l <owner>/<repo>     # show the content, install nothing
```

Fixed places: `anthropics/skills`, `obra/superpowers`, the official marketplace.

If you find something usable: **vet the licence before adoption.** Copying is the one act that
makes copyleft actually apply, also for a hosted product. Permissive (MIT/Apache-2.0/BSD/ISC/HPND)
is allowed; no licence to be found → do not take it over. Adopted material gets
`origin: extended` or `origin: external` plus `upstream` with the source *and* the licence — put
the licence name after a `(` or a `,` (`(MIT, © Someone)` or `, MIT`): a bare name does not
count, because otherwise a licence name that happens to occur in a path
(`github.com/apache/airflow`, `foo/mit-tools`) matches too.

Adopted material also carries `upstream-commit`: the full SHA (40 hex, double-quoted) of the
commit the copy **descends from** — not today's HEAD. With `origin: external` comes
`upstream-path`: the path inside the upstream repo to the skill directory or the source file,
without a leading `/`. Without a pin, falling behind is invisible and an update is no reviewable
diff.

## Step 2 — The provenance gate. No source, no instruction.

**The question:** where does the content of this instruction come from?

Valid sources:

- a session in which it actually went wrong, with the user's correction — by far the most common
  case
- a measured defect: a red CI run, a hook that fired, an outage, an OOM
- git history of fixes, or a closed issue
- upstream or vendor documentation (the Agent Skills spec, systemd, Anthropic, a library)
- a spec or ADR in the repo
- the user telling it in this conversation

**The only case that blocks: general model knowledge.** No source does not mean "then we just
write it down" — that yields text that *sounds* like knowledge ("handle errors carefully",
"document your assumptions") and costs context in every session without ever changing behaviour.
Then write nothing, and say this:

> There is no material yet to base this instruction on. Do it once for real — I note your
> corrections — and after that we distil the norm from it. Otherwise you get an instruction that
> sounds like knowledge but is not.

**No ceremony.** If the source is evident — the user corrected you two messages ago — one line
that names it is enough. The gate stops *only* at *no* source. A gate that starts an
interrogation at every trivial `CLAUDE.md` line becomes the first step everyone skips.

This gate holds for **all five layers** of step 3, not only for skills. It stands before the
choice of layer because without a source you choose no layer: then you write nothing. If it
becomes a skill, the answer lands in the `source` field (step 5); for a `CLAUDE.md` line or a
rule it lands in the commit message or the issue — `CLAUDE.md` has no frontmatter per line and a
rule carries one `paths:` block for a whole topic.

## Step 3 — Choose the layer

Five places, with substantially different triggers and costs:

| Place | Trigger | Always-on cost |
|---|---|---|
| root `AGENTS.md` | session start (Claude Code through `@AGENTS.md`) | the full body |
| root `CLAUDE.md` | session start, Claude Code only | the full body |
| nested `CLAUDE.md` | Claude reads a file in that directory | none |
| `.claude/rules/*.md` + `paths:` | Claude reads a file that matches the glob | none |
| skill (ambient) | the model judges relevance on the description | only the description |
| hook | a tool call matches | none |

```
Must this be enforced, also when the model decides otherwise?
├── yes → hook  (+ required counterpart, see step 4)
└── no
    │
    Does it hang on a path or file type?
    ├── yes → .claude/rules/<topic>.md with paths:
    └── no
        │
        Is it a procedure (steps, decision logic, measurement commands)?
        ├── yes → skill
        └── no  → root AGENTS.md  (Claude Code only? → root CLAUDE.md; keep it short)
```

**Harness memory is no layer.** Claude Code auto-memory and Codex memories live on one machine,
outside the repo, unreviewed and unread by the other harnesses. Never write a rule there, also
not when the harness prompt asks for it: put it on one of the five places above. What does not
earn a place stays in the issue or PR it came from.

One extra branch, and it is the easiest to miss: **must it fire while no file is open yet** —
during brainstorming, planning, or designing a new service? Then a rule cannot do it, because it
waits for a file read. That is the only case in which an ambient skill beats a rule, and it is a
real case: the pattern for a new service you must know before you create the first file.

**A rule without `paths:` is `CLAUDE.md` with extra steps.** The gain is entirely in the scoping.
Check 7 of `.lighthouse/scripts/check-instructions.sh` blocks on this.

## Step 4 — For a hook: refuse without a counterpart

**Hook and skill/rule are one rule at two moments.** The skill or rule carries the norm, the
rationale and the measurement command — beforehand, while writing. The hook carries the
enforcement — afterwards, at the tool call.

**Deliver no hook without a counterpart.** Refuse it, also when asked: without a counterpart the
agent learns the rule only through punishment — retry loops, wasted tokens, and with a weaker
model a loop that does not converge because it does not *know* what the hook wants. Conversely,
calling a norm "hard" without a hook is a norm with a nice title.

Concretely: the skill or rule gets `binding: hard` and **names the hook path literally in its
body** (`.claude/hooks/<name>.sh`, a subdirectory is allowed). Check 8 tests that the path exists
*and* is registered in `.claude/settings.json` — harness-independent: the check runs through
`.lighthouse/scripts/check-instructions.sh`, not through Claude Code itself. A hook script that
is registered nowhere never runs.

`.claude/settings.json` registers the hook. **OpenCode and Codex do not run those hooks** — so
the norm must stand there in `AGENTS.md` as prose — that every harness loads, Claude Code through
`@AGENTS.md`.

## Step 5 — The frontmatter contract

Eleven fields is audit machinery for an organisation with many contributors and a skill-PR
review. These fields earn their place, each because a **decision** hangs on it that otherwise
gets lost:

| Field | Values | Why this field exists |
|---|---|---|
| `binding` | `hard` \| `default` | `hard` is valid only with a hook counterpart. Machine-checkable, and it makes the pairing rule checkable instead of well-meant |
| `origin` | `own` \| `extended` \| `external` | Decides whether upstream maintenance and a licence obligation hang on it |
| `upstream` | source URL; leave the key out when there is none | Required at `extended`/`external`. Without this field an adopted skill cannot be updated or attributed three months on |
| `upstream-commit` | full SHA, double-quoted | Belongs to `upstream`, no decision of its own: the commit the copy descends from. Quote it — a SHA of only digits otherwise parses as a number |
| `upstream-path` | path in the upstream repo, without a leading `/` | Belongs to `upstream`; required at `external` |
| `source` | `"#<nr>"` \| commit SHA \| path in the repo \| public URL \| `self` | The answer to the gate of step 2. Without the field that answer evaporates as soon as the conversation is over, and three months on nobody can tell whether a norm rests on anything |

`source` has five forms, and the difference is in who can reach it:

| Form | Example | Consequence |
|---|---|---|
| issue or PR | `source: "#42"` | the discussion is the source and stays readable; quote it, otherwise YAML reads `#` as a comment and the value is empty |
| commit | `source: 68af7e22` | a measured fix; `git show` is the evidence |
| path in the repo | `source: docs/specs/<spec>.md` | when that file changes, the skill should move along — and the path is machine-checkable |
| public URL | `source: https://agentskills.io/specification` | anyone can check it; the skill may summarise and refer |
| `self` | `source: self` | the user told it, recorded nowhere else |

`self` is the escape and therefore carries the heaviest demand: there is **no original** to fall
back on, so the skill must carry the substance fully itself. Whoever uses it without writing out
that substance leaves a reference that points nowhere.

**Absence is no error** — existing skills do not get the fields after the fact.

**Nested under `metadata:`, never top-level.** The Agent Skills specification
(https://agentskills.io/specification) allows exactly six top-level fields — `name`,
`description`, `license`, `compatibility`, `metadata`, `allowed-tools` — and designates
`metadata` for properties the spec itself does not know: *"A map from string keys to string
values. Clients can use this to store additional properties not defined by the Agent Skills
spec."* Under `metadata:` the values are strings, and a key without a value should **stay away**,
not stand empty — an `upstream` that does not exist is a missing key, not an empty line
`upstream:`. `metadata:` is a block mapping: `lighthouse init` adds `metadata.lighthouse` to
every skill it installs.

Two fields do stay top-level: `disable-model-invocation` and `argument-hint`. Claude Code reads
them there and nesting breaks them. That is outside the spec, on purpose, and it should be said
explicitly — not silently.

Deliberately *not* adopted from the CEDA schema: `type`, `subtype`, `scope`, `activation`,
`execution`, `verifies`, `id`, `version`. They can be derived from the location of the file or
from the frontmatter Claude Code already knows, and a derived field kept up by hand drifts. `type` stays away in particular: the name carries the kind of the unit (see the
name shapes in step 6), so a field would only repeat it.

That argument does *not* hold for provenance: it cannot be derived from anything, and that is why
`source` is the one field adopted after all.

## Step 6 — Delegate the craft

The writing itself goes to `superpowers:writing-skills`. Do not override it — that is generic
craft, better maintained elsewhere. What this skill adds is what is not in there: the five
places, the provenance gate, the pairing rule, the name contract and the fields.

Demands that come on top:

- **The project's skill prefix**, always:
  `python3 .lighthouse/lib/lighthouse_lib/config.py project.skill_prefix` (rc 1: no prefix set —
  choose one and set it in `[project]`). Personal skills in `~/.claude/skills/` shadow project
  skills of the same name — the reverse of `settings.json` — and that happens silently (check 5).
  Exception: a vendored `origin: external` skill, because renaming breaks the update route and
  the attribution there. Lighthouse's own skills carry `lighthouse-`.
- **A negative boundary in the description.** Descriptions stand permanently in context and the
  model chooses on similarity. Form: `NOTE — not for X: use Y instead`. Two demands at once: a
  scope word as a separate word (`not for`, `instead`), *and* another skill by name. Check 4
  tests this as soon as two descriptions share five trigger words.
- **The name shape follows the kind of unit.** The prefix goes in front; the shape after it:

  | Form | Name | Why |
  |---|---|---|
  | workflow skill (the session *does* the task) | verb + object: `write-issue`, `review-spec` | a steps sequence or decision logic reads as an action |
  | standard (quality bar plus our preference and style, loaded next to another task) | `<topic>-standard`: `go-standard`, `test-standard` | the name says it is a yardstick, not a task |
  | hook, gate (registered on `PreToolUse`, `Stop` or `SubagentStop` and exits 2: the action does not get through) | `<object>-gate`: `main-commit-gate` | the name warns that it can stop the action |
  | hook, check (reports or warns afterwards, with or without exit 2: the action has happened) | `<object>-check`: `md-link-check` | the name promises no prevention |
  | agent | `<object>-<occupation>`: `issue-impact-scout` | an agent is a profession, named like one |

  A hook script with several modes is named after its strongest: if one mode can stop the
  action, it is a gate (`commit-test-gate`: pre blocks, post reports, so a gate). Check 14 reads only
  the lighthouse hooks in `.claude/settings.json` (commands under `.claude/hooks/lighthouse/`):
  `-gate` or `-check` at the end, a gate on a blocking event, a check not on `Stop` or
  `SubagentStop`. For a project's own hooks the convention is advice. Whether a `-check` on
  `PreToolUse` never blocks, and whether a workflow skill starts with a verb, no script can
  measure; that stays prose.
- **Both variants, `.claude/skills/` and `.agents/skills/`**, in the same reviewed change. Check 3
  catches a missing name, check 11 a content difference that is not recorded.

## Step 7 — Validate

```bash
.lighthouse/scripts/check-instructions.sh
```

Plus the project's own tests on its instruction layer, if it has any.

## KPIs

| Metric | Target | Measurement |
|---|---:|---|
| Rules without `paths:` | 0 | `.lighthouse/scripts/check-instructions.sh` (check 7) |
| Skills with `binding: hard` without a registered hook | 0 | `.lighthouse/scripts/check-instructions.sh` (check 8) |
| Words in `CLAUDE.md` + `AGENTS.md` | ≤ the ceiling (`python3 .lighthouse/lib/lighthouse_lib/config.py project.instructions_word_ceiling`, default 3000) | `.lighthouse/scripts/check-instructions.sh` (check 10); `lighthouse init` and `init --upgrade` report the budget before writing |
| Ambient skill surface (characters name+description) | falling | `python3 scripts/measure_skill_surface.py --ambient .claude/skills` (only if present) |
| Mirror break `.claude/skills` ↔ `.agents/skills` | 0 | `.lighthouse/scripts/check-instructions.sh` (check 3) |
| Personal skills that shadow a project skill | 0 | `.lighthouse/scripts/check-instructions.sh` (check 5) |

## Non-negotiable

- ✅ Search externally before the first line · ❌ writing and then discovering it existed
- ✅ Every rule with `paths:` · ❌ an unscoped rule
- ✅ A hook with a counterpart · ❌ delivering a hook because it was asked for
- ✅ Contract fields under `metadata:` · ❌ top-level, outside the six fields the spec allows
- ✅ The project's skill prefix · ❌ a bare name a personal skill can shadow
- ✅ A negative boundary in the description · ❌ a description that only says what it does do
- ✅ Both variants in the same reviewed change · ❌ an unreviewed copy, or an own skill that updates itself

## Known pitfalls

- **A rule fires on a *read*, not on a *write*.** A new file has no path to match. For the case
  "a new service" the skill carries it, not the rule.
- **`disable-model-invocation: true` takes the description out of context** but leaves the skill
  reachable with `/`. Only set it on what you call by name anyway; a skill that must fire on
  natural language loses its only trigger with it.

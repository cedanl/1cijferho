---
name: lighthouse-write-issue
description: 'Writing and maintaining GitHub issues and milestones of this repo. Use when creating new issues, editing issue titles or bodies, setting a block between issues, or cleaning up issue metadata. NOTE — not for a PR body: that follows .github/pull_request_template.md and lighthouse-write-pr.'
metadata:
  lighthouse: "core@v0.8.1"
---

# Writing and maintaining GitHub issues

Standards for the issues of this repository. Pull requests follow
`.github/pull_request_template.md` (`lighthouse-write-pr`). Issues and milestones are artefacts:
write them in the project language (`python3 .lighthouse/lib/lighthouse_lib/config.py project.language`).
Headings that code or a skill reads are written as `heading:<key>`: write the variant that
`python3 .lighthouse/lib/lighthouse_lib/headings.py <key>` prints.

## Tool: use `gh api`, not `gh issue list --json`

`gh issue list --json` breaks under some proxies:

```
Failed to parse gh issue list output / expected value at line 1 column 1
```

For everything you have to read, use
`R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api "repos/$R/issues?..."`.
`gh issue create` and `gh issue edit` do work as usual.

Second trap: **an unknown `--label` filter is silently ignored** instead of failing. You then get
the full, unfiltered result back and count nonsense. Verify a label name with `gh label list`
before you filter on it.

## Workflow

At `/lighthouse-write-issue [description]`:

### 0. Does this exist already?

Ask this before you choose a template: a duplicate issue costs a closing action later. It is a
targeted search on *this* topic, no bulk scan. Choose 1-2 key terms from the description
(function, file, config key, topic) and run this with `dangerouslyDisableSandbox: true` (`gh`
needs network).

Search in issues, open and closed, bounded to five:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
T='<key term>'
gh api -X GET search/issues -f q="repo:$R is:issue $T" \
  --jq '.items[:5][] | "#\(.number) \(.state) \(.title)"'
```

Then look in the codebase and docs, whether the work is already there:

```bash
B=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.default_branch) || exit 2
git fetch origin "$B" || echo 'FETCH FAILED: origin/'"$B"' may be stale'
git grep -l '<key term>' "origin/$B" -- .
```

`git grep` gives exit 1 at zero hits; only exit ≥ 2 (e.g. 128) is an error. Precision goes
before recall: only a hit that demonstrably covers the same work counts. "Looks like it" is no
hit.

- No covering hit: report exactly one line `duplicate? no — searched on <terms>`, also without
  any hit, never silence. On to step 1, without a question. If `FETCH FAILED` is in the output,
  the codebase look was done on a possibly stale `origin/$B`: then report
  `duplicate? not measurable — fetch failed` instead of `no`, and go on to step 1.
- A covering hit: report it and let the user choose (`AskUserQuestion` in Claude Code, otherwise
  a question in chat) from **do not create** · **add to** the existing issue (a comment or a body
  change through `gh api`) · **create anyway**, with the reason in the body
  (`Differs from #<nr>: …`).

This step only reports: it closes nothing and sets no label.

### 1. Choose the template

Two templates, chosen on content. Do not try to set a GitHub issue type (`issueTypeId`): issue
types are an organisation feature, and on a personal account `.type` is `null` on every issue.

| Input | Template |
|---|---|
| Broken behaviour, reproducible | Bug |
| Everything else | Task |

A **pitch** is no issue form. Larger chunks of work become a milestone; the pitch stands in the
milestone description (see "Milestones").

### 2. Format the body

Use the template from "Issue Templates". Take the user's input as the base and add context from
the codebase where needed.

### 3. Set the labels

The label taxonomy of the project lives in one file, not in this skill:

```bash
python3 .lighthouse/lib/lighthouse_lib/config.py paths.labels
```

rc 0: read that file and follow it — which axes exist, which labels each axis has, the
boundaries between them. rc 1 (not set): only the `[labels]` keys and the GitHub default labels
(`bug`, `enhancement`, `documentation`, …) apply. rc 2: stop, the configuration is broken.

#### 3a. Location axis

`python3 .lighthouse/lib/lighthouse_lib/config.py labels.location_prefix`. rc 0: every issue
carries exactly one label with that prefix; the taxonomy file says which ones and when. rc 1: the
project has no location axis, skip this step.

#### 3b. Area axis

`python3 .lighthouse/lib/lighthouse_lib/config.py labels.area_prefix`. rc 0: every issue carries
exactly one label with that prefix. If two fit, choose the centre of gravity — with two labels the
issue counts in two axis views and the axis can no longer be audited. rc 1: no area axis.

#### 3c. The block

A blocking label marks **what blocks the next step**, not what occurs somewhere in the issue. It
is a state, no property: it goes on as soon as the work reaches the human step and comes off
again (see "An existing issue").

The test: **can I just say this to the agent?** If so → no label, however much work it is. No
label means it can be picked up; that is the ready queue.

At most one. If two apply, the **lowest rank** wins — you cannot connect what has not been
delivered yet, and not judge what has not been built yet. The names come from `[labels]`
(`python3 .lighthouse/lib/lighthouse_lib/config.py labels.<key>`):

| Rank | Key | Binds when |
|---|---|---|
| 1 | `labels.needs_shaping` | What/why not sharp yet. Everything after it is premature |
| 2 | `labels.needs_decision` | One choice of the user blocks, the rest is clear |
| 3 | `labels.blocked_issue` | Waits on another issue in this repo |
| 4 | `labels.blocked_external` | Waits on a third party, API, vendor, delivery |
| 5 | `labels.needs_hands` | The user has to set up, enter, connect, buy |
| 6 | `labels.needs_testing` | The build stands; the user has to judge whether it does what was meant |

The boundary hands/testing: hands is **setting up, entering, connecting**; testing is
**verifying that something works as meant**. The technical test the agent has done already; what
remains is human judgement. `labels.needs_testing` can never stand before an implementation and
is so also the "almost done" signal.

Not to be confused with a board Status `On hold` (only if a board is configured): that is
*parked on purpose*, a blocking label is *something is in the way*. An issue can be parked and
unshaped at the same time.

`labels.blocked_issue` never goes along in `gh issue create`: it leans on the `blocked_by`
relation, and that comes first. See 3e.

#### 3d. Milestone — does it fit thematically in a running block?

Fetch the open milestones, with description:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/milestones?state=open&per_page=100" \
  --jq '.[] | "\(.title)\t\(.open_issues) open\n\(.description)\n---"'
```

Start with the milestones whose name matches the area of the issue, but look at the rest too.
The question is **thematic**: does this issue help solve the problem from the milestone's
*Problem / Opportunity*, and does it bring its `### heading:milestone_dod` closer? An issue that
only falls in the same domain does not belong yet.

Be critical. No milestone is the default, no emergency measure:

- If the issue does not touch the DoD, it does not belong, even if it falls in the same area.
- If it falls under the `### heading:milestone_no_gos`, it does not belong.
- If it stretches the appetite, it does not belong. A block does not grow silently.
- If two milestones fit, or it is an edge case, put it to the user with one sentence of
  motivation. Do not choose yourself.

If it does not fit: leave `--milestone` out in step 4. The issue goes to the backlog. Always name
the outcome in step 5: which milestone and why, or "backlog" with the closest milestone and why
it did not fit.

#### 3e. Set a block

If the issue waits on another issue in this repo, the blocked-issue label leans on the GitHub
relation `blocked_by`. The relation comes **before** the label, so that a half failure never
leaves a label without a relation. This runs after step 4; for an existing issue that gets a
block, step 1 below drops (see "An existing issue").

The invariant, per open issue (label names from `[labels]`):

| Label | open `blocked_by` | Verdict |
|---|---|---|
| `labels.blocked_issue` | ≥ 1 | **holds** |
| `labels.blocked_issue` | 0, but closed relations | **lapsed**: label off |
| `labels.blocked_issue` | no relation | **unlinked**: set the relation, or label off |
| `labels.needs_shaping` or `labels.needs_decision` | ≥ 1 | **holds**: the lower label wins, the relation waits |
| no blocking label, or one with a higher rank than `labels.blocked_issue` | ≥ 1 | **label missing**: `labels.blocked_issue` on (the higher label off) |
| no label or another one | 0 | **holds**, outside this invariant |

`labels.blocked_external` falls outside it: it waits on something outside the repo. Repair:
label off is `gh issue edit <nr> --remove-label "$BLOCKED"`, setting the relation is step 2 below.

A `blocked_by` relation between two issues the controller runs (only if present) is also their
stacking order: the controller lets the blocked issue do spec and plan and stacks the execution
on the green PR of its predecessor. The invariant and the steps below stay the same.

1. **Create without the blocked-issue label** (step 4). Note the start time beforehand with
   `date -u +%Y-%m-%dT%H:%M:%SZ` and put the literal ISO value in your answer: a shell variable
   (`START=...`) does not survive a new Bash call, so the search call after a failed create (the
   failure table below) uses that literal text, never `$START`. The number `<nr>` comes from the
   URL.
2. **Set the relation.** `issue_id` is the numeric id of the blocker, not its issue number:

   ```bash
   R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
   ID=$(gh api "repos/$R/issues/<blocker-nr>" --jq .id)
   gh api -X POST "repos/$R/issues/<nr>/dependencies/blocked_by" -F issue_id="$ID"
   ```

3. **Verify with the list, not from the summary**: `issue_dependencies_summary` runs one write
   behind and then reports a successful POST as failed. The blocker must be here:

   ```bash
   R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
   gh api "repos/$R/issues/<nr>/dependencies/blocked_by" --jq '.[].number'
   ```

4. **Only then the label.** If `labels.needs_shaping` or `labels.needs_decision` is on already,
   that label wins and the blocked-issue label does not go on. A blocking label with a higher
   rank comes off in the same edit (`--remove-label <label>`):

   ```bash
   BLOCKED=$(python3 .lighthouse/lib/lighthouse_lib/config.py labels.blocked_issue) || exit 2
   gh issue edit <nr> --add-label "$BLOCKED"
   ```

5. **Final check**: the list from step 3 and
   `R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api "repos/$R/issues/<nr>" --jq '[.labels[].name]'`.
   Only when relation and winning blocking label stand there together, report success.

At every error you never derive the state from which step failed: you read it and report what
*really* stands, with the command that repairs the invariant. Never remove a relation to
compensate; a relation without a label is caught by the sweep as **label missing**.

| Failure point | Read | Report |
|---|---|---|
| `gh issue create` fails or gives no URL | `R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && T='<title>' && gh api "search/issues?q=repo:$R+in:title+\"$T\""` | exactly one hit with `created_at` after the noted start time (the literal ISO value from step 1, not `$START`) and the same body: go on with that number. No hit: error, stop. More than one, an older one, another body, or the search call fails: **unknown** + the search call, stop before every mutation. Never create again blindly |
| POST fails | the list from step 3 | the relation stands: on to the label. It does not: one retry, then "#N stands without relation and without label" + the POST command |
| the verification misses the blocker | the same | as above; the summary does not count |
| setting the label fails | the label command from step 5 | the label stands: go on. It does not: "relation stands, label missing" + `gh issue edit <nr> --add-label "$BLOCKED"` |
| the final check differs, or a GET fails | — | the real state, or **unknown** with the read command. Never success |

### 4. Create the issue

`--label` always carries one label per axis the project has (3a, 3b).

```bash
gh issue create \
  --title "<title>" \
  --label "<location label>,<area label>" \
  --milestone "<milestone>" \
  --body "$(cat <<'EOF'
<formatted body>
EOF
)"
```

If the project has a board with auto-add, it fills itself; otherwise add the issue by hand.

If this command fails or gives no URL: go to 3e, the failure table, row `gh issue create fails or
gives no URL`.

### 5. Report the result

Show the issue URL `gh issue create` returns, plus the milestone outcome from step 3d. If the
issue waits on another issue: first run 3e steps 2–5 and report the final check (or
**unknown**), not only the URL.

### 6. Check the invariants

After bulk work on labels, with the prefixes from 3a and 3b and the blocking labels from 3c:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
LOC=$(python3 .lighthouse/lib/lighthouse_lib/config.py labels.location_prefix)
AREA=$(python3 .lighthouse/lib/lighthouse_lib/config.py labels.area_prefix)
BLK=$({ python3 .lighthouse/lib/lighthouse_lib/config.py labels.needs_shaping
  python3 .lighthouse/lib/lighthouse_lib/config.py labels.needs_decision
  python3 .lighthouse/lib/lighthouse_lib/config.py labels.blocked_issue
  python3 .lighthouse/lib/lighthouse_lib/config.py labels.blocked_external
  python3 .lighthouse/lib/lighthouse_lib/config.py labels.needs_hands
  python3 .lighthouse/lib/lighthouse_lib/config.py labels.needs_testing; } | jq -R . | jq -sc .)
gh api "repos/$R/issues?state=open&per_page=100" --paginate |
  jq -c --arg loc "$LOC" --arg area "$AREA" --argjson blk "$BLK" '.[] | select(.pull_request|not) |
    {n:.number,
     loc:[.labels[].name|select($loc != "" and startswith($loc))]|length,
     area:[.labels[].name|select($area != "" and startswith($area))]|length,
     blk:[.labels[].name|select(. as $l | $blk | index($l))]|length} |
    select(($loc != "" and .loc != 1) or ($area != "" and .area != 1) or .blk > 1)'
```

Empty output = good. An axis without a prefix (rc 1) is not checked.

## An existing issue

Updating an issue that already exists (title, body, labels, a block) runs only part of the
workflow above. Do not guess which part:

| Step | For an existing issue |
|---|---|
| 0. Does this exist already? | drops: the issue is the one that exists |
| 1. Choose the template | drops: the template stands |
| 2. Format the body | drops as a rewrite; a body change goes through the PATCH below |
| 3a., 3b. Axes | only when the axis label is missing or wrong |
| 3c. The block | holds: at most one blocking label, the lowest rank wins |
| 3d. Milestone | only when the change moves the issue to another theme |
| 3e. Set a block | holds, without its step 1: the issue exists already |
| 4. Create the issue | drops: never `gh issue create` |
| 5. Report the result | the final state as read, not the command that was sent |

**Title and body** go through `gh api`. Read the current body first
(`R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api "repos/$R/issues/<nr>" --jq .body`),
write the *whole* new body with your write tool to a temporary file, and send that file along;
never the body itself as a shell argument, because backticks and `$` are executed then:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api -X PATCH "repos/$R/issues/<nr>" -f title='<title>' -F body=@<file>
```

Leave out `-f title=…` or `-F body=…` for the field that does not change.

**A blocking label that no longer holds.** A blocking label is a state (3c): it comes off as soon
as what it names is done. Read first what stands: the labels, and for `labels.blocked_issue` the
`blocked_by` list from 3e step 3. Then one of two paths:

- **Nothing blocks any more**: label off, nothing on.
  `OLD=$(python3 .lighthouse/lib/lighthouse_lib/config.py labels.<old key>) && gh issue edit <nr> --remove-label "$OLD"`.
  For `labels.blocked_issue` only when the 3e invariant says **lapsed** or **unlinked**: with an
  open blocker the label holds.
- **Another label takes over** (a decision is taken, but now the issue waits on another one):
  choose the new label with the rank from 3c. If it is `labels.blocked_issue`, set the relation
  first with 3e steps 2–3. Then the move in one edit, so the issue never stands for a moment
  without a blocking label (the ready queue) or with two:

  ```bash
  OLD=$(python3 .lighthouse/lib/lighthouse_lib/config.py labels.<old key>) || exit 2
  NEW=$(python3 .lighthouse/lib/lighthouse_lib/config.py labels.<new key>) || exit 2
  gh issue edit <nr> --remove-label "$OLD" --add-label "$NEW"
  ```

Close with 3e step 5 as the final check, also when no relation is involved; on a failure the
failure table of 3e applies. Report what stands, never success from the command alone.

## A defect in an external tool

When the fault lies in someone else's tool (an MCP server, a vendored skill, a library), the
issue goes into this repo, never upstream. Title: `<tool>: <problem>, upstream melden` in the
project language. The body carries the code path or the observed error
with its version or commit, a decision point "do we report this upstream, from which account,
with which proposal", and the upstream text as a quoted draft in the upstream repo's language.
Acceptance criteria: the decision is taken; if yes, the upstream link is a comment here; if no,
the reason and the local workaround are. Post upstream only when the user asks for that post.

## Milestones

**The milestone description *is* the pitch.** There is no separate parent issue and no
`Milestone: ` prefix in issue titles.

- **Naming**: `<project.milestone_prefix> <name>`, for example `LH M0 — Package` or
  `WEB infra: security hygiene of the standing state`. The repo prefix goes in the title, because
  a project board over several repos slices on milestone or on repo, never on both. Read it with
  `MP=$(python3 .lighthouse/lib/lighthouse_lib/config.py project.milestone_prefix)`; rc 1 means no
  prefix: the title gets none, and the report says
  `milestone prefix: none — no [project] milestone_prefix set`. The form after the prefix
  (`M<n> — …` or `<area>: …`) is project content and lives in the file under `[paths] labels`.
- **Size**: the plan of a milestone splits it into at least about five issues. Fewer, and the
  work is one issue, or it belongs in another milestone.
- **On closing**: the title becomes `<title> - DONE` and `state=closed` in the same PATCH.
  See `lighthouse-close-issue`.
- **Description**: the pitch from "Pitch (Shape Up)" below, with its seven headings in that
  order. The ID of a milestone is its number in the URL. If the project has tenets
  (`python3 .lighthouse/lib/lighthouse_lib/config.py paths.tenets`, only if present), the pitch
  names the crux of its main area under *Problem / Opportunity*.
- **Hard rule**: *a milestone without DoD text in its description does not exist.* Without that
  rule blocks grow into eternal bins again.
- The description is at the same time the **gatekeeper**. If a new task does not belong in the
  block on the ground of DoD and no-gos, it goes to the backlog (no milestone) — not into it after
  all.

Creating goes through the REST API; `gh` has no `milestone create`:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
MP=$(python3 .lighthouse/lib/lighthouse_lib/config.py project.milestone_prefix); rc=$?
[ "$rc" -eq 1 ] && echo "milestone prefix: none — no [project] milestone_prefix set"
[ "$rc" -ge 2 ] && exit 2
gh api --method POST "repos/$R/milestones" \
  -f title="${MP:+$MP }<name>" -F description=@<file>
```

Write the pitch with your write tool to a temporary file first; never the pitch itself as a
shell argument: backticks and `$` are executed then.

Sub-issues stay, but only where one task naturally falls apart into sub-steps — not as a layer of
structure above the milestone. Linking goes through the REST API; `sub_issue_id` is the database
`id` of the child, not the number:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
CHILD_DBID=$(gh api "repos/$R/issues/<child-nr>" --jq '.id')
gh api --method POST "repos/$R/issues/<parent-nr>/sub_issues" -F sub_issue_id="$CHILD_DBID"
```

Verify with
`R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api "repos/$R/issues/<parent-nr>/sub_issues" --jq '.[].number'`.

## The board (only if configured)

A GitHub Projects v2 board exists for this skill only when `lighthouse.toml` has a
`[forge.board]` table. If one of the `forge.board.*` keys below gives rc 1, there is no board:
report `board: skipped — no [forge.board] configured` and set nothing. Labels and milestones are
the source of truth about *what* an issue is; the board shows them. The Status is the one place
where the board itself means something; the status names are in `[forge.board.status_options]`.
A `Controller` status (only if present) hands an issue over to the controller.

Call it consistently **"the board"** in documentation and skills, never "project" — otherwise it
clashes with the everyday use of "project".

The board can also have a field **Priority** (`P0` · `P1` · `P2`, no value = at the back).
Setting it goes in the board UI, or like this:

```bash
P=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.project_id) || exit $?
F=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.priority_field_id) || exit $?
S=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.priority_options.<P0|P1|P2>) || exit $?
gh project item-edit --id <item-id> --project-id "$P" --field-id "$F" --single-select-option-id "$S"
```

An item id you read with
`N=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.project_number) && O=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.owner) && gh project item-list "$N" --owner "$O" --format json --jq '.items[] | select(.content.number==<nr>) | .id'`.

`On hold` is the place for work parked on purpose; there is no on-hold label. Setting it goes in
the board UI, or through `gh project item-edit` (requires the `project` scope plus project, item,
field and option id); there is no `gh issue` flag for it.

## Acceptance Criteria vs Definition of Done

| Aspect | Acceptance Criteria (AC) | Definition of Done (DoD) |
|--------|--------------------------|--------------------------|
| **Scope** | Unique per item | Universal for ALL items |
| **Question** | "When is THIS task done?" | "Which quality standards hold for EVERYTHING?" |
| **Examples** | User can log in, error at a wrong password | Code reviewed, tests passing, docs updated |

AC = WHAT this specific task must do. DoD = HOW we secure quality.

## Issue Templates

**IMPORTANT**: every template has fixed sections. Do not mix them. Write the plain headings in
the project language; a `heading:<key>` heading in the variant `headings.py <key>` prints.

### Bug

```markdown
### Description

**What goes wrong?**
...

**Steps to reproduce:**
1. ...

**Screenshots / Logs (optional):**
...
```

### Task

```markdown
### Description

What has to be done?

### heading:acceptance_criteria

Specific conditions THIS task must meet:
- [ ] ...
- [ ] ...

### Definition of Done

Standard quality demands (tick what applies):
- [ ] Code reviewed
- [ ] Tests written and passing
- [ ] Documentation updated
- [ ] No known bugs
```

### Pitch (Shape Up) — for the milestone description

This is **no issue template**. Use it for the description of a new milestone.

```markdown
### Problem / Opportunity

What is the real problem? For whom? Evidence, examples, context.

### Appetite (timebox)

Choose from: Small (1-2 days) | Medium (3-4 days) | Large (5-6 days)

### Solution

High-level approach, maybe a few key elements.

### Risks / Rabbit holes

Which pitfalls must we avoid?

### heading:milestone_dod

When is this block done? Required — without a DoD the milestone does not exist.

### heading:milestone_no_gos

What do we explicitly NOT do in this version?

### heading:milestone_not_yet_decided

Open questions that fall inside the block but are not sharp yet. One bullet per question. None?
Write "None.".
```

**heading:milestone_not_yet_decided** — three points:
- **What belongs in it**, set against the DoD (when done) and the No-Gos (what not): a question
  here does belong to the block, only the HOW is not settled. So do not duplicate a DoD point or
  a no-go.
- **Outflow**: as soon as a question is sharp enough for acceptance criteria it becomes an issue
  of its own in this milestone through `/lighthouse-write-issue`, and the bullet becomes
  `→ #<nr>`. A bullet without `→ #<nr>` is open. That issue may be the current issue, if its spec
  answered the question. Updating: write the *whole* pitch, only that bullet changed, with your
  write tool (not through `echo` or a heredoc) to a temporary file, and send that file along:
  `R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh api -X PATCH "repos/$R/milestones/<n>" -F description=@<file>`.
  Never the pitch itself as a shell argument: backticks and `$` are executed then.
- **Timebox**: the fog gets no timebox of its own. If the answer does not fit in the appetite, it
  becomes a no-go or a next milestone. No label and no issue above the milestone.

**NOTE**: use no acceptance criteria in a pitch — they belong to Tasks.

## Title Standards

- Be specific and descriptive
- Sentence case
- No prefixes like `[FEATURE]` or `[BUG]` — labels are for that. Also no `Milestone: ` prefix.

### Good titles

- `Add real-time streaming transcription during meetings`
- `Fix database connection timeout on resume`

### Bad titles

- `Bug` (vague)
- `Fix thing` (vague)
- `NEW FEATURE` (vague, all caps)

## Important

- Never include "Generated with Claude Code" in issues unless directly relevant
- Link related issues using `#issue-number` syntax
- Provide context — explain the "why", not just the "what"
- Use `@username` for validation and sparring partners in pitches

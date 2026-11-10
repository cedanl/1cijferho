---
name: lighthouse-close-issue
description: Close GitHub issues with proper verification of Acceptance Criteria and Definition of Done. Links commits correctly with full URLs. After the merge of a `Closes` PR — check the evidence afterwards, and reopen if it does not hold (entry C).
metadata:
  lighthouse: "core@v0.8.1"
  binding: default
  origin: own
---

# Close Issue Skill

Closes GitHub issues with verification of acceptance criteria and quality standards.

Two rules hold throughout. **What counts as evidence:** a command you ran with its output, a file
you opened, or the verdict **not measurable from here** — never an assumption. **Verify first that
the commits belong to the issue; never close on unrelated work.**

## When to Use

- After finishing a task (**entry A**)
- At "can't X be closed", "isn't X already done?", "pick up X if it is still relevant" —
  the work was not done in this session (**entry B**, see below)
- `/lighthouse-close-issue 58` - Closes issue #58
- `/lighthouse-close-issue 58 --commit abc123` - Closes with a specific commit reference
- After the merge of a `Closes` PR: the issue is already closed then, and checking the PR
  evidence afterwards is **entry C**
- Prompt `/lighthouse-close-issue <nr> (issue #<nr>, controller-run)`: the controller (if present)
  started this run after the merge of a `Refs` PR. That is entry B in controller mode (**B7**)

## Step 0 — before the merge

First one query, for step 0 and C1: is the issue open, does a PR refer to it with `Closes`,
and what closed it the last time (`closer`, for C1)?

```bash
O=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.owner) || exit 2
NAME=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.name) || exit 2
gh api graphql -f owner="$O" -f name="$NAME" -F nr=<nr> -f query='query($owner:String!,$name:String!,$nr:Int!){repository(owner:$owner,name:$name){issue(number:$nr){state closedByPullRequestsReferences(first:10,includeClosedPrs:true){nodes{number state}} timelineItems(itemTypes:[CLOSED_EVENT],last:1){nodes{... on ClosedEvent{closer{__typename ... on PullRequest{number}}}}}}}}'
```

`gh` runs outside the sandbox (`dangerouslyDisableSandbox: true`).

| Outcome | What now |
|---|---|
| issue `state` `OPEN` and a node with `state` `OPEN` | report literally `waits for merge of #<pr>; run /lighthouse-close-issue <nr> again after the merge` and **stop**: no verification, no `gh issue close`. The merge of that PR closes the issue itself. |
| issue `state` `CLOSED` | **entry C** |
| otherwise (no open `Closes` PR) | entry A or B |

## Three entries

| Entry | When | Where the evidence comes from |
|---|---|---|
| **A — you did the work yourself** | the commits are in this session | your context |
| **B — close check without session context** | an old issue comes by with the question *is this done by now?* | dug up from `git log`, `gh pr list`, the files themselves |
| **C — check after the merge** | the issue is already closed (merge of a `Closes` PR or the user) | the Evidence table of the closing PR, measured again on a fresh `origin/<default branch>` |

Entry A is the `## Workflow` below, unchanged. Entry B is an **extra step up front**: it digs up
the evidence entry A takes from context, and then goes through the same steps 4 to 6. There is no
faster route to `gh issue close` — B makes the verification heavier, not lighter. Entry C never
closes: the issue is already closed, C confirms it or reopens it. Every entry ends at
*Clean up the worktree — the last step*, except B7, which runs it before its close comment; that
says when cleaning up is and is not allowed.

## Entry B — close check without session context

**Triggers.** "can't 650 be closed after recent PRs", "isn't 786 already done?", "603 is solved,
right?", "pick up 569 if it is still relevant", "can't 737 be closed with a recent commit". The
pattern: the question is not *build this*, but *has this been done by other work in the meantime?*

If `/lighthouse-start-issue` reported that an issue may already be done, the close action starts
here.

### B1. Read the issue and take out the anchors

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/issues/<nr>" --jq '{created_at, title, body}'
```

Note three things: the **creation date** (the lower bound of the search window), the
**acceptance criteria** (each criterion becomes one table row later), and the **anchors** — the
paths, file names, symbols, commands and labels the issue names literally. You search on those
anchors. The subject of the issue is not an anchor.

### B2. Dig up the evidence

Three moves, in this order. The first two deliver candidates, the third delivers evidence.

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
# 1. merged PRs since the creation date of the issue
gh pr list --repo "$R" --state merged --limit 100 \
  --json number,title,mergedAt,url \
  --jq '.[] | select(.mergedAt > "<created_at>") | "\(.number)\t\(.mergedAt[:10])\t\(.title)"'

# 2. commits since that date that touch the anchors
git log --since=<YYYY-MM-DD> --oneline -- <path from the issue>
git log --since=<YYYY-MM-DD> --oneline -S'<symbol from the issue>'
git log --since=<YYYY-MM-DD> --oneline --grep '#<nr>'

# 3. the current state of the tree on those anchors
ls <path>; grep -rn '<symbol>' <path>
```

Move 3 is the most important and the most often skipped. A criterion like "file X exists and
contains Y" you prove by opening X, not by finding a PR that was about it. If the criterion holds
a command, run it — the evidence rule above holds in full, including the third outcome
**not measurable from here**.

If you search without anchors, you find the period instead of the work — see B4.

### B3. Fill the table: one row per acceptance criterion

Not one verdict about the issue. One row per criterion, each with its own evidence line:

| # | Acceptance criterion | Verdict | Evidence |
|---|---|---|---|
| 1 | … | `demonstrably done` | `docs/developer/planning/history/README.md:3` — the heading is there |
| 2 | … | `partly` | PR #747 covers the first part, not the second |
| 3 | … | `not` | `grep -rn 'foo' src/` → 0 hits |

Three verdicts, parallel to the three outcomes of the evidence rule:

| Verdict | Meaning |
|---|---|
| `demonstrably done` | there is an evidence line that touches **this** criterion |
| `partly` | part of the criterion is covered, the rest not |
| `not` | no evidence found — **also when it is probably done** |

An evidence line is exactly one of these four, never prose:

- full commit URL (`https://github.com/<forge.repo>/commit/<hash>`)
- PR number (`#747`)
- file path with line number (`docs/workflows.md:63`)
- command output (the command **and** what it returned)

### B4. What does *not* count as evidence

The easiest mistake there is: something from the right period feels like evidence and is not.
The rule *verify first that the commits belong to the issue; never close on unrelated work* is
this demand, written out.

| ❌ Does not count | Why |
|---|---|
| a commit in the same period | time is no causality |
| a PR that touches the same file | a file has more lines than this criterion |
| a title that resembles the subject | a title is no implementation |
| "this was surely taken along in that refactor" | a guess, no evidence line |
| no evidence found | **no evidence = `not`** |

Evidence must touch the **criterion**, not the **period**.

### B5. Three outcomes

Show the table from B3 **to the user first**. No `gh issue close` goes over it before that table
has been seen — also not when everything is `demonstrably done`.

| Verdict over all AC | Outcome |
|---|---|
| all criteria `demonstrably done` | on to step 4 (DoD), 5 and 6 below; the evidence lines from B3 go along in the close comment, with full commit URLs |
| mixed | **propose to shorten the issue** to the open criteria: tick the finished criteria in the body, leave the issue open. Do not close. |
| mostly `not` | `/lighthouse-start-issue <nr>`; the issue stays as it is |

Shortening goes through `/lighthouse-write-issue`, with a comment that carries the table — so the
next session sees what was already done and why.

### B6. Boundaries

| Moment | Skill | Question |
|---|---|---|
| writing | `lighthouse-write-issue` | does this exist already? |
| picking up | `lighthouse-start-issue` | is this done by now? *(report, no close action)* |
| periodic | the backlog-stand skill, if present | which open issues are silently done? *(only top 3 and controller-ready, no bulk scan)* |
| **closing** | **this skill, entry B** | **is this done — per criterion, with evidence?** |

### B7. Controller mode — check after a merge (only if present)

**When.** The prompt is `/lighthouse-close-issue <nr> (issue #<nr>, controller-run)`: the
controller started this run after a `Refs` PR for the issue was merged. Step 0 and B1–B4 hold
unchanged.

**B5 lapses**: there is no user who sees the table. The table from B3 always goes as a comment
on the issue, through `--body-file` from a `mktemp -d` directory. Heading line:
`After-merge check of #<pr>`, without 🤖, because it is no controller heading. Then the table, and
per AC that is not `demonstrably done` a line *what is missing*.

| Verdict over all AC | Outcome |
|---|---|
| all criteria `demonstrably done` | steps 4–6 of entry A. For the DoD the merge counts as review; a DoD line that is not measurable from here goes into the close comment as such and does not block. `gh issue close` knows no file flag (only `-c, --comment string`): first post the close comment with the evidence lines through `gh issue comment <nr> --body-file <file>` (from the `mktemp -d` directory) and then close with `gh issue close <nr> --reason completed`. If the comment fails, do not close. Before that close comment, run *Clean up the worktree*; its result per branch goes along as evidence lines in the close comment. |
| otherwise: `partly`, `not`, or an AC `not measurable from here` | do not close, do not shorten through `lighthouse-write-issue`, do not touch the board (the controller sets `In progress`), no question, no `/lighthouse-write-handoff`. End after the comment. |

**Reports.** Every report that step 0, 4 or 6 addresses to the user (e.g. "DoD not met → report",
"report #m is free", "waits for merge") goes into the after-merge comment in B7: there is nobody
to talk to. A DoD line that demonstrably does *not* hold (other than not measurable) you treat as
the "otherwise" branch: do not close, reason in the comment.

- ✅ close only with an evidence line per AC
- ❌ close on a probably-done

## Entry C — check after the merge

**Triggers.** Step 0 gave `CLOSED`; "was 1091 closed correctly?", "does the evidence of that PR
still hold?". The issue is closed; the question is whether the evidence in the PR also stands on
the default branch.

### C1. Find the closing PR

What counts is what **actually** closed the issue: the `closer` of the last `ClosedEvent` from the
query of step 0. Not the nodes of `closedByPullRequestsReferences`: those keep every merged PR
with a closing keyword, also an earlier one that already led to a reopening, and also after a
manual close.

| `closer` | What now |
|---|---|
| `__typename` `PullRequest`, `number` `<pr>` | `<pr>` is the closing PR: on to C2 |
| everything else (`null`, `Commit`, `ProjectV2`) | report `closed without a closing PR — no after-merge check` and go on to *Close the milestone* |

### C2. Measure the Evidence table again on a fresh default branch

Free command text from the body **never** runs. Only the runner runs rows, and only rows on the
allowlist. It measures in a throwaway worktree on `origin/<default branch>`, not in your checkout,
in a fresh scratch directory outside the repo. One shell call, because `scratch` does not live on
into the next; every step before the runner is chained with `&&`, so a failed `gh pr view`,
`fetch` or `worktree add` does not run the runner:

```bash
B=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.default_branch) &&
scratch=$(mktemp -d) && echo "scratch=$scratch" &&
gh pr view <pr> --json body --jq .body > "$scratch/body.md" &&
git fetch origin "$B" &&
git worktree add --detach "$scratch/after-merge-<nr>" "origin/$B" &&
{ python3 .lighthouse/lib/lighthouse_lib/evidence.py run --issue <nr> --body-file "$scratch/body.md" --cwd "$scratch/after-merge-<nr>" > "$scratch/run.txt" 2> "$scratch/run.err"; rc=$?; echo "rc=$rc"; git worktree remove --force "$scratch/after-merge-<nr>"; }
```

`<scratch>` below is the path the `scratch=` line prints; copy it literally. A fresh `mktemp -d`
per run never collides with an `after-merge-<nr>` an earlier, crashed run left behind.
If there is no `rc=` line in the output, the runner did not run: that is
`COULD NOT CHECK` (see C3), no verdict on the issue.

The exit code is caught in `rc` **before** the `git worktree remove`, otherwise `$?` is lost.
Stdout and stderr go to separate files, so that the start of `run.txt` is purely the runner
output. Without `--before-pr`: here an `after PR` row is measured for the first time. The
block runs outside the sandbox (`dangerouslyDisableSandbox: true`); otherwise every `gh` row is
`heading:evidence_unmeasurable` (exit 4).

**The runner output contains free text from the PR body** (the AC cell, command details with
backticks or `$(…)`). So never paste runner output or body text into a quoted command line: in
`"..."` the shell executes it. Every comment with runner output goes through the file:
`--body-file`, or `"$(cat <scratch>/run.txt)"` (the output of a command substitution is not
evaluated again).

### C3. Act per exit code and output

The exit code alone is not unambiguous: an argparse usage error also gives `2` (empty stdout), an
uncaught Python exception (traceback) also `1`. That is why the combination of `rc` and `run.txt`
counts.

| Exit | Output in `run.txt` | Meaning | What now |
|---|---|---|---|
| `0` | table | all rows `heading:evidence_holds` | `gh issue comment <nr> --body "after-merge check: $(python3 .lighthouse/lib/lighthouse_lib/headings.py evidence_holds)"`, then step 6 (released issues) and *Close the milestone*, after that *Clean up the worktree* |
| `4` | table | `heading:evidence_unmeasurable` from here | comment through `--body-file` (see below); **do not reopen**; report the rows to the user, then step 6 (released issues) and *Close the milestone* |
| `1` | the table head `heading:evidence_table_head` is in it | `heading:evidence_fails` / `heading:evidence_invalid` | reopen, see below |
| `2` | starts with `heading:evidence_closes_without_block` | no evidence block in the PR body | reopen, see below |
| `3` | `COULD NOT CHECK` (body or `--cwd` unusable) | not measurable | see below |
| everything else | `1` without table head (traceback), `2` without `heading:evidence_closes_without_block` (argparse), an unknown exit code | the runner itself is broken | see below |

The table head a real `1` always prints is `heading:evidence_table_head`, in any language variant.

**Exit `4` — the comment file.** First the heading line, then the runner table, together in one
file; the unmeasurable rows do not count as proved at *Close the milestone*:

```bash
{ printf '%s\n\n' 'after-merge check: holds, except for rows that are not measurable from here'; cat <scratch>/run.txt; } > <scratch>/comment.md
gh issue comment <nr> --body-file <scratch>/comment.md
```

**Exit `1` or `2` with recognised output — reopen, fail-closed.** First the issue, then the board:

```bash
gh issue reopen <nr> --comment "$(cat <scratch>/run.txt)"
python3 .lighthouse/scripts/board_start.py --reopen <nr>
```

At `2` the comment (`run.txt`) starts with `heading:evidence_closes_without_block`; at
`heading:evidence_invalid` rows the table says `Evidence outside the allowlist`. Never put a prefix
in the command line yourself: what belongs to it is already in the file.

`board: skipped` (no `[forge.board]`, rc 0) is no error: report the line as it is and go on.

If `board_start.py --reopen` ends with exit 1 or 2 (output `board: ERROR — <cause>`), report
loudly `issue reopened, board not updated` with the hand command, and do not wait for it to come
right by itself:

```bash
gh project item-edit --id <item> --project-id <forge.board.project_id> --field-id <forge.board.status_field_id> --single-select-option-id <In progress>
```

**Exit `3`, an unknown exit code, an unrecognised output or no `rc=` line.** Not measurable is no
passed check and no proved failure. So: no `after-merge check` comment, **no** `gh issue
reopen`, no board mutation, and report loudly to the user `COULD NOT CHECK` with the
full content of `run.txt` and `run.err`.

Entry C closes nothing itself: the issue is already closed, and a reopened issue closes again by
the merge of the next `Closes` PR (or by the user).

## Workflow — entry A (you did the work yourself)

### 1. Fetch the issue and analyse it

Is the issue already closed? Then entry C.

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh issue view <number> --repo "$R"
```

### 2. Identify the issue type

| Type | Verification |
|------|-------------|
| **Bug** | Is the problem solved? Reproducible? |
| **Task** | Check `heading:acceptance_criteria` + `heading:milestone_dod` |

A pitch is no longer an issue but a milestone description — see "Close the milestone".

### 3. Verify the acceptance criteria (for tasks)

Go through every AC item:
- [ ] Is this criterion demonstrably met?
- [ ] Is there evidence (code, test, output)?

**If an AC is not met**: do NOT close, report what is missing.

### 4. Verify the Definition of Done (for tasks)

Standard DoD items:
- [ ] Code reviewed by a team member (or self-review documented)
- [ ] Tests written and passing (if applicable)
- [ ] Documentation updated (README, AGENTS.md, etc.)
- [ ] No known bugs

**If the DoD is not met**: do NOT close, report what is missing.

### 5. Close with a correct commit link

**IMPORTANT**: Use the FULL GitHub URL for commits, not only the hash!

```bash
# Repo from lighthouse.toml, never from the git remote of the cwd
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2

# Close with the full commit URL
# printf sets the URL; the rest in a quoted heredoc, so backticks and $ in the AC text do not run
gh issue close <number> --repo "$R" --comment "$(printf 'Completed in commit: https://github.com/%s/commit/%s\n\n' "$R" "<full-commit-hash>"; cat <<'EOF'
## Verification

### <heading:acceptance_criteria>
- [x] Criterion 1 - Done in [file](link)
- [x] Criterion 2 - Tested with ...

### <heading:milestone_dod>
- [x] Code reviewed
- [x] Tests passing
- [x] Documentation updated

## Summary
Short description of what was done.
EOF
)"
```

Write the comment in the project language (`config.py project.language`); the headings come from
`.lighthouse/headings.toml`.

### 6. Released issues

Unblocking is done by the unblock lane (`.github/workflows/lighthouse-unblock-dependents.yml`), as
the bot from `[ci]` (`python3 .lighthouse/lib/lighthouse_lib/config.py ci.bot_login`), at every
close as `completed`: every open issue that waited on this issue and has no other open blocker
any more loses the label `labels.blocked_issue` (`python3 .lighthouse/lib/lighthouse_lib/config.py labels.blocked_issue`)
and gets a comment. The closed issue gets one comment back (`Released lane after this close:`)
with what was released and what still waits. A session does not unblock itself: the bot token
exists only in Actions. You check afterwards, also in entry C (after `heading:evidence_holds` or
`heading:evidence_unmeasurable`) and in B7.

`init` installs the lane only when the `ci.bot_*` keys are set. If `ci.bot_login` gives rc 1 or
the workflow file is missing (`test -e .github/workflows/lighthouse-unblock-dependents.yml`),
report `unblock lane: skipped — no bot in [ci]`, and check the waiting issues by hand with the
fallback below.

Which open issues waited on this issue?

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/issues/<nr>/dependencies/blocking" \
  --jq '.[] | select(.state=="open") | .number'
```

No hits: done. Otherwise only what belongs to **this** close counts: an earlier close
(reopened and closed again) has a run of its own and a reply comment of its own. First the close
moment, then the newest run of the lane for this issue (the run name carries `#<nr>`) that started
after it, the reply comments after that moment, and per hit `<m>` the labels:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh issue view <nr> --repo "$R" --json closedAt -q .closedAt
gh run list --workflow lighthouse-unblock-dependents.yml --limit 20 --json databaseId,conclusion,displayTitle,createdAt \
  --jq '[.[] | select((.displayTitle | endswith(" #<nr>")) and .createdAt >= "<closedAt>")] | first'
gh api "repos/$R/issues/<nr>/comments" \
  --jq '.[] | select(.created_at >= "<closedAt>" and (.body | startswith("Released lane after this close:"))) | .body'
gh issue view <m> --repo "$R" --json labels -q '[.labels[].name]|join(",")'
```

A run or comment from before `<closedAt>` belongs to an earlier close and does not count.

| What you see | What now |
|---|---|
| run `success` and the reply comment is on `<nr>` | report per `<m>` literally what that comment says (free or still waits on …) |
| run `success`, no reply comment | no `<m>` carried `labels.blocked_issue`: nothing released, nothing to do |
| run `skipped` | closed as `not_planned` or `duplicate`: blockers stay, a human decides |
| `<m>` has `0` open blockers but still carries `labels.blocked_issue` | two blockers closed at the same time and each run still saw the other open: restart, see below |
| run still busy | wait until it is done; report nothing as free before the comment is there |
| run `failure`, or no run after the close | report loudly `unblock lane did not run for #<nr>` with the restart command below; do **not** run it yourself, the user decides |

```bash
gh workflow run lighthouse-unblock-dependents.yml -f issue=<nr>
```

If the lane does not work (the bot without Issues: write, workflow disabled, or skipped above),
the fallback is by hand, per `<m>` with `0` open blockers, also by the user:

```bash
gh issue edit <m> --remove-label <labels.blocked_issue>
```

If a call fails, report "released issues not read" with that command, never "none".

## Clean up the worktree — the last step

After the close the worktree and the local branch of the issue otherwise stay. The verdict comes
from `.lighthouse/scripts/worktree_audit.py`, the same ladder as `/lighthouse-audit-worktrees`: no
second set of rules here.

**When.**

| Entry | Clean up? |
|---|---|
| C, exit `0` in C3 | yes, the main moment: the PR is merged and C measures in a throwaway worktree |
| A or B (not B7), after the close | yes |
| B7, all criteria `demonstrably done` | yes, before the close comment |
| C with reopening, `COULD NOT CHECK` or `closed without a closing PR`; A, B or B7 without a close | no |

**Branches.** All branches of the issue: the head branch of every merged PR that refers to it
(`Closes` and `Refs`), plus every local branch `issue-<nr>-*`. The script finds them itself:

```bash
python3 .lighthouse/scripts/worktree_audit.py --issue <nr>; echo "exit=$?"
```

The script runs outside the sandbox (`dangerouslyDisableSandbox: true`): it calls `gh` for the PR
status. Run it without `cd`, from the directory where the session started: the cwd test measures
the cwd of the script, and Codex and OpenCode write no session file that sets `live`. A branch
without a merged PR gets `cleanup: no — no merged PR`.

**Verdict.** Only exit `0` plus, in a branch's block, a line `cleanup: yes` can be cleaned up.
Every other outcome is `no`: exit ≠ 0 (exit `2`: the PR lookup failed, clean up nothing at all), a
block without a `cleanup:` line, a traceback, an empty output. The absence of a signal is no yes.

| Block | What now |
|---|---|
| `cleanup: yes` | run the lines below it (a block runs from its `branch:` line to the next blank line) yourself, in that order, each as `git -C <main checkout> …`. No question: the verdict is the safety check |
| `cleanup: no — <reason>` | report the line literally with its branch; clean up nothing for that branch |
| everything else | report `cleanup: no — audit gave no verdict` with the raw output; clean up nothing |

`<main checkout>` is the first path from `git worktree list --porcelain`. If the harness refuses
`-C` (a worktree-isolated Claude Code session), run the same lines without `-C`
from your own cwd: it is the same repo, and `--issue` already checked that that cwd is not in the
worktree. The worktree that holds your own cwd is never cleaned up; its block says
`own cwd is in …`.

**Evidence.** After cleaning up, once `git -C <main checkout> worktree list` and per cleaned-up
branch `git -C <main checkout> branch --list <branch>`. The branch is in neither:

```bash
git -C <main checkout> worktree list
git -C <main checkout> branch --list <branch>
```

**Refusal.** If git refuses a line (for example `contains modified or untracked files`), report
the error line, skip the rest of that branch's lines, and go on with the next branch.

**Report.** One line per branch: `<branch>: cleanup: …`, and after `yes` the suffix `— removed` or
`— git refused: <error line>`. Headless (B7, controller run, if present) the same lines go as
evidence lines into the close comment. There is no question, in any mode.

- ✅ only the lines `--issue` delivers, without a question
- ❌ `--force`, `git worktree unlock` or `git push origin --delete` — also not when git refuses

## Commit Reference Format

| Method | Example | Result |
|---------|-----------|-----------|
| ❌ Hash only | `26948cd` | No clickable link |
| ❌ Short ref | `Completed in commit 26948cd` | No clickable link |
| ✅ Full URL | `https://github.com/<forge.repo>/commit/26948cd...` | Clickable link |

## Template: Close Comment

`<forge.repo>` is the output of `python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo`.

```markdown
Completed in commit: https://github.com/<forge.repo>/commit/<hash>

## Verification

### <heading:acceptance_criteria>
- [x] AC item 1
- [x] AC item 2

### <heading:milestone_dod>
- [x] Code reviewed
- [x] Tests passing (or N/A)
- [x] Documentation updated
- [x] No known bugs

## Summary
What was done and why.
```

## Example: closing issue #58 correctly

```bash
# Get the full commit hash
COMMIT=$(git rev-parse HEAD)
# Output: 26948cd1234567890abcdef...

# Close with the full URL
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh issue close 58 --repo "$R" --comment "$(printf 'Completed in commit: https://github.com/%s/commit/%s\n\n' "$R" "$COMMIT"; cat <<'EOF'
## Verification

### Acceptance criteria
- [x] Claude Code plugins installed (claude-md-management, pyright-lsp)
- [x] Community skills explored and relevant ones installed
- [x] Marketplaces added

### Definition of Done
- [x] Documentation updated (CLAUDE.md, dotfiles README)
- [x] No known bugs
- [x] Settings configured correctly

## Summary
Installed plugins and skills for an improved Claude Code workflow.
EOF
)"
```

## Close the milestone

If you close the **last open issue** of a milestone, you close the block in the same move. A block
that stays at 100% without being closed is exactly the eternal bin the taxonomy clears up.

> **Order:** do steps 1 and 2 below **before** you close the issue in section 5. After the
> close the counter shows `open=0` instead of `open=1` and you miss the moment. Step 3 (the
> milestone `PATCH … state=closed`) runs only **after** section 5 — the DoD evidence from step 2
> belongs in that same close comment of section 5, not in a separate message.

### Step 1. See whether the milestone becomes empty

Does the issue have no milestone? Skip this whole section ("Close the milestone").

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh issue view <number> --repo "$R" --json milestone -q '.milestone.number'
gh api "repos/$R/milestones/<nr>" -q '"open=\(.open_issues)"'
```

`open=1` means: this issue is the last one. Did you already close the issue? Then it says `open=0` — check with `R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh issue view <number> --repo "$R" --json milestone -q '.milestone.number'` whether the issue belonged to this block and just go on with step 2.

### Step 2. Go through the DoD of the milestone

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/milestones/<nr>" -q '.description'
```

Go through every point under `### heading:milestone_dod` and look for evidence (commit, test,
output, runbook). **Not every point demonstrable → do NOT close the milestone.** Report what is
missing; the issue may be closed.

### Step 3. Close the milestone and rename the title to ` - DONE`

Closing is **two** changes in one PATCH: `state=closed` and the title with ` - DONE` after it.
The board shows a closed milestone no differently from an open one — without that suffix the
block stays on the kanban as if it is still running.

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
# fetch the current title
gh api "repos/$R/milestones/<nr>" -q '.title'
# close + rename (copy the title exactly, then " - DONE" after it)
gh api --method PATCH "repos/$R/milestones/<nr>" \
  -f state=closed -f title='<title> - DONE'
```

If the title already ends on ` - DONE`, leave it (no second suffix).

Check afterwards:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/milestones/<nr>" -q '"\(.state) — \(.title)"'
```

Name explicitly in the close comment of the last issue: `Milestone <title> closed — DoD:` and
per point one line of evidence.

### Step 4. Tag and publish the release (only if present)

The release flow stays outside lighthouse. It is present when the repo has `scripts/release.py`
(`test -e scripts/release.py`); otherwise report `release: skipped — no scripts/release.py` and go
on with step 5.

A milestone is a release (`GOVERNANCE.md`). There is no version number to make up here — `--draft`
derives it from the close date of the milestone plus the existing tags, and `--cut` reads it back
from `CHANGELOG.md` afterwards.

**Does the milestone have zero closed issues?** Skip this step. A release with empty notes is
exactly the artefact `release.py` refuses; the script says so itself too and stops with exit 0.

In the worktree, right after closing the milestone:

```bash
uv run python scripts/release.py --draft --milestone <nr>
```

That generates the CHANGELOG section from the DoD and the closed issues, sets `publiccode.yml` in
the same commit to that version plus the close date of the milestone, commits everything on a
`release-<version>` branch that branches from `origin/<default branch>`, and opens a PR. Reading
is allowed, not required: the chore-merge lane merges the PR as soon as `ci` and `cla` are green.
The issue numbers are checked by machine, the prose not. Warnings about DoD points that do not
come back are in the output of the command.

You need not be on the default branch for it: the command fetches itself and branches from
`origin/<default branch>`, precisely because the repo squash-merges and the just merged issue
branch is therefore no ancestor of the default branch. That happens in a temporary worktree of
its own, which disappears after the PR: your own checkout stays on its branch.

Then the lane does the rest (the release flow, if present). It runs every hour, merges the release
PR (only `CHANGELOG.md` and `publiccode.yml` may be in it) and runs `release.py --cut` on the merge
commit: tag, push and GitHub Release. Whoever does not want to wait starts it by hand:

```bash
gh workflow run chore-merge.yml
```

Evidence that it worked:

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh run list --workflow chore-merge.yml --limit 1
git ls-remote --tags origin | grep <version>
gh release view <version> --repo "$R" --json tagName,publishedAt
```

#### Fallback by hand (release flow, only if present)

This holds when the lane is red with a `--cut after merge of #…` line in the summary, or when the
release PR touches more than `CHANGELOG.md` + `publiccode.yml` (then the lane skips it). A bare
`--cut` works only while there is no tag yet: it refuses an existing tag, and sets the tag before
the push and the publication. So first determine the state:

```bash
git ls-remote --tags origin <version>
```

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh release view <version> --repo "$R"
```

- **No tag on the remote** (the cut refused, or the tag push failed): `--cut` by hand, after
  the PR merge, on a fresh default branch. If the tag is still local, delete it first:

  ```bash
  git tag -d <version>
  ```

  ```bash
  git checkout <default branch> && git pull
  ```

  ```bash
  uv run python scripts/release.py --cut
  ```

  That tags `HEAD`, pushes the tag and publishes the GitHub Release. If another PR was merged
  after the release PR (the lane then reported `origin/<default branch> is at X`), `HEAD` is no
  longer the merge commit of the release PR, but a later commit. The tag then lands on that
  later commit. It writes nothing more: `publiccode.yml` is already updated on the release PR.
  It refuses before every mutation when there is no `## v…` heading, the section is empty, the
  tag already exists, the default branch does not run level with its origin, or `publiccode.yml`
  names another version (then the release PR is not merged). So really stand on a fresh default
  branch.
- **Tag on the remote, no GitHub Release** (the publication failed): publish only the
  Release at the existing tag. `--cut` would refuse here. Run it on a fresh default branch.

  ```bash
  uv run python scripts/release.py --tag <version>
  ```

  This has two side effects. It rewrites `publiccode.yml` in your checkout, with the
  date of the tag commit. And the Release text says it was made afterwards, from an
  existing tag (`backfilled`). So check the checkout afterwards, and put the file
  back if it changed:

  ```bash
  git status --short
  ```

  ```bash
  git checkout -- publiccode.yml
  ```
- **Tag and Release both exist**: nothing to do, the lane run was only red after the
  publication.

### Step 5. Update the near goal of the area

Only with an areas folder: `python3 .lighthouse/lib/lighthouse_lib/config.py paths.areas` (rc 1:
report `areas: skipped — no paths.areas` and stop here).

If you closed a milestone in this repo, update the *near goal* in `<paths.areas>/<area>.md`, with
`<area>` the part before `:` after `project.milestone_prefix` in the milestone title: the
closed milestone out, new open milestones of that area that do not end on `ON-HOLD` or `DONE`
in. This does not belong in the release PR (if
present): the release flow branches from `origin/<default branch>` and stages only `CHANGELOG.md`
and `publiccode.yml`. Take a worktree and branch of your own from `origin/<default branch>`:

```bash
B=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.default_branch) || exit 2
git fetch origin "$B"
git worktree add .claude/worktrees/areas-<area>-near-goal -b areas-<area>-near-goal "origin/$B"
```

Commit as `docs(areas): near goal <area> after <milestone title>`, in a PR of its own. This also
holds when step 4 was skipped (zero closed issues). You do not update the crux automatically.

## When NOT to close

- An open `Closes` PR refers to the issue → step 0: wait for the merge
- AC items not ticked → Ask for clarification or finish the work
- DoD not met → Complete the missing steps first
- Unclear whether it is done → entry B: dig up the evidence, table per criterion
- A criterion without an identifiable evidence line → verdict `not`, so do not close
- Mixed verdicts → propose to shorten the issue, do not close
- The user has not seen the verdict table yet → show it first

## Related Skills

- `/lighthouse-write-issue` - Create issues with AC and DoD (this repo)

---
name: lighthouse-write-handoff
description: Write the start command for the next session. Use at the end of a phase, or when the user asks for "a command for a new session", "how do I start this in a new window", "give me the command", "hand-off", or /lighthouse-write-handoff. With an absolute path ending in `.handoff.md` (`/lighthouse-write-handoff <path>`) it writes the block to that file as a pause, as the usage gate asks. Produces one copy-paste-ready block with worktree, branch, goal, relevant background information (or a reference to it) and the first step with the slash command that session starts with.
metadata:
  lighthouse: "core@v0.8.1"
---

# Write Handoff

One block of text the user pastes into a fresh session, after which that session can go on
without further explanation. No summary of what happened — that is in git and in the issue.
Instead: where the work stands and what the next step is.

Follows the rule **Fresh Context Per Phase**: one finished unit of work = one window. The
hand-off is the bridge between two windows.

## When to Use

- The user asks for it ("command for a new session", "how do I pick this up", "hand-off").
- A phase is done (plan written, PR open, spec ready) and the follow-up belongs in a clean context.
- You are running into the context limit and the work is at a natural seam.

Do not use in the middle of a task that can be finished in this window. The exception is the
pause form below: a usage limit stops the session whatever the task.

## Pause form: /lighthouse-write-handoff <path>

The argument is an absolute path that ends in `.handoff.md`; the usage gate passes one when
the usage window is full. This is a pause, not a phase transition. Without a path the skill
works as described below.

1. Build the block as in *Output format*. Add what is uncommitted (from `git status --short`),
   the resume moment (when the session may continue: the reset time the guard names) and, if
   you set a wake-up timer, its job ID.
2. Write the block to the path with Bash: `mkdir -p` the directory, write a temporary file in
   that same directory, then `mv` it onto the path, so a reader never sees half a file. The path
   lies outside the repo, so the write can fall outside the sandbox; ask for the permission
   that needs rather than choosing another path.
3. Show the same block also in the chat.

The pause form skips every side effect of a phase transition, also when
`LIGHTHOUSE_CONTROLLER_RUN` is set: `post_artefact.py` is skipped (so no push), the
*Controller mode* section is skipped (no `🤖` comment, no board status), there is no question
to the user, and the `/clear` suggestion is skipped. Then stop.

## Workflow

1. **Decide the place.** In a worktree? Then the absolute path + the branch:
   ```bash
   git rev-parse --show-toplevel && git branch --show-current
   ```
   On the default branch without a worktree: say that the next session sets up a worktree
   *itself* (rule: worktree before the first write).
2. **Decide the state.** What is committed, what is still open?
   ```bash
   git status --short && git log --oneline -3
   ```
   Name uncommitted work explicitly — the next session must not trip over it.
3. **Decide the anchor.** Exactly one of: issue number, plan/spec path, or PR number.
   If there is a plan in the plans folder (`python3 .lighthouse/lib/lighthouse_lib/config.py paths.plans`),
   use that path; otherwise the spec in the specs folder
   (`python3 .lighthouse/lib/lighthouse_lib/config.py paths.specs`); otherwise the issue.

   **If the anchor is a spec or plan path and the issue number is known** (from the prompt or the
   branch `issue-<nr>-…`), post it on the issue,
   also in controller mode (if present) before the `🤖 heading:kind_handoff`,
   with `dangerouslyDisableSandbox: true` (Go TLS of `gh`):
   ```bash
   cd <worktree> && python3 .lighthouse/scripts/post_artefact.py <nr> <absolute path>
   ```
   The script is idempotent: without a content change it posts nothing. If GitHub does not know
   the commit yet, it pushes the branch itself (only without an open PR, never the default
   branch). Put the `artefact:` line in the block under `State:`.
   An error does not block the handoff: the error line goes under `Watch out:`.
4. **Decide the first step, and it always carries a command.** One concrete
   action, not an agenda of five points — but *with* the slash command
   the fresh session starts with. A fresh session does not know the habits of this window:
   without a command it starts without a skill and does the work freehand.
   Route on what there is:

   | What there is | First step | Controller (if present) |
   |---|---|---|
   | a plan in the plans folder, not yet adversarially reviewed | `/lighthouse-review-plan` on that plan | `choice` |
   | a plan, review converged, open points accepted, or skipped on a decision of the user | `/superpowers:subagent-driven-development` on that plan; at *Yes, native* (controller mode step 2, only a plan that meets the native criterion) `/superpowers:executing-plans` with `lane: native`; at the answer *interactive* (controller mode step 2, *Exception*) `/superpowers:executing-plans` | `offer` |
   | a spec, not yet adversarially reviewed | `/lighthouse-review-spec` on that spec | `default` |
   | a spec, review converged or round limit with open points recorded, no plan yet | `/superpowers:writing-plans` on that spec | `no` |
   | an issue, no spec yet, several designs possible | `/superpowers:brainstorming` | `no` |
   | an issue that already settles the HOW | `/lighthouse-start-issue <nr>` | `no` |
   | a bug or failing test | `/superpowers:systematic-debugging` | `no` |
   | work done, issue can be closed | `/lighthouse-close-issue <nr>` | `no` |
   | PR open, review needed | `/code-review` | `no` |
   | PR red | `/lighthouse-fix-pr <pr>` | `offer` |
   | context full, same phase | the same command as this phase; if the previous handoff carries `lane: native`, the new one carries it too | as that phase, but `default` becomes `no` (during a running plan review: `offer`) |

   The Controller column says what controller mode step 2 does (only if present; without a
   controller every row is `no`): `no` is only the plain
   block, without a question; `choice` is the choice in chat text (step 2, *Exception*);
   `offer` is the `AskUserQuestion` from step 2; `default` is posting and Status `Controller` without a question (step 2, *`default`*). A controller run does not look at the
   column: it always posts. At `choice` you print two blocks in step 6 before the question: first *review*, then *interactive*.

   If no row fits, still choose explicitly: name the command you would run
   yourself. "Carry on where you left off" is not a first step.
5. **Name the pitfalls** this session learned and the next session
   would hit again: a hard gate, a service that is down, a test that
   only measures on one machine. At most three lines, only if they really exist.
6. **Print the block** in the format below, followed by the suggestion to run
   `/clear`.
7. **Read your own block back** before you hand it over. Is there a slash command on the
   `First step:` line? If not: back to step 4. This is the mistake this skill makes
   most often.

## Controller mode (only if present)

The controller stays outside lighthouse. It is present when `lighthouse.toml` has a
`[controller]` table (`grep -q '^\[controller\]' lighthouse.toml`); otherwise this whole section
is skipped: only the plain block, and report `controller: skipped — no [controller] in lighthouse.toml`
if the user asked for it.

The block also goes as a comment `🤖 heading:kind_handoff` on the issue, so that the controller's
tick starts the next phase itself. Two entries:

- **Controller run** (the prompt ended on `(issue #<nr>, controller-run)`, or `LIGHTHOUSE_CONTROLLER_RUN`
  is set in the environment: after a resume the prompt does not carry the suffix): always post,
  except when this run posted a question with `lighthouse ask` and no answer resumed it yet. Then
  post nothing, no block either, and stop without any further tool call: a handoff after the
  question becomes the last turn on the issue, and the answer then resumes nothing.
- **Interactive, after a phase**: offer it (see the Controller column in Workflow step 4). The user does not put the issue on the board and does not paste fields;
  this skill does that after one question.

1. **Issue number.** From the prompt, otherwise from the branch (`issue-<nr>-…`). If neither
   works: no comment, only the plain block.
2. **Offer** (only interactive, and only if the Controller column in Workflow step 4
   does not say `no`). At `no` you do not read the board status; if the issue is on `Controller` anyway, it stays there until the user moves it. Read the board status and
   the item ID (`gh` with `dangerouslyDisableSandbox: true`). Gives one of the `forge.board.*` keys
   rc 1, there is no board: report `board: skipped — no [forge.board] configured` and post without
   a status change. At rc 2, stop.
   ```bash
   set -o pipefail
   O=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.owner) || exit 2
   NAME=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.name) || exit 2
   N=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.project_number) || exit $?
   gh api graphql -f owner="$O" -f name="$NAME" -F nr=<nr> -f query='query($owner: String!, $name: String!, $nr: Int!) {
     repository(owner: $owner, name: $name) { issue(number: $nr) {
       projectItems(first: 20) { nodes { id project { number }
         status: fieldValueByName(name: "Status") {
           ... on ProjectV2ItemFieldSingleSelectValue { name } } } } } } }' \
     | jq -r --argjson n "$N" '.data.repository.issue.projectItems.nodes[] | select(.project.number == $n) | "\(.id) \(.status.name)"'
   ```
   Not `gh project item-list`: that fetches the whole board with all fields and costs hundreds
   of GraphQL points of the shared hourly budget.

   **`default`: a spec, just committed.** At `default` you ask no question: read the
   board status and the item ID (query above), post with
   `next: /lighthouse-review-spec <absolute spec path>` (step 5) and set the Status to
   `Controller` (step 6). Report in one line: the worktree now belongs to the controller, an edit there
   breaks the paired round 1 (`blind` refuses a work tree that is not clean). This holds only
   in the window that just committed the spec (the brainstorm); another window that hands over an unreviewed
   spec (pause, context full) falls under `no`. If the user gave an opt-out in this conversation before the
   commit ("interactive", "review myself"), it is `no`: only the plain block, no post,
   no Status change. It also holds only on the always-on host (`host.toml`)
   (`python3 .lighthouse/lib/lighthouse_lib/config.py host-check`, exit 0). Elsewhere: the plain block plus one
   line why: the worktree does not exist there for the controller, and the controller would
   return the issue. Back after the commit: the user moves the Status off `Controller`. That
   only stops the next phase; a running run finishes its phase
   (the controller stops no active unit).

   **Exception: a plan, not reviewed.** Here you ask no `AskUserQuestion`. You write
   in plain chat text, in this order, and end the turn after it:

   1. The block for **review** (format of step 6), with
      `First step: /lighthouse-review-plan <plan>`.
   2. The block for **interactive**, with `First step: /superpowers:executing-plans <plan>`.
   3. Plan link, advice and question. The plan link is clickable: `[<file name>](<absolute path>)`,
      not a bare path. Form:
      `Plan is in [<file name>](<absolute path>). My advice: <option>, because <one criterion>.`
      `Review, controller or interactive?`

   If the user chooses *review* or *interactive*, they paste the block and need not come back.
   Only *controller* still asks for an answer in this window. That also holds for the fourth
   answer, *controller (native)*; the question above does not name it, it is in the
   advice line and in the bullet below.

   Criteria for the advice, name one: **review** for several tasks with interfaces between
   them, for code or services, or for an irreversible step. **Skip** for one task that
   touches only markdown and whose tests pin the text. Then **controller** if no
   decision of the user is to be expected during the execution, otherwise **interactive**. When in doubt: **review**.
   **Controller (native)** you advise only for at most four tasks with few interfaces
   between the tasks, and if the execution asks no decision of the user; you name it in
   the advice line (`My advice: controller (native), because …`).

   - Answer *review*: the block is already there. No comment. If the user wants the plan review itself through the controller: `next: /lighthouse-review-plan <plan>` and steps 5 and 6.
   - Answer *controller*: `next: /superpowers:subagent-driven-development <plan>`, then
     step 5 (post) and step 6 (Status `Controller`). This also holds when the issue is already on `Controller`.
   - Answer *interactive*: the block is already there; the agent executes itself, in a fresh
     window. No comment, no status change.
   - Answer *controller (native)*: `next: /superpowers:executing-plans <plan>` plus the
     field `lane: native`, then step 5 (post) and step 6 (Status `Controller`). The controller
     starts it on Sonnet; only after this answer, never on your own initiative.

   A controller run does not ask this: there `next: /lighthouse-review-plan
   <plan>` stays, because skipping needs a decision of the user. Without an issue number (step 1) there is nothing to post: then ask the question only with *review* or *interactive*, without *controller*.

   Only at `offer` (never at `default` above): if it is already on `Controller`, post at once (step 5), except for the route *interactive*
   (above). Otherwise ask with
   `AskUserQuestion`: "Shall I add the handoff to #<nr> and put the issue on
   Controller?", with the options **Yes, now** (`when: now`), **Yes, at night** (`when: night`) and
   **No, I paste it myself** (only the plain block). If the reviewed plan meets the native criterion
   (at most four tasks, few interfaces between the tasks), a fourth option is added:
   **Yes, native** (`next: /superpowers:executing-plans <plan>` plus `lane: native`,
   `when: now`, then steps 5 and 6). The three other options stay. At yes: first post (step 5), then set the
   status (step 6). The other way round, a tick in between can see an issue on `Controller` without a handoff
   and start `/lighthouse-start-issue` itself.
3. **`next:` is exactly one command plus one argument.** Anything else the
   controller returns.

   | What there is | `next:` |
   |---|---|
   | a plan, not reviewed | `/lighthouse-review-plan <absolute plan path>` |
   | a plan, review converged or skipped on a decision of the user | `/superpowers:subagent-driven-development <absolute plan path>` |
   | a spec, not reviewed | `/lighthouse-review-spec <absolute spec path>` |
   | a spec, review converged | `/superpowers:writing-plans <absolute spec path>` |
   | PR open | `/lighthouse-close-issue <nr>` — final handoff: the controller never starts this, it waits for green |
   | PR red | `/lighthouse-fix-pr <pr>` |
   | issue without spec, design open | `/superpowers:brainstorming` — the controller returns the issue |
   | plan the user has to guide themselves (only controller run) | `/superpowers:executing-plans <absolute plan path>` without `lane:` — the controller never starts this: it returns it, the status stays `Controller` |
   | a plan, native on a decision of the user | `/superpowers:executing-plans <absolute plan path>`, with field `lane: native` — the controller starts it on Sonnet |
   | context full, same phase | the same command as this phase, and `lane: native` if the previous handoff carried it |

   A path is absolute and without spaces. At `/lighthouse-start-issue <nr>`, `worktree:` is the
   main checkout and `branch:` `main` (the default branch, `config.py forge.default_branch`). Exception: the blocked restart from
   `lighthouse-start-issue` (controller run, the gate-0 line carries `— blocker #<x> open`) —
   then `worktree:` and `branch:` are the own worktree and branch of the issue. From `main`
   the controller puts an executing handoff on hold and never stacks.
4. **`when:`** is `now`, unless the user chooses or asks for `night`.
5. **Post** the block:
   ```bash
   R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
   H=$(python3 .lighthouse/lib/lighthouse_lib/headings.py kind_handoff) || exit 2
   { printf '🤖 %s\n' "$H"; cat <<'EOF'
   issue: #<nr>
   worktree: <absolute path>
   branch: <branch>
   anchor: <anchor>
   next: <command> <argument>
   when: now

   ```
   <the block from Output format>
   ```
   EOF
   } | gh issue comment <nr> --repo "$R" --body-file -
   ```
   Between the heading and the last field line there is no blank line: the parser stops at the first.
   For native there is an extra line `lane: native` between `next:` and `when:` (without
   a blank line); otherwise leave it out. The field is valid only next to
   `next: /superpowers:executing-plans <absolute plan path>`; elsewhere the controller returns it.
   The field names are fixed tokens (`lighthouse_lib.protocol`); the prose of the block follows
   `[project] language`.
6. **Status on `Controller`** (only interactive, also at `default`, after the comment; only with a
   board, otherwise skipped). Without an item ID from step 2
   first
   `N=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.project_number) && O=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.owner) && gh project item-add "$N" --owner "$O" --url <issue-url> --format json --jq .id`.
   ```bash
   P=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.project_id) || exit 2
   F=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.status_field_id) || exit 2
   S=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.board.status_options.Controller) || exit 2
   gh project item-edit --id <item-id> --project-id "$P" --field-id "$F" --single-select-option-id "$S"
   ```
   Then report in one line that the controller picks it up within ten minutes; the
   `/clear` suggestion lapses then.
7. **In a controller run you stop after the comment.** No `/clear` suggestion, no further
   tool call: the tick sees the new handoff and starts the next phase.

## Output format

Always one fenced block, so it can be copied in one click:

```
Work in worktree <absolute path>/.claude/worktrees/<name>
(branch <branch>). Goal: <one sentence>.

Anchor: <issue #nr | path to plan/spec | PR #nr>
Context: <decisions and findings that are not in the anchor, or a reference to them>
Auto: <category> — <question in one sentence> → <chosen option>   ← only in a controller run (if present)
State: <what is committed; explicitly "nothing uncommitted" or what is open>
artefact: <the line from post_artefact.py>   ← only with a spec or plan anchor
First step: <slash command> <on what>, <what next>
Watch out: <pitfall>   ← only if there is one
```

The `First step:` line starts with a slash command. If there is no `/` in it, the
block is not done — see the rules below.

Below it, outside the block, one line: `Run /clear first, then paste the block.`

## Rules

- **Absolute paths.** A link or path to a file that exists only on the worktree branch
  must be the full `<repo>/.claude/worktrees/<name>/...` path —
  the fresh session starts in the main checkout and a relative path points at nothing there.
- **No history.** Do not retell what happened; the next session reads
  the issue and `git log`. Hand-off = forward, not back.
- **Context is what the anchor does not carry.** Decisions of the user, findings and
  trade-offs from this session that the next session needs, but that are not (yet) in the
  issue, the spec or the plan. Short, or a reference (issue comment, path). If everything is
  already in the anchor, write `Context: in the anchor.` It is not a report: what
  happened does not belong in this line, what the next session must know does.
- **Trace of automatic choices.** Only in a controller run (if present): under `Context:` one
  line per choice the run made itself (`[controller]` `auto_aanbevolen` in `lighthouse.toml`), in the form
  `Auto: <category> — <question in one sentence> → <chosen option>`. No choices = no lines.
- **One goal per hand-off.** Two goals = two blocks = two sessions.
- **No assumptions about the branch.** If the branch is behind `origin/<default branch>`, say
  so with the rebase command; do not pretend it will be fine.
- **Uncommitted work is a blocker.** Report it, or commit it first — a fresh
  session that finds a dirty tree starts guessing.
- **Every hand-off carries a command.** The `First step:` line contains a slash command
  (usually a `superpowers:` skill, see the table in step 4). A hand-off without a
  command produces a session that starts the work without a skill — exactly what the
  hand-off was meant to prevent. No exceptions: if you do not know the command, the
  first step is not decided yet.

## Example

Input: `/lighthouse-write-handoff`

Output:

```
Work in worktree /srv/src/widgets/.claude/worktrees/issue-537-feed-curation
(branch issue-537-feed-curation). Goal: execute the spec of #537.

Anchor: /srv/src/widgets/.claude/worktrees/issue-537-feed-curation/docs/developer/planning/specs/2026-08-17-feed-curation-design.md
Context: in the review the user chose curation per feed, not per category; that is not in the spec yet.
State: spec committed (a1b2c3d), adversarial review converged after 2 rounds (fixes in d4e5f6a),
nothing else uncommitted.
First step: /superpowers:writing-plans on that spec, then execute.
Watch out: on the always-on host the feed service is reached directly, no SSH.
```

Run /clear first, then paste the block.

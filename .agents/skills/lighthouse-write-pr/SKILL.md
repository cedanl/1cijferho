---
name: lighthouse-write-pr
description: 'Use before every `gh pr create` in this repo — "make a PR", the push + PR choice in finishing-a-development-branch, or when pr-create-gate.sh blocks. Verifies each acceptance criterion of the issue on the branch and writes the PR body itself — `Closes #N` with an Evidence table (one row per AC), `Refs #N — <reason>`, or `No issue: <reason>` — without asking a question, interactive and headless alike. NOTE — not for closing an issue or checking after the merge: use lighthouse-close-issue instead; repairing a red PR is lighthouse-fix-pr.'
metadata:
  lighthouse: "core@v0.8.1"
  binding: hard
  origin: own
---

# PR body with evidence

A PR says itself whether it finishes its issue: `Closes #N` with an evidence row per acceptance
criterion, or `Refs #N` with the reason why not yet. This skill verifies and writes that body; it
never closes an issue itself (the merge of a `Closes` PR does that, or the user). It asks no
question: interactive and headless (controller run, if present) the outcome is the same.

## When

- Before every `gh pr create`, also "make a PR" and the choice push + PR in
  `finishing-a-development-branch`.
- When `.claude/hooks/lighthouse/pr-create-gate.sh` blocks a `gh pr create`.
- `/lighthouse-write-pr <nr>`. Without an argument the number comes from the branch
  `issue-<nr>-…` (`git branch --show-current`). No issue: the body gets `No issue: <reason>`
  (≥10 characters) and no milestone; skip V1–V3 (V2b still runs).

## V1. Read the issue

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) || exit 2
gh api "repos/$R/issues/<nr>" \
  --jq '{title, milestone: .milestone.title, body}'
```

Every checkbox under `### heading:acceptance_criteria` (any language variant) is one AC; the hook
counts them and demands as many Evidence rows. Also note the DoD and the milestone title. `gh`
runs outside the sandbox (`dangerouslyDisableSandbox: true`).

## V2. Evidence per AC

Look for evidence per AC on the branch: `git diff origin/<default branch>...HEAD`, run commands,
open files. What counts as evidence and what does *not*: B2–B4 of
`lighthouse-close-issue`. No evidence = `not`, also when it is probably done.

## V2b. Roborev review (only with `[review] roborev = "local"`)

The session that did the work reviews it before `gh pr create`, so CI runs once, on code where
the findings are already processed. This step also runs with `No issue:`, where V1–V3 are
skipped. One command per step, in this order. Every command here runs outside the sandbox
(`dangerouslyDisableSandbox: true`): the gate reads the Codex budget, the review needs the
network. Any other non-zero exit of `roborev_gate.py` than exit 3 of `gate` (step 0) and exit 2
of `record` (step 3), such as `gate` exit 1 or a failing `classify`, `fixed` or `record`: report
it in the answer and skip the rest of V2b; V4 still adds the `body-line` output.

0. **Gate.**

   ```bash
   git fetch origin <default branch>
   python3 .lighthouse/scripts/roborev_gate.py gate
   ```

   One JSON object. Every `verdict` except `off` carries `state` and `raw`, absolute paths: take
   them literally, never build a path yourself.

   | `verdict` | Do |
   |---|---|
   | `off` | skip this section; V4 gets no roborev line and no comment |
   | `skip` | on to V4: the gate recorded the reason, the body repeats it |
   | `done` | on to V4: this branch was reviewed and processed already (a restart) |
   | `classify` | step 2, without a review: raw output of an earlier run is there |
   | `run` | step 1 |

   Exit 3 (`COULD NOT CHECK`): no PR; report the message.
1. **Review** (`run` only), with `<range>` and `<raw>` from the gate output:

   ```bash
   timeout 540 roborev ci review --agent codex --ref <range> > <raw> 2>&1
   ```

   In the foreground, with Bash timeout `600000`: start no background work (controller run, if
   present). Exit 124 is the timeout:
   `python3 .lighthouse/scripts/roborev_gate.py record --skip "review faalde: timeout na 9 min"`,
   then V4. Every other exit code goes to step 2: roborev exits 0 also with findings, so the exit
   code alone is no failure signal.
2. **Classify.**

   ```bash
   python3 .lighthouse/scripts/roborev_gate.py classify
   ```

   `verdict` `skip`: on to V4; the script recorded the reason. `done`: step 3, with `sha` and
   `findings` from this output.
3. **Process, one round** (`done` only). First the findings a commit on the branch already
   fixed (a run broken off halfway):

   ```bash
   python3 .lighthouse/scripts/roborev_gate.py fixed
   ```

   Each one listed is `fixed` in that commit; do not judge it again. For every other finding
   follow `superpowers:receiving-code-review`: fix it or refute it with a reason, with the
   severity rules of section 6 of `lighthouse-fix-pr` (high and medium seriously; a low only if
   the fix is trivial, otherwise `left`). Every fix is a commit of its own whose body has the
   line `roborev: <path>:<line> (<severity>)`, with `path`, `line` and `severity` as `classify`
   printed them (a finding without `path` gets no such line).

   Write the decisions to a JSON file in the scratchpad or `$TMPDIR`: a list with one object per
   finding, keys `path`, `line`, `severity` (as `classify` printed them), `uitkomst` (`fixed`,
   `refuted` or `left`), `commit` (the fix commit, else `null`) and `reden` (the reason, `null`
   with `fixed`). Record them, with `<sha>` the `sha` of `classify` and `<range>` the `range`
   that `gate` printed at step 0, the reviewed range (the fix commits came after it):

   ```bash
   python3 .lighthouse/scripts/roborev_gate.py record --done <sha> --decisions <file> --range <range>
   ```

   After a `classify` verdict at step 0 (a restart, no review in this run) there is no `range`:
   leave out `--range`.

   Exit 2: the decisions do not match the findings one to one, or one is inconsistent; the
   message says which. Fix the file and record again. No second review, also when a fix changed
   a lot. Then check whether the branch is already on origin:

   ```bash
   git ls-remote --exit-code origin <branch>
   ```

   Exit 0: push the fix commits before V4, so CI runs once, on the processed code:

   ```bash
   git push origin HEAD
   ```

## Verdicts

The three of B3 in `lighthouse-close-issue` (`demonstrably done`, `partly`, `not`), plus two:

| Verdict | Meaning |
|---|---|
| `after merge` | the AC depends on something that can only happen after the merge: a deploy, a measurement over days, a check on the always-on host (`host.toml`) |
| `after PR` | the AC measures a property of the PR itself and so can only exist once it does: its link to the issue (`closedByPullRequestsReferences`), the live run of the hook on its body. Nothing else falls under this. |

## V3. Choose the form

- Every AC `demonstrably done` or `after PR` → `Closes #N`, with a heading `### Evidence #N` and
  one table row per AC. An `after PR` row has as Result `heading:evidence_after_pr<expected fragment>`;
  entry C of `lighthouse-close-issue` measures it after the merge. The PR number exists only after
  `gh pr create`, so measure an `after PR` row through the issue: `gh api graphql` on
  `closedByPullRequestsReferences` of `issue(number:<nr>)` (see *Shape of the evidence block*).
  `gh pr view` without a number is refused by the allowlist: after the merge entry C measures in
  a detached worktree, without a branch to fall back on.
- Otherwise → `Refs #N — <reason>` on one line (reason ≥10 characters), with below it per open AC
  the verdict and what is still needed.

Mixing is allowed: `Closes #5` with Evidence #5 next to `Refs #6 — <reason>`.

## Changelog fragment (if present)

Only if present: `changelog.d/.lighthouse` in the repository root. Otherwise skip this section.
The release workflow rolls each PR's fragment into `CHANGELOG.md`, so a PR that changes anything
a user can notice adds one. First look at what the branch already adds:
`git diff --name-only --diff-filter=A origin/<default branch>...HEAD -- changelog.d/`. A change
that already has a fragment there gets no new one; a re-run of the skill (after a `pr-create-gate`
block, say) writes nothing twice.

For each change without a fragment, write one before `gh pr create`, commit it on the branch and
push. The file is `changelog.d/<branch>.md`, a `/` in the branch name becoming `-`. For the
branch's next fragment, or a name held by an open fragment of another branch, take the first free
`-<n>` from 2 (`<branch>-2.md`, `<branch>-3.md`). The format is front matter with exactly one
`type:` line between two `---` lines, then one to three sentences:

```
---
type: fixed
---
`lighthouse init` no longer fails on a repository without a `docs/` directory.
```

- `added`: new behaviour a user can use.
- `changed`: existing behaviour that works differently now.
- `fixed`: a defect that no longer occurs.
- `removed`: behaviour that is gone.
- `internal`: what a user does not notice (developer docs, tests, CI); it stays out of the release notes.

Pick the type from the diff and the AC. Write the text in English for a user, whatever
`[project] language` is.

## Evidence rules

Entry C runs every row again after the merge on `origin/<default branch>`, so every row must be
repeatable:

- no branch-relative command (`origin/main...HEAD`, `HEAD~`, `@{u}`);
- the Command cell is one single command between backticks, without pipe, `;`, `&&`, `$(…)` or
  redirect, and it is on the Evidence allowlist below. If an AC has to be proved outside it, it is
  no Evidence row: find a command on the list, or it becomes `after merge` (so `Refs`);
- the Result cell is a stable fragment of the output: no line number and no test count that moves
  with the default branch. The verdict is in the exit code; Result is a literal substring of the
  output.

A `|` inside a cell is written as `\|` (markdown); inside quotes that becomes a plain `|`, bare it
stays a pipe and the row is invalid.

## Evidence allowlist

At argv level, complete; the same list as `.lighthouse/lib/lighthouse_lib/evidence.py` (the
source) and entry C. Always: `argv[0]` is the bare name (no `/usr/bin/grep`, no `FOO=1 grep`), an
option not on the list is refused, an option with `=` and an empty value too.

| Command | Allowed | Not allowed |
|---|---|---|
| `grep` | fixed-text, regex and output options (`-F`, `-E`, `-i`, `-n`, `-r`, `-l`, `-c`, `-v`, `-w`, `-x`, `-o`, `-q`, …), `-e`, `--include`, `--exclude`, `--`, paths | — |
| `git grep` | the flags of `grep` without `-r`/`-R`/`-s`; as a value option only `-e`; `--`, refs and paths | `-O`, `--open-files-in-pager`, `--include`, `--exclude`, `--regexp` (write `-e`), branch-relative refs |
| `test`, `ls`, `wc`, `cat` | `test`: any expression; `ls`: `-l`, `-a`, `-A`, `-1`, `-d`, `-R`, `-h`; `wc`: `-l`, `-w`, `-c`, `-m`; `cat`: `-n`; paths | any other option |
| `git log`, `git show`, `git ls-files` | only `--oneline`, `--stat`, `--name-only`, `-n <number>`, `--format=`/`--pretty=` (only with `=`), `--grep=`, `--`, refs and paths | any other option (among them `--output`, which writes); `%G` in `--format`/`--pretty` (starts gpg); branch-relative refs (`..`, `@{`, `~`, `^`); global git options before the subcommand (`git -c`, `git -C`, `--git-dir`) |
| `uv run pytest` | only paths under the test directory `config.py test_gate.evidence_test_dir` (default `tests/`, with `::<test>`), plus `-q`/`-v` | any other option (`-m`, `-p`, `-o`, `-k`, `--`), any path outside that directory, options between `run` and `pytest` |
| `gh api <path>` | GET with `--jq`/`-q`; a path, not a URL | `-X`/`--method`, `-f`/`-F`/`--field`/`--raw-field`, `--input`, `-H`/`--header` |
| `gh api graphql` | exactly one `-f query=…` without `mutation`, plus `--jq` | any other field flag |
| `gh issue view`, `gh pr view` | exactly one number or github.com URL, `--json`, `--jq`/`-q`, `--repo`/`-R`, `--comments`/`-c` | `--web`/`-w` (starts a browser); no number (gh then falls back on the branch of the cwd) |

The test directory is `python3 .lighthouse/lib/lighthouse_lib/config.py test_gate.evidence_test_dir`.
A `--jq` expression may not contain `env`, `$ENV` or `input`/`inputs`. Extending the list is a
change of this skill *and* of `evidence.py`, never something a PR body can do.

Check each row as soon as you write it:

```bash
python3 .lighthouse/lib/lighthouse_lib/evidence.py check '<Command cell>'
```

Exit 0 (silent) = allowed; exit 1 + reason = choose another command.

Before the PR, run all rows together, outside the sandbox (`dangerouslyDisableSandbox: true`,
otherwise every `gh` row is `heading:evidence_unmeasurable`):

```bash
python3 .lighthouse/lib/lighthouse_lib/evidence.py run --issue <nr> --body-file <path> --cwd . --before-pr
```

Output (stderr included): a table `heading:evidence_table_head`. Every row must be
`heading:evidence_holds` or `heading:evidence_after_pr_unmeasured`; another row goes out, and then
that AC needs another command or it is not proved (`Refs`).

| Exit | Meaning | Do |
|---|---|---|
| 0 | all `heading:evidence_holds` or `heading:evidence_after_pr_unmeasured` | on to V4 |
| 1 | ≥1 `heading:evidence_fails` or `heading:evidence_invalid` | repair the row or take it out |
| 2 | no valid `Evidence #N` table (`heading:evidence_closes_without_block`) | repair the body |
| 3 | `COULD NOT CHECK` (body unreadable, `--cwd` not a directory) | blocks: no PR, report it |
| 4 | no deviation, but ≥1 `heading:evidence_unmeasurable` | blocks: take the row out, or run again outside the sandbox |
| any other | unknown | blocks: no PR on untested evidence |

## DoD

Goes to `## Verification` and does not decide between `Closes`/`Refs` ("code reviewed" can never
be true before the merge). Exception: if the tests are not green, the skill makes no PR and
reports that.

In Claude Code the commit gate (`.claude/hooks/lighthouse/commit-test-gate.sh` runs the test command
after every `git commit`, on the files of that commit) proves only the files of each commit, not the
suite. The suite evidence is CI on the PR: write `suite: CI` in `## Verification`, and do not run the
full suite by hand. A red or `COULD NOT CHECK` outcome of the gate on this branch that no later
commit fixed means the tests are not green. Only Outside Claude Code (OpenCode, Codex) do you run
the suite yourself, with the command from
`python3 .lighthouse/lib/lighthouse_lib/config.py test_gate.command` (rc 1: no test command
configured; say so in `## Verification`).

## V4. Body file and command

Write the body from `.github/pull_request_template.md` to the scratchpad or `$TMPDIR`, in a
separate step (the Write tool), not in the same Bash call as `gh pr create`: the PreToolUse hook
reads the file before the command runs.

Open review points: give the spec and plan documents the branch touches
(`git diff --name-only origin/<default branch>...HEAD -- '<paths.specs>/*.md' '<paths.plans>/*.md'`)
to `python3 .lighthouse/scripts/plan_review_record.py pr-open-points <path>...`; the script follows the spec
from the **Spec:** line of a plan itself, also when that spec is already on the default branch. If
the output is not empty, put it unchanged as a section of its own `## Open review points` in the
body; if it is empty, leave the section out. It is no Evidence row and does not count in the AC
count of `pr-create-gate.sh`. Name the headings here only inline between backticks: a line that
starts with `## ` breaks the V4 section the tests cut out.

Roborev line (V2b ran): add the output of `python3 .lighthouse/scripts/roborev_gate.py body-line`
to the body as a line of its own, outside the Evidence block. It is the marker
`<!-- roborev-lokaal: <sha> -->` after a review, `Roborev overgeslagen: <reason>` after a skip, and
empty with `off` (then no line).

Then give the command:

```bash
gh pr create --title '<title>' --body-file <path> --milestone "<milestone>"
```

`--title` always: without it gh asks for it, and headless it refuses. Single quotes, so the shell
executes nothing in the title. `--milestone` only if the issue has one; with `No issue:` never.
Never `--body "$(…)"`, `--fill` or `--web`: the hook blocks them.

Roborev comment, only after a `done` outcome of V2b (also when every finding was refuted). Right
after `gh pr create`, with `<nr>` the number at the end of the URL it printed, and `<state>` and
`<raw>` literally from the output of `roborev_gate.py gate` (after a restart, run `gate` again;
never build a path yourself):

```bash
python3 .lighthouse/scripts/roborev_comment.py render --pr <nr> --bron lokaal --state <state> --raw <raw> > <file>
```

```bash
R=$(python3 .lighthouse/lib/lighthouse_lib/config.py forge.repo) && gh pr comment <nr> --repo "$R" --body-file <file>
```

`gh` outside the sandbox. If `gh pr comment` fails, report it: the state and the raw output stay
on disk, so a second try needs only these two commands.

## Shape of the evidence block

```markdown
## Related issues

Closes #1091

### Evidence #1091

| AC | Command | Result |
|---|---|---|
| 1. Template and skill describe both forms | `grep -F 'Evidence #' .github/pull_request_template.md` | `### Evidence #` |
| 3. Hook blocks a body without a form | `uv run pytest tests/test_pr_create_gate.py -q` | `passed` |
| 6. Development panel shows the PR | `gh api graphql -f query=…closedByPullRequestsReferences…` | `after PR: "number":` |
```

## Counterpart

`.claude/hooks/lighthouse/pr-create-gate.sh` (PreToolUse on `Bash`) enforces this form at every
`gh pr create`/`gh pr new`: no valid form, an Evidence table with another number of rows than
ACs, or a body that does not come from a literal value or `--body-file` → block. If it cannot
count the ACs (no `gh`, no network), it lets through loudly with
`AC count not measurable` — that is no pass, name it in `## Verification`.

## Outside Claude Code

OpenCode and Codex do not run the hook. Check the body yourself then:

```bash
printf '%s' "gh pr create --body-file <path>" | python3 .lighthouse/lib/lighthouse_lib/pr_body_gate.py "$PWD"
```

Exit 0 = fine (read the messages on stdout); 2 = blocked (reason on stderr); 3 = open quote; any
other exit = not checked, so no PR.

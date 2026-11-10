---
name: lighthouse-audit-worktrees
description: 'Audits worktrees, local and remote branches, PR state and sessions of this repo in one table with a verdict per row. Use at "which worktrees can go", "clean up the worktrees", "which branches on origin are merged", "was a session still open on a worktree", "where did I leave off on X", "how many worktrees are there", or /lighthouse-audit-worktrees. Reports and delivers commands; changes nothing itself. NOTE — for milestones, labels or the board status this is the wrong skill; for creating a new worktree instead of cleaning one up, use the skill using-git-worktrees.'
metadata:
  lighthouse: "core@v0.8.1"
---

# Audit Worktrees

One join over `git worktree list`, `git branch` (local + remote), the PR state per head branch
and the sessions on disk, with a verdict per row. Six views on the same set of rows.

```bash
python3 .lighthouse/scripts/worktree_audit.py            # --cleanup (default)
python3 .lighthouse/scripts/worktree_audit.py --sessions
python3 .lighthouse/scripts/worktree_audit.py --origin
python3 .lighthouse/scripts/worktree_audit.py --kpi
python3 .lighthouse/scripts/worktree_audit.py --only <branch>
python3 .lighthouse/scripts/worktree_audit.py --issue <nr>
```

`--only` and `--issue` are the views for `lighthouse-close-issue`: one branch, or every branch of
an issue, each with a line `cleanup: yes` or `cleanup: no — <reason>`. `--only` exits 0 only at
`yes`; `--issue` exits 2 when the PR lookup fails, and otherwise 0 with the verdict per block.
That skill runs the delivered lines of every `yes` itself, without a question; this skill and the
script still change nothing.

The repo and the default branch come from `lighthouse.toml` (`forge.repo`,
`forge.default_branch`).

## When to Use

- The worktrees have piled up again ("which can go?").
- After a series of merged PRs, to clean up origin.
- "Was a session still open on a worktree?" / "where did I leave off?" → `--sessions`.
- Periodically, as a cleanup round; row 2 of the KPI table is the trigger.

Do not use it to *make* a worktree — that is `superpowers:using-git-worktrees`. A board or
milestone sweep (board-sweep, only if present) is another skill.

## Hard rule

**This skill changes nothing.** No `git push origin --delete`, no `git branch -D`, no
`git worktree remove`, no `unlock`, no `kill`, no writing to a port registry. The script prints
copyable commands; the human pastes. The human gate stands once, right before the irreversible
— `git push origin --delete` cannot be undone from a script.

`run_git` in the script refuses every git verb that does not read, so that norm sits in the
execution path and not only in this text.

## Four things `git worktree prune` does not catch

1. **Squash-merge makes `git branch --merged` a lie.** Everything is squash-merged to the
   default branch, so the original commits are not in its history. The PR lookup on the head
   branch is the only reliable test.
2. **Unpushed commits.** Work that exists nowhere else. The script counts with
   `git rev-list --count <b> --not --remotes` — "exists nowhere else", not "not merged".
   **After a squash-merge that count is by construction the whole branch history**: GitHub
   throws the separate commits away and puts one new commit on the default branch, so the
   originals never reach a remote. That is why rank 4 also looks at the merge moment; see the
   ladder below.
3. **Fixture branches from a failed test run.** A test that makes its fixtures with
   `git checkout --orphan` in a `mktemp -d` repo lands them in the **real** repo when that
   `mktemp` fails (a sandboxed shell). They read as plain unpushed work; the ladder names them
   separately.
4. **Locked worktrees.** A lock can stay behind after the session that set it. Report it, do
   not force it. **In auto mode the permission classifier refuses `git worktree unlock` and
   `git worktree remove` on a locked worktree.** Then do not clean up; report it. A lock with a
   dead pid falls through the ladder with the note "stale lock"; the skill never unlocks.

## The verdict ladder — first hit wins

| # | Condition | Verdict |
|---|---|---|
| 1 | name or directory on the protect list | 🔒 never touch |
| 2 | worktree `locked` and the lock pid lives in `/proc/<pid>` (or the pid is unknown), or a running session has the worktree as cwd | ⛔ active session / running session |
| 3 | dirty tree, or tree status unreadable | ⛔ write it away first |
| 4a | unpushed commits, tip without a parent, local, no remote, no PR | ⚠️ orphan commit without parent — likely test leakage |
| 4b | unpushed commits while the PR is merged and the tip lies before the merge | *no hit* — falls through to rank 6 with the modifier `[squash-merge]` |
| 4c | unpushed commits (`--not --remotes` > 0), or the count is unknown | ⚠️ check |
| 5 | open PR | ⏸️ PR open |
| 6 | merged PR | ✅ safe to remove |
| 7 | no open or merged PR, and PR state unknown or content check failed or unique files against the default branch | ⚠️ PR status unknown / content check failed / unique work — review |
| 8 | no PR, the content is in the default branch — or there is no content (`<default>..<branch>` counts 0) | ✅ safe to remove / ✅ empty — no own commits |
| 9 | directory under `.claude/worktrees/` not in `git worktree list` | ❓ orphan — manual |

Rank 9 typically appears in a devcontainer: the `.git` of such a directory points to a path
inside the container, so git does not know it and `git worktree prune` never touches it. Only
`git diff --name-only <default>...<branch>` plus `git cat-file -e <default>:<f>` per file finds
what really exists only there.

Rank 8 has two texts, because "nothing unique" has two causes. Either the work landed in the
default branch, or there never was work: a branch that was created and never committed. The
second is called **empty — no own commits**. Both are safe to remove: the tree is clean
(rank 3), so nothing gets lost. But an empty worktree is often a session that still has to
start. For a fresh *last session* check whether someone is just starting on it.

**Worktrees of a session server** (a remote-control service that starts sessions in
`.claude/worktrees/bridge-…`) never get a PR. As long as a session has them as cwd: rank 2.
After that: rank 8 without own commits, rank 4 with. Their lock (`claude agent bridge-…`)
carries the pid of the server, not of the session; the audit therefore does not count it as a
session signal (modifier `server-lock`). Such a ✅ row is still locked, so it is not in the
cleanup commands but in a section of its own: the user runs `git worktree unlock` by hand and
then cleans up, or leaves it until the next server start.

Ranks 3 and 4 stand **above** rank 6: a merged branch with unpushed commits never gets a flat
"safe to remove".

**4b is the exception that keeps that rule standing.** The question of rank 4 is "does this
branch carry work that exists nowhere else". After a squash-merge `--not --remotes` answers
that question wrongly: it then always counts the whole history, also when every line is neatly
in the default branch. Because rank 4 stands above the content check of ranks 7 and 8, such
branches were never measured on content. The replacement measure is narrow: **was anything
committed after the merge moment?** If not, everything landed in the squash commit and the row
drops to rank 6 with the modifier `[squash-merge]`, so that it stays visible *why*. If so — or
if one of the two timestamps is missing — rank 4 stands: committing after the merge is real
work.

Both timestamps are **epoch seconds**, and that is no implementation detail. `merged_at` comes
from GitHub in UTC ("Z"), the commit date from git with a local offset. Compared as text,
`08:24+02:00` reads as *later* than `08:08Z` while it is two hours earlier. Hence
`git rev-list -1 --timestamp` (epoch, no offset) and not `log --format=%cI`.

**4a names, it gives no permission.** The verdict stays ⚠️ and so never "safe to remove" — a
heuristic may not decide to delete by itself in this skill. The check is narrow on purpose: one
commit that is its own root. A pre-rewrite branch also has a separate history against the
default branch, but it does have parents, and so falls outside it.

## The protect list

The default branch is always protected; a setting you can forget to fill in is no protection.
On top come the refs and paths only this project knows, from `lighthouse.toml`:

```toml
[worktree_audit]
protected_refs = ["entire/*"]        # e.g. checkpoint refs of a CLI
protected_paths = [".keep-me"]       # plus every directory that contains one
```

Such things have no PR, are not merged, and are not missed until something breaks. A port
registry (`.port-registry`) belongs to the opencode lane of the source project
(`scripts/opencode-lane/worktree-run.sh`, only if present), which hands out a reproducible port
block per worktree name.

## Non-negotiables

- **Change nothing**, not even "just this one obvious case".
- **`gh api`, never `gh pr list --json`** — that breaks under a proxy, and an unknown filter is
  silently ignored there. In Claude Code `gh` runs with `dangerouslyDisableSandbox: true`.
- **Never `git branch --merged`** as a merge test.
- **Never force or unlock a lock.** Report the pid with it.
- **Port blocks are reported, never killed** (only if present, with a port registry). The skill
  shows which block is registered on which name; which *process* holds that port it does
  **not** look up — for that it would have to scan the ports. So do not read a missing pid as
  "nothing runs". Killing dev servers that hang on a removed path is out of scope; this skill
  detects, it does not guard.
- **Write uncommitted work away first** before anyone pastes a `worktree remove --force`.

## KPIs

All four come from the same call: `python3 .lighthouse/scripts/worktree_audit.py --kpi`.

| Metric | Field | Target |
|---|---|---:|
| Worktrees under `.claude/worktrees/` | `worktrees=` | ≤ 4 |
| Rows with verdict ✅ that stay | `safe=` | 0 |
| Remote branches on origin | `remote_branches=` | ≤ 15 |
| Rows with ⚠️ (unique work or local commits) | `warn=` | 0 |

Every column comes from one `--kpi` call, after `git fetch --prune`; they are measurements, not
hand counts. They also move between two calls when another session commits in the meantime —
that is the drift this skill makes visible, no measurement error.

Row 2 is the real measure. It drops to zero as soon as the command block is pasted and then
rises again by itself; that rise *is* the signal to run this skill.

## Output format

`--cleanup` opens with the table and closes with copyable blocks:

```
| worktree | branch | PR state | unpushed commits | dirty tree | last session | live? | verdict |
```

Then, filled only with rows that carry verdict ✅, in this order — never reversed:

```bash
git worktree remove <path>
git branch -D <branch> <branch> ...
git push origin --delete <branch> <branch> ...
```

The order is no taste but pasteability: git cannot delete a branch that is checked out in a
worktree ("Cannot delete branch … checked out at …"), so `worktree remove` must stand before
`branch -D` — the reversed order breaks a top-to-bottom paste halfway.

Report to the user the rows that are **not** ✅ with their reason — that is the signal half.
Writing out a list of 84 correct ✅ lines is noise; name the count and paste the block.

## Overlap

- A session index answers "where did I do X again" from indexed transcript content. This skill
  answers "what can go" from git state. The only touching point is the column *last session*,
  and that costs one `stat` here — not worth an API coupling.

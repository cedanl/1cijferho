---
name: lighthouse-shell-standard
description: Use when writing or changing shell that guards something — hooks in .claude/hooks/, pre-commit, pre-push or CI gate scripts, and the tests that assert on them. Carries the norm "the absence of a signal is treated as a negative result" plus a KPI table with commands that run in this repo. NOTE — for plain Python or pytest this is not the skill; for workflow YAML neither.
metadata:
  lighthouse: "shell@v0.8.1"
---

# Shell that guards something

## Domain

Shell whose exit code another person or another agent believes: the hooks in
`.claude/hooks/`, the gate scripts, and the tests that assert on them. Which directories
count as gate shell is set in `lighthouse.toml` under `[shell] strict_dirs` (default
`.claude/hooks`); ask with `python3 .lighthouse/lib/lighthouse_lib/config.py shell.strict_dirs`.
Ordinary helper scripts fall half under it: only the warning bar applies there, see KPI
row 2.

Out of scope: Python and workflow YAML.

What a good test is lives in `lighthouse-test-standard`; this file gives only its shell form
(below). Before the first edit, write the classification sentence from that skill in the chat;
the classes and their minimal checks are in its section *Classify first*:

```text
This is a <class> change; the likely regression surface is <observable behaviour>.
```

## The norm

**The absence of a signal is treated as a negative result.**

"Nothing found" and "could not look" both come out as green. That is one class of
defect, and these six cases of it slipped past every existing check: tests green, gate
test green, the code looked fine.

| # | What was written | What it did |
|---|---|---|
| 1 | `check "... is blocked" nonzero "$?"` | `PASS` while the script did not exist (rc=127 is nonzero too) |
| 2 | `die()` inside `$( )` | a sha256 mismatch printed the error and still exited **0**; it left `-shellcheck ""` behind, which actionlint reads as "integration off" |
| 3 | a hook shim with `$(git rev-parse --show-toplevel)/scripts/...` | `.git/` is shared across worktrees; from any other branch rc=127 |
| 4 | `git diff ... 2>/dev/null \| grep -q` | a failing diff gives empty output, read as "no change", and the push went through unaudited |
| 5 | a test fixture without `origin` | the "brand-new branch" check never reached the merge-base path it was testing |
| 6 | `--online` without a token | logged `(online)`, silently ran offline, rc=0 |

Four of the six were in the **plan**, not in the execution. A review lane afterwards is
therefore not where this gets solved; this skill is the place before, the shell-lint-check hook
the place after.

**Three outcomes, not two.** `shellcheck-repo.sh` returns 0 = checked and clean, 1 =
checked with findings, 4 = **not checked**. A hook has only exit 0 and exit 2, so it tells
the last two apart on its first line: a finding starts with `<gate>:` and its message,
"not checked" with `<gate>: COULD NOT CHECK — <reason>`. "Not checked" is a valid answer
and may never be presented as a pass.

## KPIs

Run each row from the repository root.

| Norm | Command | Target |
|---|---|---|
| Gate shell is clean down to `style` | `bash .lighthouse/scripts/shellcheck-repo.sh` | rc 0 |
| All shell in the repo is clean on `error`+`warning` | `git ls-files -z --cached --others --exclude-standard '*.sh' '*.bash' \| xargs -0 bash .lighthouse/scripts/shellcheck-repo.sh --severity warning --` | rc 0 |
| No gate shell falls outside git | `dirs=$(python3 .lighthouse/lib/lighthouse_lib/config.py shell.strict_dirs) && printf '%s\n' "$dirs" \| while IFS= read -r d; do [ -n "$d" ] && find "$d" \( -name '*.sh' -o -name '*.bash' \) 2>/dev/null; done \| git check-ignore --stdin` | rc 1 |

`shellcheck-repo.sh` without files checks the strict set, at `style`; finding no file
there is rc 2, not green. Without shellcheck it is rc 4: nothing was checked.

The row this table cannot carry is the most important one: the tests of your own gates
went red once for the right reason. A test that stays green after you break the mechanism
it guards measures nothing. See the first non-negotiable.

### Two rows that were dropped

Mentioned on purpose, because a KPI table that pretends to cover the whole class of
defect is itself an instance of that class.

- **"No unmotivated error suppression: 0 hits on `2>/dev/null`."** Nearly all hits are
  harmless (version probes where the `grep` behind them is the real assertion, globs).
  The dangerous variant, defect 4, cannot be told apart from the harmless one with grep;
  the difference is semantic: *does an empty result take the lenient branch?* Dropped as
  a KPI, kept below as a non-negotiable.
- **"Every blocking assertion also checks the output."** Too noisy: a negative `grep`
  rightly has no output. Remove the cause instead: an assertion helper that does not count
  rc 126 and 127 as a finding makes defect 1 impossible. That is stronger than a counter.

## Non-negotiable

These three cannot be grepped. They stand apart from the KPI table because no command
measures them, not because they count for less.

- **A gate is not done until you have seen it red once for the right reason.** Break the
  thing under test, watch the assertion fail, restore it. This is what surfaced defects 2
  and 6. ✅ run the mutation and show the FAIL · ❌ "the test should pass now".
- **Tell "checked and found nothing" apart from "could not check".** Every exit code you
  ignore, every empty output you draw a conclusion from. ✅ `rc=4` and a separate branch ·
  ❌ `2>/dev/null | grep -q` whose empty result means "no change".
- **A test that names a code path must also reach that path.** Defect 5 was green and
  tested nothing. ✅ build the fixture so the path is provably reached (a marker in the
  output) · ❌ trusting the name of the test.

## The shell form of lighthouse-test-standard

The norm is in `lighthouse-test-standard`; this is its form for a gate or a hook.

- **Run the script, against controlled input.** Build a temporary directory (`mktemp -d`, or
  pytest `tmp_path`) holding exactly the fixture that reaches the path under test. Never point
  the script at the real repo or at `$HOME`.
- **Assert the exit code and the output.** Both: the exit code alone lets defect 1 through
  (rc 127 is nonzero too), and the first line's prefix tells a finding from `COULD NOT CHECK`.
- **Run it, never read or grep the source.** A test that greps the script stays green when
  the mechanism breaks.
- **Per gate, one input that must block and one that must pass.** This is the minimal check
  of the `hook/gate` class. With only the blocking input, a gate that blocks everything
  still passes.

## The pairing

| Half | Carries | When |
|---|---|---|
| this skill | the norm, the six defects, the commands | while you write |
| `lighthouse-test-standard` | the test norms and the class table | for every test, and before the first edit |
| `.claude/hooks/lighthouse/shell-lint-check.sh` | shellcheck on every edited `*.sh` and `*.bash`, blocking | right after the edit |

The hook is deliberately **not fail-open**. Skipping silently is itself the defect the
gate exists to prevent: shellcheck is pinned, sha256-verified and lives in the shared tool
cache, so "I could not look" is a defect and not a normal state. A missing tool means
exit 2 with the reason, never exit 0. Its first line is then
`shell-lint-check: COULD NOT CHECK — <reason>`.

Two blocking bars, deliberately two different sets: the strict directories down to
`style`, the rest on `error`+`warning`. Info and style findings in the other scripts
would otherwise block every edit on debt that has nothing to do with the change.

The pin and its download live in `.lighthouse/scripts/lib/shellcheck-bin.sh`. The order
is `SHELLCHECK_BIN`, the pinned binary in `<git-common-dir>/lighthouse-tools/`, a pinned
shellcheck on `PATH`, a download of the pinned release, and only then a different version
on `PATH`, with a notice. A different version is no equal alternative to the pin: an older
shellcheck does not know today's disable codes, so the same tree goes red on one machine
and green on another.

## Known pitfalls

- **A comment line that starts with `# shellcheck ` is a directive.**
  `# shellcheck is GPL-3.0. ...` gives SC1072/SC1073. Put something in front of it
  (`# License: shellcheck ...`).
- **`# shellcheck source=...` belongs directly above the `.` line.** With
  `set -a; . "$f"; set +a` on one line, the directive applies to `set -a` and SC1090
  fires anyway. Split the line.
- **`git ls-files` sees only tracked files.** A brand-new hook falls outside it and the
  gate reports green over a set it was not in. Always `--cached --others --exclude-standard`.
- **And that is not enough when the path itself is ignored.** The standard Python
  `.gitignore` contains an unanchored `lib/`, which matches every directory with that name,
  so a `hooks/lib/` silently falls outside git. Hence KPI row 3.
- **`$(git rev-parse --show-toplevel)` from inside `.git/` points at the wrong tree.**
  `.git/` is shared across worktrees (defect 3). Use `LIGHTHOUSE_REPO_ROOT`, or derive
  the root from the path you received. Not `$CLAUDE_PROJECT_DIR`: it points at the main
  checkout, so an edit in a worktree would be checked against another branch.
- **`die()` inside `$( )` kills the subshell, not the script.** `X="$(f)" || exit "$?"`,
  and then check that `X` is not empty.

## macOS: three GNU assumptions CI does not catch by construction

A Linux runner has bash 5, GNU coreutils and gawk; a Mac has bash **3.2.57**, BSD realpath
and BSD awk. A hook can be green everywhere in CI and broken on the Mac. Without a macOS
runner, check these three by hand.

- **`realpath -m` exists only in GNU coreutils.** BSD answers
  `realpath: illegal option -- m`, and `X=$(realpath -m "$p") || exit 0` turns that into a
  silent fail-open: defect 4, in the hook that was meant to close the gap. Use a helper
  that tries `realpath -m` and otherwise
  `python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))'`. The flag matters:
  the target path of a `Write` need not exist yet.
- **No `VAR=$(cat <<'EOF' … EOF\n)`.** Bash 3.2 reads a heredoc inside a command
  substitution as shell, not as a heredoc; every backtick in the body becomes a
  substitution itself. Result: `unexpected EOF while looking for matching \``. In a
  PreToolUse hook that is fatal: it does not parse and so blocks **every** Bash command in
  the session. Write `IFS= read -r -d '' VAR <<'EOF' || true`.
- **`awk -v` does not take a newline.** BSD awk stops with `awk: newline in string`. Pass
  a multi-line value through the environment (`FOO="$bar" awk 'BEGIN { x =
  ENVIRON["FOO"] }'`) and normalise it with `tr '\n' ' '`; an `index(s, " " w " ")` never
  finds a word at the end of a line otherwise.

Check on the Mac itself: `bash -n <script>` (bash 3.2 is `/bin/bash`) plus the matching
tests. Green on the runner says nothing here.

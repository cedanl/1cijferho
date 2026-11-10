---
name: lighthouse-python-standard
description: Use when writing or changing Python in this repository's source or test directories — modules, pytest tests, and the ruff/ty checks on them — and when planning a refactor, merging modules or choosing a test strategy, before a file is open. Carries a KPI table whose every row is a command that runs in this repository, plus the measurement rules (a ty zero without its extras is not a zero). NOT for shell that guards something (use lighthouse-shell-standard if present), workflow YAML, or files outside the repository. In OpenCode the enforcing hooks do not run — run the KPI commands by hand.
metadata:
  lighthouse: "python@v0.8.1"
---

# Writing Python in this repository

## Domain

Python in the source directories (`python3 .lighthouse/lib/lighthouse_lib/config.py python.source_dirs`)
and the tests, and the checks on them: `ruff`, `ty`, pytest. Also applies when no file is open yet —
while brainstorming or planning, when you decide *how* you will test something. That is why this
is a skill and not a rule.

Out of scope: shell that guards something (`/lighthouse-shell-standard`, if present) and workflow YAML.

The project's **norms** for Python, such as no network in unit tests or patching the import name,
live in `.claude/rules/python.md` if present. They live there and not here, so one place carries
them. This file carries the procedure and the measurement, plus one norm that applies before the
first file: *replace, don't layer* (below).

What a good test is lives in `lighthouse-test-standard`; this file gives only its Python form
(below). Before the first edit, write the classification sentence from that skill in the chat;
the classes and their minimal checks are in its section *Classify first*:

```text
This is a <class> change; the likely regression surface is <observable behaviour>.
```

## KPIs

Every row is a command that runs in this repository.

| Norm | Command | Target |
|---|---|---|
| The blocking ruff rules are at zero | `uvx ruff@"$(cat .ruff-version)" check --select E722,B006,F403,F811,B023,PT012,PGH003,PGH004 $(python3 .lighthouse/lib/lighthouse_lib/config.py python.lint_dirs)` | rc 0 |
| The source directories are type-clean | `uvx ty@0.0.72 check $(python3 .lighthouse/lib/lighthouse_lib/config.py python.source_dirs)` | 0 diagnostics, naming the extras |
| The test suite is green | `bash -c "$(python3 .lighthouse/lib/lighthouse_lib/config.py test_gate.command)"` | rc 0 |
| Diff coverage on your new source lines | `python3 .lighthouse/scripts/quality/ratchet.py coverage` (optional, when in doubt) | ≥ 90% |

Without a `.ruff-version`, drop the pin: `uvx ruff check …`. Row 3 is the command the test gate
runs after every commit, on the files of that commit;
`python3 .lighthouse/scripts/gate_python.py -- uv run pytest -q` picks the tests of the commit. A red
run comes back to you: fix it in a new commit. The selector exits 3 when it could not check (a
list it cannot read, a command that cannot start): list 3 in `[test_gate] unchecked_exit_codes`.
When `config.py test_gate.command` gives rc 1 or an empty line, the project has none
(`command = ""` turns the gate off).

Row 4 is not a required step before the PR. When the repository runs the nightly ratchet
(`python3 .lighthouse/scripts/quality/ratchet.py nightly`), that judges each merged PR; this row is
its counterpart for when you want to know beforehand whether you will pass. The base is
`origin/<forge.default_branch>`.

The ratchet reads three project files: `quality-baseline.toml` (the counters and the coverage
floors), `.ruff-version` and `.complexipy-version` (the pins). `lighthouse init` does not write
them. Without them the repository does not use the ratchet, and `ratchet.py` stops with
`… is missing`: skip row 4 and the ratchet commands then.

### Why row 1 is scoped

The naive form — `ruff check` on the changed files, target 0 — fails as soon as the project lints
rules that still have existing hits, without anything being wrong with your change.

**The linted set and the blocking set are two different sets.** `extend-select` in
`pyproject.toml` is what ruff reports everywhere; `BLOCKING_RULES` in
`.claude/hooks/lighthouse/ruff-format-check.sh` is the subset on which an edit fails hard — on purpose
only rules that are at zero repository-wide, so legacy debt never becomes a block. The KPI row
takes exactly that set, so skill and hook measure the same axis.

### Why row 2 names its extras

`ty` only finds type errors in code whose imports it can resolve. A module it cannot find makes
every symbol from it `Unknown`, and on `Unknown` no type error is possible. The same commit can
therefore give two outcomes: with only the test extras synced an optional dependency does not
resolve and the source is green; with all extras synced `ty` checks those bodies for real. That
state is recorded nowhere and does not show in the diff. Say which extras were synced.

Note: the test gate runs only in Claude Code. In OpenCode nothing local runs the suite before a
commit or push — run `bash -c "$(python3 .lighthouse/lib/lighthouse_lib/config.py test_gate.command)"`
by hand.

### The row that is not there

On purpose: a KPI table that pretends to cover the whole class of errors is itself an instance of
that class.

**"`mock.patch` without an assert: 0 hits."** Most patches that stub out config, time or a path
rightly have no call assertion. The dangerous case (a patch that *misses its target*) cannot be
told apart from the harmless one with grep; the difference is semantic. It is a norm, not a
counter.

## Non-negotiable

These two are about the measurement, not about the file.

- **A zero without naming the extras is not a zero.** ✅ "0 diagnostics with
  `uv sync --all-extras`" · ❌ "`ty` is green".
- **Three outcomes, not two**: passed, failed, and *not measurable from here*. A module that
  imports a platform-only framework does not resolve on another platform. ✅ saying that module is
  not measurable here · ❌ presenting the repository-wide zero as if it covers that module.

## Reporting done

Before you say done, run rows 1 to 3, also those you think you did not touch; row 4 is optional
(see above). Report per row, row 4 included:

- the command, as it ran;
- the outcome: passed, failed, or not measurable from here;
- for row 2, the extras that were synced;
- what was not measurable, and why. Row 4 without the ratchet's project files is not
  measurable, not passed; row 4 left out by choice is reported as not run.

A row left out of the report reads as passed. Leave none out.

## Merging or deepening: replace, don't layer

When you merge shallow modules into one deeper interface, or pull logic behind an existing
interface, the tests on the old, shallow modules are waste as soon as the new interface covers the
same behavior. Remove them in the same change; do not leave them next to the new tests.

- ✅ `test_a.py` and `test_b.py` gone in the commit in which `test_ab.py` tests their behavior
  through the new interface · ❌ keeping both sets "to be safe".
- **First covered, then gone.** An old test goes only once the behavior it tests is tested through
  the new interface. Otherwise removing it is losing coverage, not cleaning up.

Why here and not in a rule: the decision falls while planning ("we merge A and B"), before a source
or test file is open — when this skill loads and a rule does not yet. Why no KPI row: whether an
old test covers the same behavior as a new one is semantic, like the patch row above; grep does not
see the difference.

## The Python form of lighthouse-test-standard

The norm is in `lighthouse-test-standard`; these are the Python tools for it.

- **Literal expectations.** `pytest.mark.parametrize` with a literal `expected` per case, never
  one computed in the test.
- **Specific fakes.** `create_autospec` or `spec=`, so a call with the wrong signature fails;
  `assert_called_once_with` where the argument belongs to the contract.
- **Never mock the subject.** A real `tmp_path`, `monkeypatch` and `capsys` instead of a mocked
  collaborator for the filesystem, the environment or stdout.
- **Shell scripts run, they are not read.** Run them with `subprocess.run` against a `tmp_path`
  and assert stdout, the exit code and the files they write; never grep the source.

## Reaching a complexity limit: deepen, don't split

When a function is over the limit (cyclomatic 20, cognitive 25; for new code 10 and 15), solve it
by hiding a decision behind an interface of its own, not by chopping it into pieces that can only
be understood together.

- ✅ replacing an `if/elif` chain over message types with a dispatch table, or lifting the parsing
  into a pure function with its own test, so the caller reads one line · ❌ `_step_1(a, b, c, d)`
  and `_step_2(a, b, c, d)` that are called once and pass the local state back and forth
  (*conjoined methods*), or a helper that only passes through (*pass-through method*).
- **Why an agent needs this.** It optimizes literally for the number, and splitting lowers the
  number without making anything simpler.
- **Measure it.** `python3 .lighthouse/scripts/quality/ratchet.py ruff`. A new helper above
  cognitive 15 raises the counter `cognitive_over_15`, so shallow splitting shows there.

Source: Ousterhout, *A Philosophy of Software Design* (deep modules, *conjoined methods*,
*pass-through method*), the same thought as "replace, don't layer" above. Why here and not in a
rule: the decision falls while planning the refactor ("how do I trim this function"), before a
source file is open.

## The pairing

> **OpenCode does not run these hooks.** Claude Code hooks do not fire here, and
> `.claude/rules/python.md` (if present) is not loaded by itself, so both the enforcement and the
> norm half are missing. After every Python change, run by hand:
> `uvx ruff@"$(cat .ruff-version)" check --select E722,B006,F403,F811,B023,PT012,PGH003,PGH004 <file>`
> and `uvx ty@0.0.72 check $(python3 .lighthouse/lib/lighthouse_lib/config.py python.source_dirs)`.
> Open the norms yourself, as `AGENTS.md` prescribes. This is a deliberate asymmetry, not an
> omission: do not "fix" it by removing the hook reference.

| Half | Carries | When |
|---|---|---|
| this skill | the procedure, the KPI table, the measurement rules | while you write, also before the first file |
| `lighthouse-test-standard` | the test norms and the class table | for every test, and before the first edit |
| `.claude/rules/python.md` (if present) | the norms | as soon as Claude reads a source or test file |
| `.claude/hooks/lighthouse/ruff-format-check.sh` | the blocking rules, per edit, exit 2 | right after the edit |
| `.claude/hooks/lighthouse/ty-gate.sh` | a signal on type-dirty source directories, once per turn, where the turn touched a Python path | at `Stop` |

When a hook cannot check — no `jq`, no `uvx` or `ruff`, a broken environment — it says so with
`COULD NOT CHECK` instead of passing silently. `ruff-format-check` then blocks (exit 2): an unchecked
file must not read as a clean one. `ty-gate` runs at `Stop`, where blocking would keep the
session from stopping, so it reports it as a message. It checks only a checkout in which the turn
touched a Python path (a `.py` or `.pyi` file in `python.source_dirs`, `pyproject.toml`, `ty.toml` or
`uv.lock`: an edit, a Bash write or a commit in the turn); any other checkout gets `not checked`.

## Known pitfalls

- **A fresh worktree has no `.venv`.** `ty` then reports diagnostics for every unresolved import;
  the hook falls back to the venv of the main checkout, a manual `uvx ty` does not. Run one
  `uv run` or `uv sync` first.
- **`uv run ruff` may not exist in a fresh worktree**: ruff need not be in the project venv. Use
  `uvx ruff@<pin>`; the pin is in `.ruff-version`.
- **The patch target is the import name, not the definition name.** A patch on the definition
  misses the name the code under test actually calls.
- **A test that is fast and green proves nothing about the mock.** Add the
  `assert_called_once()`; without that assertion a later patch can silently miss its target again.

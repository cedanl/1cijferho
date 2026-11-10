---
name: lighthouse-test-standard
description: 'Use when writing, changing, fixing or reviewing a test, a mock, a fake or an assert; when adjusting a test after a red run; and when planning a test strategy, before any file is open. Carries what a good test is: the production change that should make it fail, the classification sentence before the first edit, and the mutation check before reporting done. Triggers on the work, not on the language. Use it alongside superpowers:test-driven-development, which carries when to write the test. NOTE — not for the tooling of one language: that is lighthouse-python-standard, lighthouse-go-standard or lighthouse-shell-standard, if present.'
metadata:
  lighthouse: "core@v0.8.1"
  binding: default
  origin: own
  source: "#22"
---

# Test Standard

A test earns its place when it fails as soon as the behaviour it protects breaks. Everything
else is a tautology: it passes today, and it would pass after the bug too.

## Domain

In scope: what a good test asserts, what it fakes, and how you know it can fail. For every
language and every repository.

Not here: *when* to write the test (red first, then green) is
`superpowers:test-driven-development`. *How* in one language (the tools, the fixtures, the
runner) is the language skill: `lighthouse-python-standard` or `lighthouse-shell-standard`, if
present.

## The core question

Before every test, answer it in one sentence in the chat:

> What production change should make this test fail?

If you cannot name that change, the test protects nothing. Redesign it before you write it.

## Required checks

- **Assert an observable effect.** A return value, a file written to disk, stdout or the exit
  code, a raised error. Never private state: it changes with the implementation, not with the
  behaviour.
- **Specific fakes.** Where the arguments, the number of calls or the branch taken belong to the
  contract, the fake checks them. A fake that accepts anything proves only that the code ran.
- **One double per branch.** Success, failure and malformed input each get their own fake; a
  shared fake hides which branch ran.
- **Never mock the subject.** Mock boundaries and slow or non-deterministic collaborators (the
  network, the clock, randomness), never the code under test.
- **Investigate before you change an expectation.** A red test is information. Do not flip the
  expected value to turn it green; first decide whether the production change was intended.
- **No mirror assertions.** Do not compute the expected value with the logic under test. Use
  literal values, fixtures checked by hand, or an invariant.
- **No upstream behaviour.** Test your own boundary, not that a library does what it promises.
- **No confirmation of the current code.** No test on a bare assignment, a getter or a
  pass-through, unless it carries validation, a default, a derivation or a side effect.
- **Run shell scripts, do not read them.** Run the script against controlled input and assert its
  output, its side effects or its exit code. A test that greps the source passes on a script that
  never runs.
- **No negative-existence tests after a removal.** The removal proves itself, and whatever
  replaced it gets a behaviour test. Replace the old test; do not layer a "still gone" test on
  top.

## Contract tests and code tests

Two kinds of test, with different rules.

- **Code tests** test the behaviour of code. Every check above applies.
- **Contract tests on instruction text** (skills, agents, `AGENTS.md`) read the text and pin
  words. For an instruction file the text *is* the product, so reading it is no red flag here.
  Do not "repair" such a test into a behaviour test. A hook message is not instruction text:
  it lives in a script, so assert it from the output of a run, never from the source.

A prohibition in a contract test is fine when it guards a **lasting invariant** ("a skill names
no issue numbers"). It is not fine when it only guards that something stays gone after a
migration: that is a negative-existence test, in a contract test too.

## Classify first

Before the first edit, write one sentence in the chat, not in the commit message or the PR body:

```text
This is a <class> change; the likely regression surface is <observable behaviour>.
```

Pick the class from the table, and let its minimal check set the floor for the tests:

| Class | Regression question | Minimal check |
|---|---|---|
| `pure-logic` | Which output changes for which input? | A unit test with a literal expected value, per branch and per edge (empty, zero, malformed). |
| `io/filesystem` | Which file lands on disk differently, or not at all? | A test against a temporary directory that reads back what was written. |
| `cli-output` | What does the user see differently in stdout, stderr or the exit code? | Run the command; assert the output and the exit code. |
| `hook/gate` | What is blocked now that passed before, or the other way round? | Run the script with one input that must block and one that must pass. |
| `config` | Which behaviour switches with the setting? | One test per value, including the default and a missing key. |
| `test-only` | Which production change does the test catch now, or no longer? | The mutation check below, on the tests you touched. |

Touching more than one class? Each class brings its own minimal check; run all of them.

## Mutation check

Before you report done, mutate the production code in your head. For every realistic mutation,
at least one test must fail:

- a wrong constant, or a branch that takes the other path;
- a missing state change or side effect;
- an empty or default return;
- missing validation on empty, zero or malformed input.

If a mutation leaves every test green, the test for it is probably a tautology. Fix the test,
not the mutation list.

## Red flags

Each of these means: stop and redesign the test.

- You cannot say which production change would make it fail.
- It asserts that a mock was called, and nothing about the result, unless the call itself is the
  contract.
- The expected value comes out of the function under test, or out of a copy of its logic.
- It passed on the first run, before the implementation existed.
- You changed the expected value to make a red run green without deciding the change was meant.
- It reads the source of a script instead of running it.
- It only checks that something removed is still absent.
- It checks that a library works.
- Deleting the line under test leaves the test green.
- The expected value is built by a helper, loop or builder instead of written out.
- Setup and expectation share one object, so they are equal by construction.
- It only fails by crashing: an exception or a missing element, never a wrong value.
- It restates the code line by line, so any change to the code is a change to the test.
- It exists only for coverage: it runs the code and asserts nothing that matters.

## Inspiration

Written fresh for lighthouse, with the ideas of these two. One line is shared: the
classification sentence has the same shape as the template in forge's skill, because the issue
behind this skill asked for exactly that sentence.

- [agentsview `testing-without-tautologies`](https://github.com/kenn-io/agentsview/blob/main/.agents/skills/testing-without-tautologies/SKILL.md)
- [forge `small-change-verification`](https://github.com/kenn-io/forge/blob/main/skills/small-change-verification/SKILL.md)

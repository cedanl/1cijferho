<!-- lighthouse:begin core -->
## Lighthouse: core

- Nothing lands directly on `main`. Work on a branch in a worktree; `main-commit-gate` blocks `git commit` and `git push` to `main`.
- Escape hatch, deliberately and once: `touch .claude/allow-main` in the repo root; remove it afterwards.
- A worktree before the first write: `git worktree add .claude/worktrees/<name> -b <branch>`.
- Every change goes through a PR and is squash-merged. `/lighthouse-write-pr` writes the PR body; `pr-create-gate` checks it.
- Test gate: `commit-test-gate` runs `[test_gate] command` after each commit, on its files (default branch: before). Fix red in a new commit; never `--no-verify`.
- Artefacts (issues, PRs, comments, commit messages) are written in `[project] language`.
- With `[project] code_language` set, names, comments, docstrings, file names and everything under `code_language_paths` are English.
- Finish what you are already authorised to do; never end on an offer to do it ("ready to push if you want" is not done).
- `heading:<key>` in a skill names a contract heading in `.lighthouse/headings.toml`. Write what `python3 .lighthouse/lib/lighthouse_lib/headings.py <key>` prints, never the literal `heading:<key>`; reading accepts every language variant.

Skills:

- `/lighthouse-start-issue`: pick up an issue.
- `/lighthouse-route-issue`: read-only triage, one route per issue.
- `/lighthouse-write-issue`: write or edit issues and milestones.
- `/lighthouse-write-pr`: check the acceptance criteria, write the PR body.
- `/lighthouse-fix-pr`: make a red or conflicting PR green.
- `/lighthouse-close-issue`: close an issue with evidence, or reopen it.
- `/lighthouse-write-handoff`: the start command for the next session.
- `/lighthouse-review-spec`: a spec reviewed by another model family.
- `/lighthouse-review-plan`: a plan reviewed by another model family.
- `/lighthouse-audit-worktrees`: which worktrees and branches can go.
- `/lighthouse-place-instruction`: on which layer a new instruction belongs.
- `/lighthouse-test-standard`: tests that fail when behaviour breaks.
<!-- lighthouse:end core -->

<!-- lighthouse:begin python -->
## Lighthouse: python

- Python code follows `/lighthouse-python-standard`.
- Two hooks check it: `ruff-format-check` after every edit (it blocks on a finding), `ty-gate` when the session stops.
- Before a PR, when the repository has a `quality-baseline.toml`: `python3 .lighthouse/scripts/quality/ratchet.py ruff` and `python3 .lighthouse/scripts/quality/ratchet.py coverage`. A rising counter is red.
<!-- lighthouse:end python -->

<!-- lighthouse:begin shell -->
## Lighthouse: shell

- Every edited `.sh` or `.bash` file is shellchecked; a finding blocks. `shell-lint-check: COULD NOT CHECK` means nothing was checked, not that the file is clean.
- Before writing shell that guards something (a hook, a gate, its tests), read the skill `lighthouse-shell-standard`.
<!-- lighthouse:end shell -->

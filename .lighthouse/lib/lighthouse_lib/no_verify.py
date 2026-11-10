# lighthouse: core@v0.8.1
"""Recognise `--no-verify` / `-n` in a `git commit` command.

Moved out of a `$(cat <<'PYEOF'` heredoc in the test gate, now
commit-test-gate.sh (#884): a heredoc inside a command substitution is exactly the bash 3.2 pitfall
(macOS). As a separate file that is gone, and the check can be tested on its own.

Preventive: the test gate is a Claude Code PreToolUse hook, not a git hook,
so today the flag skips nothing. Once there is a git hook this is
the escape; cheap to close now. Quoted content (e.g. `-m "... --no-verify
..."`) does not count: the segmentation of `git_invocations.walk` makes one word of it,
and that word is then the value of `-m`, not a flag. Only the arguments of a
`git … commit` invocation are scanned (#1453).

CLI: command on stdin; exit 2 if the flag is there, otherwise 0.
"""

import os
import sys
from pathlib import Path

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib`
# imports from this installation.
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lighthouse_lib.git_invocations import Unparseable, basename, git_target, walk

# Long options that demand a value: without "=" the next token is the
# value, not a separate flag to scan (otherwise "-m --no-verify"
# would read the message value itself as a flag).
VALUE_LONG_OPTS = {
    "--message",
    "--file",
    "--reuse-message",
    "--reedit-message",
    "--template",
    "--author",
    "--date",
    "--cleanup",
    "--fixup",
    "--squash",
}
# Short options that demand a value (git commit -m/-F/-C/-c/-t). With a
# bare -m at the end of the cluster the next token is the value.
VALUE_SHORT_LETTERS = set("mFCct")
# -S and -u also take a value, but it must be attached to the letter
# (-Skeyid, -uno) — their value is optional (git falls back to a default),
# so a bare -S or -u at the end of the cluster demands NOTHING from the
# next token. They DO have to stop the rest of the cluster from being scanned,
# otherwise "-uno" reads the "n" of "no" as the -n flag (issue #591 finding 2).
ATTACHED_ONLY_SHORT_LETTERS = set("uS")


def _scan_short_cluster(letters: str) -> tuple[bool, bool]:
    """Read a cluster of short options (`-anm` → `anm`).

    Returns `(has_n, swallows_next_token)`. Scanning stops at the first
    letter that takes a value: what comes after is that value, not a flag.
    """
    for idx, ch in enumerate(letters):
        if ch == "n":
            return True, False
        if ch in VALUE_SHORT_LETTERS:
            return False, idx == len(letters) - 1
        if ch in ATTACHED_ONLY_SHORT_LETTERS:
            return False, False
    return False, False


def has_no_verify(tokens: list[str]) -> bool:
    """Is `--no-verify` or a `-n` (also in a cluster) among the tokens?"""
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok == "--":
            break
        if tok == "--no-verify":
            return True
        if tok.startswith("--"):
            name = tok.split("=", 1)[0]
            takes_value = name in VALUE_LONG_OPTS and "=" not in tok
            i += 2 if takes_value else 1
            continue
        if tok.startswith("-") and len(tok) > 1:
            found_n, consumes_next = _scan_short_cluster(tok[1:])
            if found_n:
                return True
            i += 2 if consumes_next else 1
            continue
        i += 1
    return False


def command_has_no_verify(command: str) -> bool:
    """`has_no_verify` on the arguments of every `git … commit` invocation.

    Only the words AFTER the subcommand `commit` count: a `-n` of
    `grep -n` or `git log -n` in the same command is no --no-verify
    (#1453). The segmentation is that of the test gate itself (`walk`), so
    `bash -c "…"` and `$(…)` count too, and a heredoc body does not.

    An open quote is no block here: the test gate already refuses an
    unparseable commit command earlier, through the command parser.
    """
    try:
        for step in walk(command, os.getcwd()):
            if basename(step.words[step.idx]) != "git":
                continue
            action_idx = git_target(step)[3]
            is_commit = (
                action_idx < len(step.words) and step.words[action_idx] == "commit"
            )
            if is_commit and has_no_verify(step.words[action_idx + 1 :]):
                return True
    except Unparseable:
        return False
    return False


def main() -> int:
    return 2 if command_has_no_verify(sys.stdin.read()) else 0


if __name__ == "__main__":
    sys.exit(main())

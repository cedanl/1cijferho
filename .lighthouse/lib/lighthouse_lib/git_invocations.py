# lighthouse: core@v0.8.1
"""Parser for `git commit`/`git push` inside an arbitrary shell command.

Moved out of an `IFS= read -r -d '' PARSE <<'PYEOF'` heredoc in
main-commit-gate.sh (#884): that form existed only so bash 3.2 (macOS) would
not trip over a heredoc INSIDE a command substitution. As a separate file that
reason is gone and the parser can simply be tested on its own.

Used by main-commit-gate.sh (the PreToolUse hook that blocks `git commit`/`git
push` on `main`) and, through the same `find_invocations`, by the pre-commit
test gate.
"""

# The hooks run under the host's `python3` (macOS: 3.9.6); without this
# import, `list[str] | None` in `main` is a TypeError at import time.
from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from typing import NamedTuple

UNRESOLVED = "<unknown>"
NEW_DIR = "<new>"

# Boundary markers of a sub-shell in the segment list. Identity counts, not
# content: flush_segment() always makes a fresh list, so they cannot be confused.
SUB_ENTER = ["<enter-subshell>"]
SUB_LEAVE = ["<leave-subshell>"]

# Separator `&&`: what follows runs ONLY if the previous one succeeded.
# After `;`, `|`, `||` and a newline the next one runs regardless. That
# difference matters for a `cd` to a directory that does not exist yet (F3).
AND_SEP = ["<and-sep>"]
MARKERS = (SUB_ENTER, SUB_LEAVE, AND_SEP)

# Words that may come before the actual command without being it.
# Per wrapper: (options that swallow a SEPARATE value, number of positional
# arguments before the command). `timeout 5 git commit` has one that way —
# skipping only the wrapper word let that commit through (N7).
WRAPPERS = {
    "sudo": ({"-u", "-g", "-p", "-C", "--user", "--group", "--prompt"}, 0),
    "doas": ({"-u", "-C"}, 0),
    "command": (set(), 0),
    "nice": ({"-n", "--adjustment"}, 0),
    "time": (set(), 0),
    "env": ({"-u", "--unset", "-C", "--chdir"}, 0),
    "rtk": (set(), 0),
    "exec": ({"-a"}, 0),
    "nohup": (set(), 0),
    "stdbuf": ({"-i", "-o", "-e"}, 0),
    "timeout": ({"-k", "--kill-after", "-s", "--signal"}, 1),
    "xargs": (
        {"-I", "-n", "-P", "-s", "-d", "-E", "-a", "--replace", "--max-args"},
        0,
    ),
}
# Shells that receive a command as a string: `bash -c <script>`. That
# script is a real command and is parsed separately.
SHELL_WRAPPERS = {"bash", "sh", "zsh", "dash", "ksh"}
SKIP_WORDS = {
    "{",
    "}",
    "!",
    "if",
    "then",
    "else",
    "elif",
    "fi",
    "while",
    "until",
    "do",
    "done",
    "for",
    "case",
    "esac",
}
GLOBAL_WITH_VALUE = {
    "-C",
    "-c",
    "--git-dir",
    "--work-tree",
    "--namespace",
    "--exec-path",
}
PATH_OPTIONS = {"-C", "--git-dir", "--work-tree"}


class Unparseable(Exception):
    """Open quote: no conclusion can be drawn about this command."""


def read_delimiter(line, j):
    """Read the heredoc delimiter from position j. Returns (delimiter, new j)."""
    n = len(line)
    out = []
    while j < n:
        c = line[j]
        if c == "\\":
            j += 1
            if j < n:
                out.append(line[j])
                j += 1
            continue
        if c in "'\"":
            quote = c
            j += 1
            while j < n and line[j] != quote:
                out.append(line[j])
                j += 1
            if j < n:
                j += 1
            continue
        if c.isspace() or c in ";|&<>()":
            break
        out.append(c)
        j += 1
    return "".join(out), j


def skip_ansi_c(text, j):
    """Read a `$'...'` string from j, the position AFTER the opening quote.

    Returns (content, index after the closing quote, closed). Inside ANSI-C quoting
    a backslash escapes the next character, an apostrophe too — that is exactly
    the difference with an ordinary single quote.
    """
    out = []
    n = len(text)
    while j < n:
        if text[j] == "\\" and j + 1 < n:
            out.append(text[j + 1])
            j += 2
            continue
        if text[j] == "'":
            return "".join(out), j + 1, True
        out.append(text[j])
        j += 1
    return "".join(out), j, False


def quoted_step(line, i, sq, dq):
    """One step inside `'...'` or `"..."`. Returns (i, sq, dq)."""
    c = line[i]
    if sq:
        if c == "'":
            sq = False
        return i + 1, sq, dq
    if c == "\\" and i + 1 < len(line):
        return i + 2, sq, dq
    if c == '"':
        dq = False
    return i + 1, sq, dq


def heredoc_operator(line, i, arith):
    """Read a `<<` from i. Returns (i, word_start, delimiter), "" without a delimiter."""
    if line.startswith("<<<", i):
        return i + 3, True, ""
    if arith > 0:
        # `$((1 << 2))` is a bit shift, not a heredoc.
        return i + 2, False, ""
    n = len(line)
    j = i + 2
    if j < n and line[j] == "-":
        j += 1
    while j < n and line[j] in " \t":
        j += 1
    delimiter, j = read_delimiter(line, j)
    return j, True, delimiter


def unquoted_step(line, i, arith):
    """One step outside quotes. Returns (i, arith, sq, dq, word_start)."""
    c = line[i]
    if c == "\\":
        return i + 2, arith, False, False, False
    if c == "'":
        return i + 1, arith, True, False, False
    if c == '"':
        return i + 1, arith, False, True, False
    if c == "$" and line.startswith("((", i + 1):
        return i + 3, arith + 1, False, False, False
    if c == "$" and line.startswith("'", i + 1):
        # ANSI-C quoting: in `$'...'` a backslash-escaped apostrophe does NOT
        # close the string. Without this branch the scanner read `$'it\'s'` as an
        # ordinary quote, closed it at the escaped apostrophe and left the last one
        # open (F4).
        return skip_ansi_c(line, i + 2)[1], arith, False, False, False
    if arith > 0 and line.startswith("))", i):
        return i + 2, arith - 1, False, False, False
    return i + 1, arith, False, False, c in " \t;&|()<>"


def scan_line(line, sq, dq):
    """Heredoc operators in redirection position plus the start of a `#` comment.

    Returns (delimiters, sq, dq, cut point). The cut point is the index where a
    comment begins, or len(line). Bash starts a comment only at a `#` at the START
    of a word and outside quotes: `foo#bar`, `$#` and `'a # b'` are therefore no
    comment. That distinction is exactly why this belongs here and not in a
    separate regex: without the quote state it cannot be seen, and one stray
    apostrophe in a comment would otherwise block EVERY command (N1) or a `<<` in
    a comment would swallow the rest (N2).
    """
    delimiters = []
    arith = 0
    word_start = not (sq or dq)
    i, n = 0, len(line)
    while i < n:
        if sq or dq:
            i, sq, dq = quoted_step(line, i, sq, dq)
            continue
        if line[i] == "#" and word_start:
            return delimiters, sq, dq, i
        if line.startswith("<<", i):
            i, word_start, delimiter = heredoc_operator(line, i, arith)
            if delimiter:
                delimiters.append(delimiter)
            continue
        i, arith, sq, dq, word_start = unquoted_step(line, i, arith)
    return delimiters, sq, dq, n


def strip_heredocs(command):
    """Heredoc bodies and `#` comments removed.

    An example command INSIDE a heredoc is no command — this is exactly where
    commit-test-gate.sh trips. The delimiter is really parsed (also
    `<<-`, `<<'EOF-1'`, `<<"EOF"`), and `<<` counts as a heredoc only when it is
    in redirection position — not in a string, not in `$(( ))`, not in a
    comment.

    The comment goes here because scan_line already tracks the quote state:
    one scanner, two jobs. What remains goes cleanly into the tokenizer, so an
    apostrophe in a comment no longer opens anything there.
    """
    kept = []
    queue = []
    sq = dq = False
    for line in command.split("\n"):
        if queue:
            if line.strip() == queue[0]:
                queue.pop(0)
            continue
        delimiters, sq, dq, cut = scan_line(line, sq, dq)
        kept.append(line[:cut])
        queue = delimiters
    return "\n".join(kept)


class Splitter:
    """State machine behind split_segments.

    One character per step, or one whole piece: a quote, an escape, `&&`, a
    `<>` sequence or a `$(` opener.
    """

    def __init__(self, text):
        self.text = text
        self.n = len(text)
        self.segments = []
        self.outer = []
        self.stack = []
        self.current = []
        self.buf = []
        self.dirty = False
        self.started = False
        self.tilde = False

    def flush_word(self):
        if self.started or self.buf:
            self.current.append(("".join(self.buf), not self.dirty, self.tilde))
        self.buf = []
        self.dirty = False
        self.started = False
        self.tilde = False

    def flush_segment(self):
        self.flush_word()
        self.segments.append(self.current)
        self.current = []

    def enter_sub(self, kind):
        self.outer.append(
            (self.current, self.buf, self.dirty, self.started, self.tilde)
        )
        self.current = []
        self.buf = []
        self.dirty = False
        self.started = False
        self.tilde = False
        self.stack.append(kind)
        self.segments.append(SUB_ENTER)

    def leave_sub(self, substitution=True):
        self.flush_segment()
        self.segments.append(SUB_LEAVE)
        current, buf, dirty, started, tilde = self.outer.pop()
        self.current = current
        if substitution:
            # The outcome of a substitution cannot be traced.
            self.buf = buf + ["$"]
            self.dirty = True
            self.started = True
            self.tilde = False
        else:
            # `( )` is a group, not a word: the surrounding word stays whole.
            self.buf = buf
            self.dirty = dirty
            self.started = started
            self.tilde = tilde
        self.stack.pop()

    def dq_char(self, i):
        """One character inside `"..."`. Returns the new i."""
        text, n, c = self.text, self.n, self.text[i]
        if c == "\\" and i + 1 < n and text[i + 1] in '$`"\\\n':
            if text[i + 1] != "\n":
                self.buf.append(text[i + 1])
                self.started = True
            return i + 2
        if c == '"':
            self.stack.pop()
            return i + 1
        if c == "$" and i + 1 < n and text[i + 1] == "(":
            self.enter_sub("sub")
            return i + 2
        if c == "`":
            self.enter_sub("bq")
            return i + 1
        if c == "$":
            self.dirty = True
        self.buf.append(c)
        self.started = True
        return i + 1

    def quote_char(self, i):
        """`$'`, `'`, `"` or `\\` at i. Returns the new i, or None."""
        text, n, c = self.text, self.n, self.text[i]
        if c == "$" and i + 1 < n and text[i + 1] == "'":
            body, i, closed = skip_ansi_c(text, i + 2)
            if not closed:
                raise Unparseable()
            # The escapes are not expanded here (`\n` stays `n`), so the
            # word counts as non-literal: good enough to recognise a path,
            # too uncertain to base a destination on.
            self.buf.append(body)
            self.dirty = True
            self.started = True
            return i
        if c == "'":
            end = text.find("'", i + 1)
            if end < 0:
                raise Unparseable()
            self.buf.append(text[i + 1 : end])
            self.started = True
            return end + 1
        if c == '"':
            self.stack.append("dq")
            self.started = True
            return i + 1
        if c == "\\":
            if i + 1 < n and text[i + 1] == "\n":
                # Line continuation: the two lines are one command.
                return i + 2
            if i + 1 < n:
                self.buf.append(text[i + 1])
                self.started = True
                return i + 2
            return i + 1
        return None

    def subshell_char(self, i, top):
        """`$(`, `)`, backtick or `(` at i. Returns the new i, or None."""
        text, n, c = self.text, self.n, self.text[i]
        if c == "$" and i + 1 < n and text[i + 1] == "(":
            self.enter_sub("sub")
            return i + 2
        if c == ")" and top in ("sub", "paren"):
            self.leave_sub(top == "sub")
            return i + 1
        if c == "`":
            if top == "bq":
                self.leave_sub()
            else:
                self.enter_sub("bq")
            return i + 1
        if c == "(":
            self.flush_segment()
            self.enter_sub("paren")
            return i + 1
        return None

    def separator_char(self, i):
        """Whitespace, `&&`, a segment boundary or `<>` at i. Returns the new i, or None."""
        text, n, c = self.text, self.n, self.text[i]
        if c in " \t\r":
            self.flush_word()
            return i + 1
        if c == "&" and i + 1 < n and text[i + 1] == "&":
            self.flush_segment()
            self.segments.append(AND_SEP)
            return i + 2
        if c == "\n" or c in ";&|)":
            self.flush_segment()
            return i + 1
        if c in "<>":
            self.flush_word()
            start = i
            while i < n and text[i] in "<>":
                i += 1
            self.current.append((text[start:i], True, False))
            return i
        return None

    def word_char(self, i):
        """An ordinary word character at i. Returns the new i."""
        c = self.text[i]
        if c in "$*?":
            self.dirty = True
        elif c == "~" and not self.started and not self.buf:
            self.tilde = True
        self.buf.append(c)
        self.started = True
        return i + 1

    def run(self):
        i = 0
        while i < self.n:
            top = self.stack[-1] if self.stack else None
            if top == "dq":
                i = self.dq_char(i)
                continue
            step = self.quote_char(i)
            if step is None:
                step = self.subshell_char(i, top)
            if step is None:
                step = self.separator_char(i)
            i = self.word_char(i) if step is None else step

        while self.stack:
            if self.stack[-1] == "dq":
                raise Unparseable()
            self.leave_sub(self.stack[-1] != "paren")
        self.flush_segment()
        return self.segments


def split_segments(text):
    """Split into command segments. Per word we remember how it came to be.

    A new segment begins at a newline, `;`, `&`, `|`, a sub-shell `(`,
    a command substitution `$(` or a backtick — exactly the places where a
    new command begins. Redirections only split the word.

    A substitution yields two things, and that is deliberate: its content is
    examined as a segment of its own (`echo "$(git commit ...)"` really commits),
    and the surrounding word is marked as untraceable (`git -C "$(pwd)"`
    cannot silently fall back to the cwd).

    SUB_ENTER/SUB_LEAVE go into the list around every sub-shell — `( )`, `$( )`,
    backticks. Bash does not hand the cwd of a sub-shell back to the parent;
    without those brackets a `cd` inside would linger and the hook would block a
    perfectly legitimate commit after it (N3).

    Per word there is a triple (text, literal, tilde): `tilde` says whether the
    word began with an UNquoted `~`, because only then does bash expand it.
    """
    return Splitter(text).run()


def resolve(word, literal, tilde):
    """Resolve a path word. None = cannot be resolved with certainty.

    The tilde expands only if it was UNquoted: `'~/repo'` is to bash
    an ordinary relative path that starts with a tilde (N8).
    """
    text = word if literal else os.path.expandvars(word)
    if tilde:
        text = os.path.expanduser(text)
    if any(ch in text for ch in "$`*?"):
        return None
    return text


def basename(word):
    return word.rsplit("/", 1)[-1] if "/" in word else word


def is_assignment(word):
    name, sep, _ = word.partition("=")
    if not sep or not name:
        return False
    if not (name[0].isalpha() or name[0] == "_"):
        return False
    return all(ch.isalnum() or ch == "_" for ch in name)


def command_start(words):
    """Skip assignments, wrappers and shell keywords."""
    idx = 0
    moved = True
    while moved and idx < len(words):
        moved = False
        while idx < len(words) and is_assignment(words[idx]):
            idx += 1
            moved = True
        while idx < len(words) and words[idx] in SKIP_WORDS:
            idx += 1
            moved = True
        if idx < len(words) and basename(words[idx]) in WRAPPERS:
            with_value, positional = WRAPPERS[basename(words[idx])]
            idx += 1
            while idx < len(words) and words[idx].startswith("-") and words[idx] != "-":
                if "=" not in words[idx] and words[idx] in with_value:
                    idx += 2
                else:
                    idx += 1
            for _ in range(positional):
                if idx < len(words) and not words[idx].startswith("-"):
                    idx += 1
            moved = True
    return idx


def refspec_target(arg):
    spec = arg.removeprefix("+")
    if ":" in spec:
        spec = spec.split(":", 1)[1]
    spec = spec.removeprefix("refs/heads/")
    return spec


def push_destination(words, literal, default_branch="main"):
    """Destination of a push: yes (`default_branch`), all (all branches), no,
    unresolved.

    `unresolved` is the fourth outcome and it blocks. `git push origin $BR`
    may be main and the hook cannot see it; silently letting it through would be
    the same gap that an untraceable `-C` path may not make (N4).

    The FIRST positional argument is the remote, not the destination. An
    untraceable remote says nothing about where the push goes: with
    `git push "$REMOTE" issue-1-something` the branch is simply known and is not main.
    Letting that count turned a known destination into a block (F2).
    """
    result = "no"
    positional = 0
    for k, arg in enumerate(words):
        if arg[:1] in ("<", ">"):
            break
        if arg in ("--all", "--mirror"):
            result = "all"
            continue
        if arg.startswith("-"):
            continue
        positional += 1
        if not literal[k]:
            if result == "no" and positional > 1:
                result = "unresolved"
            continue
        if refspec_target(arg) == default_branch:
            return "yes"
    return result


def existing_ancestor(path):
    """The nearest directory that exists NOW, going up from `path`. None = none."""
    current = path
    while True:
        if os.path.isdir(current):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def repo_of(path):
    """Repo root that `path` lies in, or None.

    Without calling git: walk up until a `.git` is found. A worktree
    has a file there instead of a directory, hence `exists` and not `isdir`.
    The outcome need not be exact — it only decides whether the path is put
    to git; git itself then passes the real judgement.

    One thing git already skips, and so do we: a `.git` DIRECTORY without
    `HEAD`. That is not a (broken) repo but a stray directory — on the Mac
    `~/.git/workflow-audit-tools/` — and git's discovery walks past it.
    If it counted here, every directory under `~` would become a root that git
    answers with "not a git repository". A `.git/HEAD` with garbage still IS a
    root: that is a repo that git declares broken, and that judgement must be loud.
    """
    current = path
    while True:
        dot_git = os.path.join(current, ".git")
        if os.path.isfile(dot_git) or os.path.lexists(os.path.join(dot_git, "HEAD")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def new_dirs(cwd):
    """Directory state of the shell: where we are, where we came from, the stack."""
    return {
        "here": cwd,
        "raw": cwd,
        "old": None,
        "old_raw": None,
        "old_set": False,
        "stack": [],
        "new_dir": False,
        "stale": None,
    }


def apply_cd(dirs, here, raw):
    dirs["old"], dirs["old_raw"], dirs["old_set"] = dirs["here"], dirs["raw"], True
    dirs["here"], dirs["raw"] = here, raw
    dirs["new_dir"] = False


def stack_move(verb, words, args, dirs):
    """`popd`, `cd -` and `pushd`; True if the move is done with that.

    With `pushd` WITH an argument it only puts `(here, raw)` on the stack and returns
    False: the actual move is done by `cd_to`.
    """
    if verb == "popd":
        if dirs["stack"]:
            previous, previous_raw = dirs["stack"].pop()
            apply_cd(dirs, previous, previous_raw)
        else:
            apply_cd(dirs, None, "popd")
        return True
    if args and words[args[0]] == "-":
        if dirs["old_set"]:
            apply_cd(dirs, dirs["old"], dirs["old_raw"])
        else:
            # We do not know the OLDPWD from before this command.
            apply_cd(dirs, None, "cd -")
        return True
    if verb == "pushd":
        if not args:
            # `pushd` without an argument swaps the top two: cannot be followed.
            apply_cd(dirs, None, "pushd")
            return True
        dirs["stack"].append((dirs["here"], dirs["raw"]))
    return False


def cd_to(word, literal, tilde, dirs):
    """Move to `word`: resolve, join with `here`, and the fresh directory."""
    target = resolve(word, literal, tilde)
    if target is None:
        apply_cd(dirs, None, word)
        return
    if os.path.isabs(target):
        target = os.path.normpath(target)
    elif dirs["here"] is not None:
        target = os.path.normpath(os.path.join(dirs["here"], target))
    else:
        dirs["raw"] = word
        return
    if not os.path.isdir(target):
        # The directory does NOT exist now, and that has two opposite outcomes.
        # Either the command creates it itself — `mkdir -p X && cd X`,
        # `git worktree add X && cd X`, the incantation that AGENTS.md prescribes —
        # and bash will soon be in that fresh directory. Or the `cd` fails and bash
        # stays where it is. After `&&` what follows runs only in the first case:
        # we then keep calculating WITH this not yet existing path, so that a `cd ..`
        # or `git -C ..` after it simply comes out right. Which repo it is, is decided by
        # settle_target() via the nearest existing ancestor (C1) — a fresh directory
        # UNDER a checkout is that same checkout, with that same branch. After `;`
        # the rest runs here regardless; settle_new_dir() then puts the old directory
        # back, because that is where bash really is.
        stale = (dirs["here"], dirs["raw"])
        apply_cd(dirs, target, target)
        dirs["new_dir"], dirs["stale"] = True, stale
        return
    apply_cd(dirs, target, target)


def change_dir(verb, words, literal, tildes, idx, dirs):
    """Follow `cd`, `pushd` and `popd` the way bash executes them.

    Three things the previous version missed (N6): a `cd` to a path that does not
    exist does not move bash, `cd -` jumps back to the previous directory, and
    `pushd`/`popd` move just as well as `cd`.
    """
    args = [
        k
        for k in range(idx + 1, len(words))
        if not (words[k].startswith("-") and len(words[k]) > 1)
    ]
    if stack_move(verb, words, args, dirs):
        return
    if not args:
        apply_cd(dirs, os.path.expanduser("~"), "~")
        return
    k = args[0]
    cd_to(words[k], literal[k], tildes[k], dirs)


def shell_script(words, literal, idx):
    """The literal `-c` string of a shell, or None."""
    j = idx + 1
    while j < len(words):
        word = words[j]
        if not word.startswith("-") or word == "-":
            return None
        flags = word[1:] if not word.startswith("--") else ""
        if word == "-c" or "c" in flags:
            if j + 1 < len(words) and literal[j + 1]:
                return words[j + 1]
            return None
        j += 1
    return None


def is_name(word):
    """Does this word look like a function name?"""
    if not word or word[0].isdigit():
        return False
    return all(ch.isalnum() or ch in "_-." for ch in word)


def body_end(queue):
    """Index just AFTER the `{ ... }` block with which a function body begins."""
    depth = 0
    for k, item in enumerate(queue):
        if any(item is marker for marker in MARKERS):
            continue
        for word, _, _ in item:
            if word == "{":
                depth += 1
            elif word == "}":
                depth -= 1
        if depth <= 0:
            return k + 1
    return len(queue)


def function_body(words, queue):
    """Is this a `foo() { ... }` definition? If so: isolate the body.

    The body runs only at a CALL, so a `cd` in it does not move the
    caller — following it as if it were executed blocked a perfectly
    legitimate commit after it (F5). The commands in the body DO stay
    visible, because `f() { git commit -m x; }; f` really commits; only the
    directory state is isolated, exactly as with a sub-shell.
    """
    if not (len(queue) >= 3 and queue[0] is SUB_ENTER and queue[2] is SUB_LEAVE):
        return False
    if queue[1]:
        return False
    if not is_name(words[-1]):
        return False
    if len(words) > 2 or (len(words) == 2 and words[0] != "function"):
        return False
    del queue[:3]
    queue.insert(body_end(queue), SUB_LEAVE)
    queue.insert(0, SUB_ENTER)
    return True


Invocation = tuple[str, str, str, str]


class Step(NamedTuple):
    """One segment during the `walk`, with the directory state that holds at that moment.

    `idx` is `command_start(words)`: the word where the actual command
    begins. `dirs` is the LIVE directory state — the same dict object that `walk`
    keeps internally, not a copy. A consumer that wants to keep the state of THIS
    segment copies what it needs itself before it asks the generator
    to continue.
    """

    words: list[str]
    literal: list[bool]
    tildes: list[bool]
    idx: int
    dirs: dict


def segment_columns(segment):
    """The three columns of a segment: `(words, literal, tildes)`."""
    words = [w for w, _, _ in segment]
    literal = [lit for _, lit, _ in segment]
    tildes = [til for _, _, til in segment]
    return words, literal, tildes


class WalkState:
    """The state that `walk` carries from segment to segment."""

    def __init__(self, queue, cwd):
        self.queue = queue
        self.dirs = new_dirs(cwd)
        self.saved = []
        self.after_and = False

    def take_marker(self, segment):
        """Handle `AND_SEP`/`SUB_ENTER`/`SUB_LEAVE`; True if it was a marker."""
        if segment is AND_SEP:
            self.after_and = True
            return True
        if segment is SUB_ENTER:
            self.saved.append(dict(self.dirs, stack=list(self.dirs["stack"])))
            return True
        if segment is SUB_LEAVE:
            if self.saved:
                self.dirs = self.saved.pop()
            return True
        return False

    def settle_new_dir(self):
        """Fall back to the old directory if no `&&` preceded a fresh directory."""
        if self.dirs["new_dir"] and not self.after_and:
            # No `&&` before it: this also runs if that `cd` failed, and then
            # bash is still in the old directory. See cd_to.
            self.dirs["here"], self.dirs["raw"] = self.dirs["stale"]
            self.dirs["new_dir"] = False
        self.after_and = False

    def apply_effect(self, words, literal, tildes, idx):
        """Apply the effect of a `cd`/`pushd`/`popd` or a shell (`bash -c`)."""
        verb = basename(words[idx])
        if verb in ("cd", "pushd", "popd"):
            change_dir(verb, words, literal, tildes, idx, self.dirs)
        elif verb in SHELL_WRAPPERS:
            script = shell_script(words, literal, idx)
            if script is not None:
                # A shell of its own with a cwd of its own: brackets around it.
                inner = split_segments(strip_heredocs(script))
                self.queue[:0] = [SUB_ENTER] + inner + [SUB_LEAVE]


def walk(command: str, cwd: str) -> Iterator[Step]:
    """Walk every command segment of `command`, executed from `cwd`.

    Yield one `Step` per non-empty segment with `idx < len(words)` that is not a
    function definition — also for `cd`/`pushd`/`popd` and a shell
    (`bash -c`). Only AFTER the consumer asks the generator to continue (the
    next `next()`) does `walk` apply the effect of THAT segment: a
    `cd`/`pushd`/`popd` moves `dirs`, and a shell with a literal
    `-c` string recurses into it. That way the consumer sees the directory state as it
    was WHEN the segment ran, not after.

    Saving/restoring sub-shells (`SUB_ENTER`/`SUB_LEAVE`), the `&&` fallback
    for a `cd` to a fresh directory, and isolating a function body
    all live here — that is exactly the state that has to be right
    per segment.

    Raises `Unparseable` on an open quote (see split_segments), at the
    FIRST `next()`: `split_segments` runs only once the generator starts.
    """
    state = WalkState(split_segments(strip_heredocs(command)), cwd)
    while state.queue:
        segment = state.queue.pop(0)
        if state.take_marker(segment):
            continue
        words, literal, tildes = segment_columns(segment)
        if not words:
            continue
        state.settle_new_dir()
        if function_body(words, state.queue):
            continue
        idx = command_start(words)
        if idx >= len(words):
            continue

        yield Step(words, literal, tildes, idx, state.dirs)

        state.apply_effect(words, literal, tildes, idx)


def git_target(step: Step) -> tuple[str | None, str, bool, int]:
    """The global-option loop of `git`: `-C`, `--git-dir`, `--work-tree`.

    `step.idx` points at the word `git`. Returns (target, target_raw,
    from_new_dir, action_idx) — `action_idx` is the word after the last
    global option, so `action_idx == len(step.words)` if no
    subcommand follows.
    """
    words, literal, tildes, idx, dirs = (
        step.words,
        step.literal,
        step.tildes,
        step.idx,
        step.dirs,
    )
    target, target_raw = dirs["here"], dirs["raw"]
    from_new_dir = dirs["new_dir"]
    idx += 1
    while idx < len(words) and words[idx].startswith("-"):
        name, sep, inline = words[idx].partition("=")
        raw, value_idx = None, None
        if name in PATH_OPTIONS and sep:
            raw, value_idx = inline, idx
            idx += 1
        elif name in GLOBAL_WITH_VALUE and not sep:
            if name in PATH_OPTIONS and idx + 1 < len(words):
                raw, value_idx = words[idx + 1], idx + 1
            idx += 2
        else:
            idx += 1
        if raw is None:
            continue
        resolved = resolve(raw, literal[value_idx], tildes[value_idx])
        if resolved is None:
            target, target_raw, from_new_dir = None, raw, False
        elif os.path.isabs(resolved):
            target, target_raw = os.path.normpath(resolved), raw
        elif target is not None:
            target = os.path.normpath(os.path.join(target, resolved))
            target_raw = target
        else:
            target_raw = raw
    return target, target_raw, from_new_dir, idx


def settle_target(target, from_new_dir, action, to_main, report_new_dirs):
    """Resolve a target directory that does not exist; returns `(target, keep)`.

    `keep=False` means: this invocation falls outside the protection and is
    skipped.
    """
    if target is not None and not os.path.isdir(target):
        # This directory does not exist at hook time. That does not make it
        # no-man's-land: walk up to the nearest directory that DOES exist.
        # If that one lies in a repo, the fresh directory is a subdirectory of
        # that same repo, with that same branch — `mkdir -p sub && cd sub &&
        # git commit` simply lands on main, and so that blocks (C1).
        # Only an ancestor that lies in no repo at all (`/tmp/wt` →
        # `/tmp`) is truly unprotectable.
        ancestor = existing_ancestor(target)
        if ancestor is None or repo_of(ancestor) is None:
            target, from_new_dir = None, True
        elif report_new_dirs and action == "commit":
            # Likewise deliberately only for `commit`: see the docstring of
            # find_invocations.
            target = NEW_DIR
        else:
            target = ancestor
    if target is None and from_new_dir and to_main != "yes":
        # The directory comes into being only during this command and lies in no
        # repo: there is no branch to protect here, and this keeps the prescribed
        # `git worktree add … && cd … && git commit` incantation
        # working. A push with a LITERAL main destination does not need that
        # directory knowledge and therefore always counts.
        # Deliberate asymmetry with an untraceable `-C` path: THAT path NAMES
        # an existing repo that we could not trace, and there
        # 'could not look' stays a block. Do not harmonise those two (F3).
        if report_new_dirs and action == "commit":
            target = NEW_DIR
        else:
            return target, False
    return target, True


def find_invocations(
    command: str,
    cwd: str,
    *,
    report_new_dirs: bool = False,
    default_branch: str = "main",
) -> list[Invocation]:
    """Find git commit/push invocations in `command`, executed from `cwd`.

    Returns a list of (action, directory-or-marker, to-main, raw-path) tuples.
    Raises Unparseable on an open quote (see split_segments).

    `report_new_dirs=True` changes the behaviour only for `action == "commit"`,
    in exactly the two places where a fresh directory was otherwise silently
    resolved: an existing ancestor inside a repo (`target = ancestor`) and
    a fresh directory outside every repo (which otherwise did `continue`). Both then
    yield `NEW_DIR` as directory, instead of the ancestor or nothing at all
    respectively.
    For `push` nothing changes: a push with a LITERAL
    main destination (`to_main == "yes"`) does not need that directory knowledge
    anyway, and a push without one is never the reason here that something is
    let through.
    """
    found: list[Invocation] = []
    for step in walk(command, cwd):
        words = step.words
        idx = step.idx
        if basename(words[idx]) != "git":
            continue

        target, target_raw, from_new_dir, idx = git_target(step)
        if idx >= len(words):
            continue

        action = words[idx]
        if action not in ("commit", "push"):
            continue

        literal = step.literal
        # A push with an explicit main destination counts even when it happens from a
        # feature branch: `git push origin HEAD:main`, `+main`, `--all`.
        if action == "push":
            to_main = push_destination(
                words[idx + 1 :], literal[idx + 1 :], default_branch
            )
        else:
            to_main = "no"
        target, keep = settle_target(
            target, from_new_dir, action, to_main, report_new_dirs
        )
        if not keep:
            continue
        shown = target if target is not None else UNRESOLVED
        found.append((action, shown, to_main, target_raw))
    return found


def main(argv: list[str] | None = None) -> int:
    """CLI: `git_invocations.py [--report-new-dirs] [--default-branch B] <cwd>`,
    command on stdin. `B` is the protected branch (default `main`).

    Print one tab-separated line per invocation found on stdout
    (`<action>\\t<directory-or-marker>\\t<to-main>\\t<raw-text>`). Returns 0, or 3
    on an open quote (Unparseable) — the same two outcomes as the old
    heredoc call in main-commit-gate.sh.
    """
    args = sys.argv[1:] if argv is None else argv
    report_new_dirs = False
    default_branch = "main"
    while args and args[0] in ("--report-new-dirs", "--default-branch"):
        if args[0] == "--report-new-dirs":
            report_new_dirs = True
            args = args[1:]
        else:
            default_branch = args[1]
            args = args[2:]
    cwd = args[0]
    command = sys.stdin.read()
    try:
        invocations = find_invocations(
            command, cwd, report_new_dirs=report_new_dirs, default_branch=default_branch
        )
    except Unparseable:
        return 3
    for action, shown, to_main, target_raw in invocations:
        print(f"{action}\t{shown}\t{to_main}\t{target_raw}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

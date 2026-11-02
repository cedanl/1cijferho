# lighthouse: core@v0.8.1
"""Gate of the local roborev review that `write-pr` runs before `gh pr create` (#119).

    roborev_gate.py classify [<path>] | fixed | record ... | body-line
        See `classify`, `fixed_findings`, `record` and `body_line`.

    roborev_gate.py gate
        Prints one JSON object, exit 0, with a `verdict` the skill acts on:

        off       `[review] roborev` is not `local`: do nothing.
        done      a review of this branch is recorded (`sha`).
        skip      no review, with a `reason` the PR body repeats.
        classify  raw review output exists but is not classified yet (`raw`).
        run       run the review on `range` (`lines` code lines); output goes to `raw`.

        Every verdict except `off` carries `state` and `raw`, the absolute paths of
        `<stem>.json` and `<stem>.md`, so the skill and a restart never build a path.
        Exit 3 with `COULD NOT CHECK: <reason>` on stderr: no git repo or a config error.

Why a gate: a review costs Codex quota and minutes, so a small diff or a nearly empty
quota must skip it, and the same answer must come back after a restart. The skip is
therefore written to the state file at once: a second run repeats the reason instead of
deciding again on a budget that may have moved.

The state lives in `<git-dir>/lighthouse/roborev/`, never in the work tree: the git dir
is per worktree, so two worktrees on one repo do not share a verdict, and nothing can be
committed by accident. The file stem is the branch with `/` turned into `-`.

State json has two forms, both read by `read_state`:
`{"range", "uitkomst": "done", "sha", "bevindingen": [...]}` and
`{"range", "uitkomst": "skip", "reden"}`. `range` is `<base>..<head>`: the range `gate`
printed with verdict `run` when the skill passes it to `record --range`, else the range at
the time of writing. The Dutch keys and reasons are literals: the reasons end up in the PR
body.

The size rule is that of the `Size` step of a roborev CI workflow: added plus removed lines of
`git diff --numstat --no-renames <base> HEAD`, without binary files, without `docs/`,
and without prose or data extensions (except under `.github/workflows/`).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import shutil
import subprocess
import sys
from collections.abc import Callable

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib` imports
# from this installation.
if not __package__:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib.config import ConfigError, load, lookup

THRESHOLD = 100
NON_CODE_SUFFIXES = (
    ".md", ".rst", ".txt", ".toml", ".yaml", ".yml", ".json", ".lock", ".csv", ".ini", ".cfg", ".cff",
)  # fmt: skip
CODEX_USAGE = pathlib.Path(__file__).resolve().parent / "codex_usage.py"
RANGE = re.compile(r"[0-9a-f]{7,64}\.\.[0-9a-f]{7,64}")


class GateError(Exception):
    """The gate could not check: the CLI turns it into `COULD NOT CHECK` and exit 3."""


def _git(root: pathlib.Path, *args: str, strip: bool = True) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args], capture_output=True, text=True, check=False
        )
    except OSError as exc:
        raise GateError(f"git did not run: {exc}") from exc
    if result.returncode != 0:
        raise GateError(f"git {' '.join(args)}: {result.stderr.strip() or result.returncode}")
    return result.stdout.strip() if strip else result.stdout


def state_dir(root: pathlib.Path) -> pathlib.Path:
    """`<git-dir>/lighthouse/roborev`, absolute and per worktree."""
    return pathlib.Path(_git(root, "rev-parse", "--absolute-git-dir")) / "lighthouse" / "roborev"


def branch_stem(branch: str) -> str:
    return branch.replace("/", "-")


def _counts(path: str) -> bool:
    if path.startswith("docs/"):
        return False
    if path.startswith(".github/workflows/"):
        return True
    return not path.endswith(NON_CODE_SUFFIXES)


def count_code_lines(numstat: str) -> int:
    """Code lines of `git diff --numstat --no-renames -z` output.

    `-z` keeps path names unquoted: without it git writes `"docs/caf\\303\\251.md"` and the
    `docs/` and extension rules would not see the path.
    """
    total = 0
    for line in numstat.split("\0"):
        parts = line.split("\t", 2)
        if len(parts) != 3 or parts[0] == "-" or parts[1] == "-":
            continue
        if _counts(parts[2]):
            total += int(parts[0]) + int(parts[1])
    return total


def run_budget() -> tuple[int, str]:
    """Exit code and stdout of `codex_usage.py budget` (exit 6 means `fallback`)."""
    result = subprocess.run(
        [sys.executable, str(CODEX_USAGE), "budget"], capture_output=True, text=True, check=False
    )
    return result.returncode, result.stdout


def read_state(state: pathlib.Path) -> dict | None:
    """The verdict a recorded state file stands for, or None when there is no file."""
    if not state.is_file():
        return None
    try:
        data = json.loads(state.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"{state}: unreadable: {exc}") from exc
    if not isinstance(data, dict):
        raise GateError(f"{state}: not a JSON object")
    if data.get("uitkomst") == "done":
        return {"verdict": "done", "sha": data.get("sha")}
    if data.get("uitkomst") == "skip":
        return {"verdict": "skip", "reason": data.get("reden")}
    raise GateError(f"{state}: unknown uitkomst {data.get('uitkomst')!r}")


def write_state(state: pathlib.Path, data: dict) -> None:
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def _budget_skip(budget: Callable[[], tuple[int, str]]) -> str | None:
    """The skip reason when Codex may not be used, else None."""
    code, out = budget()
    try:
        data = json.loads(out)
        decision, reason = data["decision"], data["reason"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return f"Codex-budget: onleesbaar - exit {code}"
    if code not in (0, 6):
        return f"Codex-budget: onleesbaar - exit {code}"
    if decision == "codex" and code == 0:
        return None
    return f"Codex-budget: {decision} - {reason}"


def gate(root: pathlib.Path, budget: Callable[[], tuple[int, str]] = run_budget) -> dict:
    try:
        cfg = load(root / "lighthouse.toml")
        setting = lookup(cfg, "review.roborev")
        default_branch = str(lookup(cfg, "forge.default_branch"))
    except (ConfigError, KeyError) as exc:
        raise GateError(f"config: {exc}") from exc
    if setting != "local":
        return {"verdict": "off"}

    stem = branch_stem(_git(root, "rev-parse", "--abbrev-ref", "HEAD"))
    sdir = state_dir(root)
    # The skill redirects the review into `raw`: the directory must exist before roborev runs.
    sdir.mkdir(parents=True, exist_ok=True)
    paths = {"state": str(sdir / f"{stem}.json"), "raw": str(sdir / f"{stem}.md")}

    recorded = read_state(sdir / f"{stem}.json")
    if recorded is not None:
        return {**recorded, **paths}
    if (sdir / f"{stem}.md").is_file():
        return {"verdict": "classify", **paths}

    head = _git(root, "rev-parse", "HEAD")
    base = _git(root, "merge-base", f"origin/{default_branch}", "HEAD")
    lines = count_code_lines(
        _git(root, "diff", "--numstat", "--no-renames", "-z", base, "HEAD", strip=False)
    )

    reason: str | None = None
    if not (root / ".roborev.toml").is_file():
        reason = "geen .roborev.toml"
    elif not (shutil.which("roborev") and shutil.which("codex")):
        reason = "geen roborev of codex op PATH"
    elif lines < THRESHOLD:
        reason = f"{lines} coderegels (drempel {THRESHOLD})"
    else:
        reason = _budget_skip(budget)

    if reason is not None:
        write_state(sdir / f"{stem}.json", {"range": f"{base}..{head}", "uitkomst": "skip", "reden": reason})
        return {"verdict": "skip", "reason": reason, **paths}
    return {"verdict": "run", "range": f"{base}..{head}", "lines": lines, **paths}


HEADING = re.compile(r"^## roborev: (?P<kind>.+?) \(`(?P<sha>[^`]+)`\)\s*$", re.MULTILINE)
SEVERITY = re.compile(r"^### (?:\d+\. )?(?P<severity>\S.*?)\s*$")
NUMBERED = re.compile(r"^### \d+\. ")
BULLET = re.compile(r"^- (?P<path>\S+?):(?P<line>\d+):\s*(?P<text>.*)$")
LOCATION = re.compile(r"^\*\*Location:\*\*\s*(?P<path>\S+?):(?P<line>\d+)")
PROBLEM = re.compile(r"^\*\*Problem:\*\*\s*(?P<text>.*)$")
# roborev's own log lines on stderr (`2026/11/01 15:04:29 ci review: ref=...`): never the reason.
LOG_LINE = re.compile(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ")
DONE_KINDS = ("Review Complete", "Combined Review", "Review Passed")
OUTCOMES = ("fixed", "refuted", "left")


def _findings(raw: str) -> list[dict]:
    """Findings in both forms: CI `### <Severity>` + `- <path>:<line>: <text>` bullets, and
    the local `### <n>. <Severity>` + `**Location:** <path>:<line>` + `**Problem:** <text>`."""
    found: list[dict] = []
    severity = ""
    local: dict | None = None
    for line in raw.splitlines():
        head = SEVERITY.match(line)
        if head:
            severity = head["severity"].lower()
            local = None
            if NUMBERED.match(line):
                local = {"path": None, "line": None, "severity": severity, "text": ""}
                found.append(local)
            continue
        if line.startswith("#"):
            severity, local = "", None
            continue
        if not severity:
            continue
        if local is not None:
            if m := LOCATION.match(line):
                local["path"], local["line"] = m["path"], int(m["line"])
            elif m := PROBLEM.match(line):
                local["text"] = m["text"].strip()
        elif m := BULLET.match(line):
            found.append(
                {"path": m["path"], "line": int(m["line"]), "severity": severity, "text": m["text"].strip()}
            )
        elif line.startswith("- "):
            found.append({"path": None, "line": None, "severity": severity, "text": line[2:].strip()})
    return found


def classify(raw: str) -> dict:
    """Turn raw review output into a verdict; the exit code of roborev plays no part.

    Heading: ``## roborev: <kind> (`<sha>`)``. `Review Complete`, `Combined Review` and
    `Review Passed` give `{"verdict": "done", "sha", "kind", "findings"}` (a pass has no
    findings; a complete review that parses to none is `review faalde: bevindingen onleesbaar`); any other kind, `Review Failed` and `Review Skipped` included, gives
    `{"verdict": "skip", "reason": "review faalde: <kind>"}`. No heading: the reason names the
    first non-empty line that is no roborev log line (`2026/11/01 15:04:29 ...`; the skill
    redirects stderr into the file), else the first line; 200 characters at most; empty
    output: `geen output`.

    Measured on roborev v0.69.0 (`roborev ci review --agent codex --ref <base>..<head>`, no
    `.roborev.toml`): the exit code is 0 also when the review has findings ("Agent assessment:
    Fail"); stderr holds only log lines such as `ci review: ref=... agents=[codex] ...`; the
    review, heading included, is on stdout. The sha in the heading is the head part of `--ref`
    as given (`803c1a6`, or the literal `HEAD`). It takes about 50 seconds. Without
    `.roborev.toml` the review type defaults to `security`.
    """
    match = HEADING.search(raw)
    if not match:
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        first = next((ln for ln in lines if not LOG_LINE.match(ln)), lines[0] if lines else "")
        return {"verdict": "skip", "reason": f"review faalde: {first[:200] or 'geen output'}"}
    kind = match["kind"]
    if kind not in DONE_KINDS:
        return {"verdict": "skip", "reason": f"review faalde: {kind}"}
    findings = [] if kind == "Review Passed" else _findings(raw[match.end():])
    if kind != "Review Passed" and not findings:
        return {"verdict": "skip", "reason": "review faalde: bevindingen onleesbaar"}
    return {"verdict": "done", "sha": match["sha"], "kind": kind, "findings": findings}


def _base(root: pathlib.Path) -> str:
    try:
        default_branch = str(lookup(load(root / "lighthouse.toml"), "forge.default_branch"))
    except (ConfigError, KeyError) as exc:
        raise GateError(f"config: {exc}") from exc
    return _git(root, "merge-base", f"origin/{default_branch}", "HEAD")


def fixed_findings(root: pathlib.Path) -> list[dict]:
    """Findings named by a `roborev: <path>:<line> (<severity>)` line in a commit body of `<base>..HEAD`."""
    log = _git(root, "log", "--reverse", "--format=%x01%H%n%B", f"{_base(root)}..HEAD", strip=False)
    pattern = re.compile(r"^roborev: (\S+?):(\d+) \((\w+)\)\s*$", re.MULTILINE)
    fixed: list[dict] = []
    for entry in log.split("\x01")[1:]:
        commit, _, body = entry.partition("\n")
        for m in pattern.finditer(body):
            fixed.append({"path": m[1], "line": int(m[2]), "severity": m[3], "commit": commit.strip()})
    return fixed


def _validate(decisions: list[dict], findings: list[dict] | None) -> list[dict]:
    clean = []
    for d in decisions:
        outcome, commit, reason = d.get("uitkomst"), d.get("commit"), d.get("reden")
        where = f"{d.get('path')}:{d.get('line')}"
        if outcome not in OUTCOMES:
            raise ValueError(f"{where}: uitkomst must be one of {', '.join(OUTCOMES)}, not {outcome!r}")
        if outcome == "fixed" and (not commit or reason is not None):
            raise ValueError(f"{where}: fixed needs a commit and reden null")
        if outcome != "fixed" and (not reason or commit is not None):
            raise ValueError(f"{where}: {outcome} needs a reden and commit null")
        clean.append({k: d.get(k) for k in ("path", "line", "severity", "uitkomst", "commit", "reden")})
    if findings is not None:
        want = sorted((f["path"] or "", f["line"] or 0, f["severity"]) for f in findings)
        have = sorted((d["path"] or "", d["line"] or 0, d["severity"]) for d in clean)
        if want != have:
            raise ValueError(f"decisions do not match the findings one to one: want {want}, got {have}")
    return clean


def _paths(root: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    stem = branch_stem(_git(root, "rev-parse", "--abbrev-ref", "HEAD"))
    sdir = state_dir(root)
    return sdir / f"{stem}.json", sdir / f"{stem}.md"


def record(
    root: pathlib.Path,
    *,
    skip: str | None = None,
    sha: str | None = None,
    decisions: list[dict] | None = None,
    out: pathlib.Path | None = None,
    raw: pathlib.Path | None = None,
    range_: str | None = None,
) -> pathlib.Path:
    """Write the state file: a skip with its reason, or done with one decision per finding.

    The decisions are checked one to one against the findings of a raw review: `raw` when
    given (fix-pr passes the CI comment), else the branch's local `<stem>.md`, but only
    without `out`: a record elsewhere is not about the local review. That raw review must be a
    `done` with the same sha, so the marker never claims a failed or another review. `range_` is the reviewed
    range `<base>..<head>` as `gate` printed it; without it the state holds `<base>..HEAD`
    of now, which after fix commits is no longer the reviewed range.
    """
    if (skip is None) == (sha is None):
        raise ValueError("give either skip or sha")
    if range_ is not None and not RANGE.fullmatch(range_):
        raise ValueError(f"--range must be <base>..<head> as gate printed it, not {range_!r}")
    state, local = _paths(root)
    target = out or state
    if raw is not None and not raw.is_file():
        raise ValueError(f"{raw}: no such raw review")
    if raw is None and out is None:
        raw = local
    data: dict = {"range": range_ or f"{_base(root)}..{_git(root, 'rev-parse', 'HEAD')}"}
    if skip is not None:
        data.update({"uitkomst": "skip", "reden": skip})
    else:
        findings = None
        if raw is not None and raw.is_file():
            verdict = classify(raw.read_text())
            if verdict["verdict"] != "done":
                raise ValueError(f"{raw}: no review to record as done ({verdict['reason']})")
            if verdict["sha"] != sha:
                raise ValueError(f"--done {sha} is not the reviewed sha {verdict['sha']}")
            findings = verdict["findings"]
        data.update({"uitkomst": "done", "sha": sha, "bevindingen": _validate(decisions or [], findings)})
    write_state(target, data)
    return target


def body_line(root: pathlib.Path) -> str:
    """The line the PR body carries: empty when off, never silent when `local`."""
    try:
        setting = lookup(load(root / "lighthouse.toml"), "review.roborev")
    except (ConfigError, KeyError) as exc:
        raise GateError(f"config: {exc}") from exc
    if setting != "local":
        return ""
    recorded = read_state(_paths(root)[0])
    if recorded is None:
        return "Roborev overgeslagen: geen reviewuitkomst vastgelegd"
    if recorded["verdict"] == "done":
        return f"<!-- roborev-lokaal: {recorded['sha']} -->"
    return f"Roborev overgeslagen: {recorded['reason']}"


def _run(argv: list[str]) -> str:
    parser = argparse.ArgumentParser(prog="roborev_gate.py")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("gate")
    sub.add_parser("fixed")
    sub.add_parser("body-line")
    cls = sub.add_parser("classify")
    cls.add_argument("path", nargs="?")
    rec = sub.add_parser("record")
    rec.add_argument("--skip")
    rec.add_argument("--done")
    rec.add_argument("--decisions")
    rec.add_argument("--out")
    rec.add_argument("--raw")
    rec.add_argument("--range", dest="range_")
    args = parser.parse_args(argv)
    root = pathlib.Path(_git(pathlib.Path.cwd(), "rev-parse", "--show-toplevel"))
    if args.command == "gate":
        return json.dumps(gate(root))
    if args.command == "fixed":
        return json.dumps(fixed_findings(root))
    if args.command == "body-line":
        return body_line(root)
    if args.command == "classify":
        source = pathlib.Path(args.path) if args.path else _paths(root)[1]
        try:
            result = classify(source.read_text())
        except OSError as exc:
            raise GateError(f"{source}: unreadable: {exc}") from exc
        if result["verdict"] == "skip" and not args.path:
            record(root, skip=result["reason"])
        return json.dumps(result)
    if (args.skip is None) == (args.done is None):
        raise ValueError("record needs --skip <reason> or --done <sha> --decisions <file>")
    decisions = None
    if args.done is not None:
        if not args.decisions:
            raise ValueError("--done needs --decisions <file>")
        try:
            decisions = json.loads(pathlib.Path(args.decisions).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"{args.decisions}: unreadable: {exc}") from exc
        if not isinstance(decisions, list):
            raise ValueError(f"{args.decisions}: not a JSON list")
    return str(record(root, skip=args.skip, sha=args.done, decisions=decisions,
                      out=pathlib.Path(args.out) if args.out else None,
                      raw=pathlib.Path(args.raw) if args.raw else None, range_=args.range_))


def main(argv: list[str] | None = None) -> int:
    try:
        print(_run(sys.argv[1:] if argv is None else argv))
    except GateError as exc:
        print(f"COULD NOT CHECK: {exc}", file=sys.stderr)
        return 3
    except ValueError as exc:
        print(f"roborev_gate: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

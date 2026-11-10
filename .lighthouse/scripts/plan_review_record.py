# lighthouse: core@v0.8.1
"""Attempt directory and measurement record of the adversarial plan and spec review.

Two kinds (`kind`, `"plan"` or `"spec"`) share one write path and one
record format: only the document key (`plan`/`spec`) and the matching
hash fields (`plan_sha256_*`/`spec_sha256_*`) follow `kind`. Four subcommands,
called by the operator of `review-plan` or
`review-spec`:

    plan_review_record.py start [--kind plan|spec] <DOC>
        Reserves the ephemeral attempt directory
        `${TMPDIR:-/tmp}/<kind>-review/<chain-key>/<attempt>/` and records the commit
        that round 1 reads. Prints the path. The document must be committed.
        On the host from host.toml the attempt also appears at once as `.<attempt>.tmp/` in
        the data directory: the newest attempt is authoritative, even if `write` never runs.

    plan_review_record.py write <ATTEMPT_DIR> --outcome ... --review-mode ... [...]
        Writes the round JSONs, their decisions sidecars and one `record.json` to
        `<data-dir>/<kind>-review/<chain-key>/<attempt>/`, through a staging directory
        and one rename. `kind` comes from `attempt.json` (if it is missing, it is `plan`).
        Only on the host from host.toml; elsewhere no record and a loud message.

    plan_review_record.py accept-open <ATTEMPT_DIR> --doc <DOCPATH>
        At the round limit, sets every still-open BLOCKER/MAJOR of the last round to
        `accepted_open` in `round-<last>.decisions.json` and appends a block
        `heading:open_after_review` to `<DOCPATH>` (replaces an existing block;
        every variant counts). Prints the block, in `[project] language`. Runs on any host.
        Exit 0 ok | 2 no open point | 4 read, parse or write error
        (`ACCEPT-OPEN FAILED`). An error before
        writing leaves sidecar and document untouched; if writing the
        document fails after the sidecar, the sidecar is already rewritten: running again repairs that.

    plan_review_record.py pr-open-points <DOCPATH>...
        Prints for the PR body the `heading:open_after_review` blocks of the documents
        (and of the specs that the `**Spec:**` line of a plan names) under `## Open
        review points`; no block, then nothing. A missing path is skipped.
        Runs on any host. Exit 0 ok | 4 other read or config error
        (`PR-OPEN-POINTS FAILED`).

`<chain-key>` is `PurePosixPath(docpath).stem`. The data directory comes from
`datadir.data_dir()` (decision 27), which refuses a directory inside a git worktree. The
spec and plan directories come from `paths.specs` and `paths.plans`.

Exit: 0 ok | 2 wrong usage | 3 not the host from host.toml, or host unknown/not
measurable: no record |
4 write or validation error (only the staging directory stays). On 3 and 4 the
skill carries on with the handoff: the review is the product, the record measurement material.

Since #1066 round 1 is a review and every following round is a verification
(`verify.sh`, recognisable by the top-level key `verified` in the round JSON).
`round_kinds` records that per round for the measurement; `schema_version: 1` records
do not have the field and are all review rounds.

`schema_version` goes from `2` to `3` with #988: that adds `kind` to the
record, and for round 1 `paired`/`shadow` plus per finding `source` and
`severity_by_source` — whether, and with which model, `review_pair.py merge` (outside this
file) saw a paired shadow run against Codex. v1 and v2 records lack those
fields: a reader treats a missing `kind` as `plan` and missing
pair fields as unpaired.

`schema_version` goes from `3` to `4` with #1093: `budget_checks` is a list with
per `round-<n>*.budget.json` (the budget check the runner does before each round,
also `round-1.codex.json.budget.json` of the paired round 1) one entry with
`round`, `bestand`, `besluit`, `reden`, `session_pct`, `week_pct`, `bron` and
`drempels`; a file that is not a JSON object becomes `besluit: "onleesbaar"`
with the rest `None`. The files themselves are kept next to the record. A reader
treats a missing `budget_checks` as "no check done" (v1-v3).

`schema_version` goes from `4` to `5` with the move to lighthouse: the fields
of a `budget_checks` entry follow the English budget JSON of `codex_usage.py`
(`round`, `file`, `decision`, `reason`, `session_pct`, `week_pct`, `source`,
`thresholds`), and an unreadable file becomes `decision: "unreadable"`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import secrets
import shutil
import subprocess
import sys

# Run as a file on its path: put `.lighthouse/lib` first, so `lighthouse_lib` imports
# from this installation.
if not __package__:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "lib"))

from lighthouse_lib import headings
from lighthouse_lib.config import (
    ConfigError,
    Host,
    config_path,
    host_path,
    is_local,
    load,
    load_host,
    lookup,
    repo_root,
)
from lighthouse_lib.datadir import data_dir

# An unknown or unmeasurable host is a config error of `host.toml`.
HostError = ConfigError

KINDS = ("plan", "spec")
DEFAULT_SKILL_DIRS = {
    "plan": ".claude/skills/lighthouse-review-plan",
    "spec": ".claude/skills/lighthouse-review-spec",
}
OUTCOMES = ("converged", "round_limit", "aborted")
REVIEW_MODES = ("codex", "codex_same_family", "fallback")
DECISIONS_BY_KIND = {
    "plan": ("accept", "reject", "spec", "accepted_open", "minor_yes", "minor_no"),
    "spec": ("accept", "reject", "defer", "accepted_open", "minor_yes", "minor_no"),
}
# "Justified": plan review accept/spec, spec review accept/defer (Task 4).
JUSTIFIED_BY_KIND = {"plan": ("accept", "spec"), "spec": ("accept", "defer")}
# Decisions that close a BLOCKER/MAJOR finding (build_record.still_open).
CLOSED_BY_KIND = {"plan": ("reject",), "spec": ("reject", "defer")}
ROUND_FILE = re.compile(r"^round-(\d+)\.json$")
BUDGET_FILE = re.compile(r"^round-(\d+)\..+\.budget\.json$")
# The fields of the budget JSON of `codex_usage.py budget` that the record keeps.
BUDGET_FIELDS = ("decision", "reason", "session_pct", "week_pct", "source", "thresholds")
SOURCES = ("codex", "fallback", "both")
SEVERITIES = ("BLOCKER", "MAJOR", "MINOR")
# Files of the paired round-1 experiment (#988, `review_pair.py`); `write`
# takes them along to the record directory if they exist, round 1 is the only pair round.
PAIR_FILES = (
    "pair.json",
    "round-1.codex.json",
    "round-1.fallback.json",
    "round-1.blind-decisions.json",
    "round-1.sources.json",
)


def recognize() -> Host | None:
    """The host from `host.toml` if this is that machine, otherwise `None`.

    `HostError` if `host.toml` is missing or broken, or the address is not measurable.
    """
    host = load_host(host_path())
    return host if is_local(host.tailscale_ip) else None


def _git(repo: pathlib.Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    ).stdout


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _blob_sha256(repo: pathlib.Path, sha: str, path: str) -> str:
    blob = subprocess.run(
        ["git", "-C", str(repo), "show", f"{sha}:{path}"],
        capture_output=True,
        check=True,
    ).stdout
    return _sha256(blob)


def chain_key(plan: str) -> str:
    return pathlib.PurePosixPath(plan).stem


def host_refusal(kind: str) -> str | None:
    """`None` on the recognised host (host.toml), otherwise the stderr line that refuses the measurement."""
    try:
        if recognize() is not None:
            return None
        cause = "not on the host from host.toml"
    except HostError as exc:
        cause = str(exc)
    return (
        f"{kind.upper()}-REVIEW NOT RECORDED: {cause} — this chain falls outside "
        "the measurement. The review itself is valid; carry on."
    )


def start(
    doc: str, repo: pathlib.Path, tmp_root: pathlib.Path, kind: str = "plan"
) -> pathlib.Path:
    """Reserve a new attempt directory; atomic via `mkdir`, never reused."""
    if not (repo / doc).is_file():
        raise ValueError(f"{kind} not found under the repository root: {repo / doc}")
    if _git(repo, "status", "--porcelain", "--", doc).strip():
        raise ValueError(f"{kind} has uncommitted changes, commit first: {doc}")
    base = tmp_root / f"{kind}-review" / chain_key(doc)
    base.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    while True:
        attempt = base / f"{stamp}-{secrets.token_hex(3)}"
        try:
            attempt.mkdir()
            break
        except FileExistsError:
            continue
    (attempt / "attempt.json").write_text(
        json.dumps(
            {
                "kind": kind,
                "chain_key": chain_key(doc),
                "attempt": attempt.name,
                kind: doc,
                "reviewed_sha": _git(repo, "rev-parse", "HEAD").strip(),
            },
            indent=2,
        )
    )
    return attempt


def _meta(attempt_dir: pathlib.Path) -> dict:
    """Read `attempt.json`; if `kind` is missing (v1/v2 attempt), it is `plan`."""
    meta = json.loads((attempt_dir / "attempt.json").read_text())
    meta.setdefault("kind", "plan")
    return meta


def _staging(
    data_root: pathlib.Path, kind: str, chain: str, attempt: str
) -> pathlib.Path:
    return data_root / f"{kind}-review" / chain / f".{attempt}.tmp"


def reserve(attempt_dir: pathlib.Path) -> None:
    """Put the new attempt in the data directory at once (spec section 5).

    Per chain the newest attempt is authoritative anyway, even without a record.
    Without this reservation a review that never reaches `write` would be invisible,
    and an older attempt would wrongly count. A failed reservation
    does not block the review: `write` then creates the directory after all.
    """
    try:
        meta = _meta(attempt_dir)
    except (OSError, ValueError) as exc:
        print(
            f"PLAN-REVIEW-RESERVATION FAILED: could not read attempt.json: {exc}",
            file=sys.stderr,
        )
        return
    kind = meta["kind"]
    if (reason := host_refusal(kind)) is not None:
        print(reason, file=sys.stderr)
        return
    try:
        staging = _staging(data_dir(), kind, meta["chain_key"], meta["attempt"])
        staging.mkdir(parents=True)
        shutil.copy2(attempt_dir / "attempt.json", staging / "attempt.json")
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        print(
            f"{kind.upper()}-REVIEW-RESERVATION FAILED: {exc}. The review carries on; "
            "`write` creates the staging directory after all.",
            file=sys.stderr,
        )


def _rounds(attempt_dir: pathlib.Path) -> dict[int, pathlib.Path]:
    found = {}
    for p in attempt_dir.iterdir():
        m = ROUND_FILE.match(p.name)
        if m:
            found[int(m.group(1))] = p
    return dict(sorted(found.items()))


def _budget_checks(attempt_dir: pathlib.Path) -> list[dict]:
    """One entry per budget file of the runner; unreadable or not an object -> `unreadable`."""
    checks = []
    for p in attempt_dir.iterdir():
        m = BUDGET_FILE.match(p.name)
        if not m:
            continue
        try:
            doc = json.loads(p.read_text())
        except (OSError, ValueError):
            doc = None
        if not isinstance(doc, dict):
            doc = {"decision": "unreadable"}
        checks.append(
            {
                "round": int(m.group(1)),
                "file": p.name,
                **{k: doc.get(k) for k in BUDGET_FIELDS},
            }
        )
    return sorted(checks, key=lambda c: (c["round"], c["file"]))


def _decisions(
    attempt_dir: pathlib.Path, n: int, n_findings: int, kind: str
) -> dict[str, dict]:
    """The decisions of round `n`, per `finding_id`; every ID exists and occurs once."""
    sidecar = attempt_dir / f"round-{n}.decisions.json"
    if not sidecar.is_file():
        return {}
    valid = {f"r{n}-f{i}" for i in range(1, n_findings + 1)}
    parsed = json.loads(sidecar.read_text())
    if not isinstance(parsed, list):
        # TRY004: deliberately ValueError, not TypeError — main() only catches
        # ValueError/OSError/... for exit 4 (PLAN-REVIEW-RECORD FAILED).
        raise ValueError(  # noqa: TRY004
            f"{sidecar.name}: expected a list, got {type(parsed).__name__}"
        )
    by_id: dict[str, dict] = {}
    for row in parsed:
        if not isinstance(row, dict):
            raise ValueError(  # noqa: TRY004
                f"{sidecar.name}: row is no object but {type(row).__name__}"
            )
        fid = row.get("finding_id")
        if row.get("decision") not in DECISIONS_BY_KIND[kind]:
            raise ValueError(
                f"{sidecar.name}: unknown decision {row.get('decision')!r}"
            )
        if fid not in valid:
            raise ValueError(
                f"{sidecar.name}: finding_id {fid!r} does not exist in round {n}"
            )
        if fid in by_id:
            raise ValueError(f"{sidecar.name}: finding_id {fid!r} occurs twice")
        by_id[fid] = row
    return by_id


def _sources(attempt_dir: pathlib.Path, n_findings: int) -> dict[str, dict] | None:
    """The source per round-1 finding from `round-1.sources.json`, `None` without a pair.

    `review_pair.py merge` (Task 3) writes the sidecar; if it is missing, round 1
    was not a pair (Codex limit, or simply no paired run) and this function
    returns nothing. `blind_ids` is on every row but is measurement material here for
    Task 3/4, not something this record uses itself.
    """
    sidecar = attempt_dir / "round-1.sources.json"
    if not sidecar.is_file():
        return None
    valid = {f"r1-f{i}" for i in range(1, n_findings + 1)}
    parsed = json.loads(sidecar.read_text())
    if not isinstance(parsed, list):
        raise ValueError(  # noqa: TRY004
            f"{sidecar.name}: expected a list, got {type(parsed).__name__}"
        )
    by_id: dict[str, dict] = {}
    for row in parsed:
        if not isinstance(row, dict):
            raise ValueError(  # noqa: TRY004
                f"{sidecar.name}: row is no object but {type(row).__name__}"
            )
        fid = row.get("finding_id")
        source = row.get("source")
        if source not in SOURCES:
            raise ValueError(f"{sidecar.name}: unknown source {source!r} at {fid!r}")
        severity_by_source = row.get("severity_by_source") or {}
        if not isinstance(severity_by_source, dict) or any(
            k not in ("codex", "fallback") or v not in SEVERITIES
            for k, v in severity_by_source.items()
        ):
            raise ValueError(
                f"{sidecar.name}: invalid severity_by_source at {fid!r}: "
                f"{severity_by_source!r}"
            )
        if fid not in valid:
            raise ValueError(
                f"{sidecar.name}: finding_id {fid!r} does not exist in round 1"
            )
        if fid in by_id:
            raise ValueError(f"{sidecar.name}: finding_id {fid!r} occurs twice")
        by_id[fid] = {"source": source, "severity_by_source": severity_by_source}
    missing = valid - set(by_id)
    if missing:
        raise ValueError(
            f"{sidecar.name}: misses finding_id(s) {', '.join(sorted(missing))}"
        )
    return by_id


def _build_findings(
    attempt_dir: pathlib.Path, rounds: dict[int, pathlib.Path], kind: str
) -> tuple[list[dict], list[str], dict[str, dict] | None]:
    """All findings over all rounds, plus `round_kinds` and the round-1 source.

    `sources` is `None` without `round-1.sources.json` (no pair); only round-1
    findings then get a non-`None` `source`/`severity_by_source`.
    """
    findings = []
    round_kinds = []
    sources: dict[str, dict] | None = None
    for n, path in rounds.items():
        round_doc = json.loads(path.read_text())
        round_kinds.append("verify" if "verified" in round_doc else "review")
        by_id = _decisions(attempt_dir, n, len(round_doc["findings"]), kind)
        if n == 1:
            sources = _sources(attempt_dir, len(round_doc["findings"]))
        for index, f in enumerate(round_doc["findings"], start=1):
            fid = f"r{n}-f{index}"
            row = by_id.get(fid, {})
            src_row = sources.get(fid) if sources is not None and n == 1 else None
            findings.append(
                {
                    "finding_id": fid,
                    "round": n,
                    "severity": f["severity"],
                    "cat": f.get("cat"),
                    "decision": row.get("decision"),
                    "same_as": row.get("same_as"),
                    "source": src_row["source"] if src_row else None,
                    "severity_by_source": (
                        src_row["severity_by_source"] if src_row else None
                    ),
                }
            )
    return findings, round_kinds, sources


def _check_shadow_model(
    sources: dict[str, dict] | None, shadow_model: str | None
) -> None:
    """`--shadow-model` goes exactly with a paired round 1, never alone."""
    if sources is not None and not shadow_model:
        raise ValueError(
            "--shadow-model is required: round-1.sources.json exists "
            "(round 1 is paired)"
        )
    if sources is None and shadow_model:
        raise ValueError(
            "--shadow-model belongs only to a paired round 1 "
            "(round-1.sources.json is missing)"
        )


def _codex_config(codex_home: pathlib.Path) -> dict[str, str]:
    config = codex_home / "config.toml"
    values: dict[str, str] = {}
    if config.is_file():
        for line in config.read_text().splitlines():
            m = re.match(r'^(model|model_reasoning_effort)\s*=\s*"([^"]*)"', line)
            if m:
                values[m.group(1)] = m.group(2)
    return values


def codex_version() -> str | None:
    try:
        out = subprocess.run(
            ["codex", "--version"], capture_output=True, text=True, check=False
        )
    except FileNotFoundError:
        return None
    return out.stdout.strip() or None


def build_record(
    attempt_dir: pathlib.Path,
    *,
    repo: pathlib.Path,
    outcome: str,
    review_mode: str,
    specs: list[str],
    final_sha: str,
    level: str | None,
    reviewer_model: str | None,
    skill_dir: str,
    codex_home: pathlib.Path,
    issue: dict | None,
    shadow_model: str | None = None,
) -> dict:
    meta = _meta(attempt_dir)
    kind = meta["kind"]
    doc_path, reviewed_sha = meta[kind], meta["reviewed_sha"]
    rounds = _rounds(attempt_dir)
    if not rounds:
        raise ValueError(f"no round-<n>.json in {attempt_dir}")
    missing = sorted(set(range(1, max(rounds) + 1)) - set(rounds))
    if missing:
        raise ValueError(
            f"missing round(s) in {attempt_dir}: "
            + ", ".join(str(m) for m in missing)
        )

    findings, round_kinds, sources = _build_findings(attempt_dir, rounds, kind)
    _check_shadow_model(sources, shadow_model)
    undecided = [f["finding_id"] for f in findings if f["decision"] is None]
    if undecided and outcome != "aborted":
        raise ValueError(
            f"no decision for {', '.join(undecided)}; at {outcome} every finding "
            "has one (one sidecar per round, the last one too)"
        )
    last = max(rounds)
    last_round_open = [
        {
            "finding_id": f["finding_id"],
            "severity": f["severity"],
            "decision": f["decision"],
        }
        for f in findings
        if f["round"] == last and f["severity"] in ("BLOCKER", "MAJOR")
    ]
    still_open = [
        o["finding_id"]
        for o in last_round_open
        if o["decision"] not in CLOSED_BY_KIND[kind]
    ]
    if outcome == "converged" and still_open:
        raise ValueError(
            f"converged, but {', '.join(still_open)} is still open: a fix after the "
            "last round is unreviewed (skill step 8, *Open*)"
        )
    if outcome == "round_limit":
        unaccepted = [
            o["finding_id"]
            for o in last_round_open
            if o["finding_id"] in still_open and o["decision"] != "accepted_open"
        ]
        if unaccepted:
            raise ValueError(
                f"round_limit, but {', '.join(unaccepted)} carries no accepted_open: "
                "run 'plan_review_record.py accept-open <ATTEMPT> --doc <DOC>' first"
            )

    config = _codex_config(codex_home)
    if review_mode == "fallback":
        if not reviewer_model:
            raise ValueError("--reviewer-model is required with --review-mode fallback")
        reviewer = {"model": reviewer_model, "codex_version": None, "same_family": True}
    else:
        reviewer = {
            "model": config.get("model", "unknown"),
            "codex_version": codex_version(),
            "same_family": review_mode == "codex_same_family",
        }

    skill = repo / skill_dir
    verifier_prompt = skill / "verifier-prompt.md"
    verification_schema = skill / "verification.schema.json"
    return {
        "schema_version": 5,
        "kind": kind,
        "chain_key": meta["chain_key"],
        "attempt": meta["attempt"],
        "review_mode": review_mode,
        kind: doc_path,
        "reviewed_sha": reviewed_sha,
        "final_sha": final_sha,
        f"{kind}_sha256_reviewed": _blob_sha256(repo, reviewed_sha, doc_path),
        f"{kind}_sha256_final": _blob_sha256(repo, final_sha, doc_path),
        "specs": [
            {"path": s, "sha256": _blob_sha256(repo, final_sha, s)} for s in specs
        ],
        "issue_spec": issue,
        "prompt_sha256": _sha256((skill / "reviewer-prompt.md").read_bytes()),
        "verifier_prompt_sha256": (
            _sha256(verifier_prompt.read_bytes()) if verifier_prompt.is_file() else None
        ),
        "schema_sha256": _sha256((skill / "findings.schema.json").read_bytes()),
        "verification_schema_sha256": (
            _sha256(verification_schema.read_bytes())
            if verification_schema.is_file()
            else None
        ),
        "reviewer": reviewer,
        "level": level
        or (
            None if review_mode == "fallback" else config.get("model_reasoning_effort")
        ),
        "rounds": len(rounds),
        "round_kinds": round_kinds,
        "outcome": outcome,
        "paired": sources is not None,
        "shadow": (
            {"model": shadow_model, "same_family": True}
            if sources is not None
            else None
        ),
        "budget_checks": _budget_checks(attempt_dir),
        "findings": findings,
        "last_round_open": last_round_open,
        "written_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
    }


def write(
    attempt_dir: pathlib.Path,
    record: dict,
    data_root: pathlib.Path,
    issue_body: pathlib.Path | None,
) -> pathlib.Path:
    """Fill the staging directory of `reserve`, record.json last, then one rename."""
    kind = record["kind"]
    final = data_root / f"{kind}-review" / record["chain_key"] / record["attempt"]
    staging = _staging(data_root, kind, record["chain_key"], record["attempt"])
    if final.exists():
        raise FileExistsError(f"attempt already exists in the data directory: {final}")
    staging.mkdir(parents=True, exist_ok=True)
    for path in sorted(attempt_dir.iterdir()):
        if (
            ROUND_FILE.match(path.name)
            or BUDGET_FILE.match(path.name)
            or path.name.endswith(".decisions.json")
            or path.name == "attempt.json"
            or path.name in PAIR_FILES
        ):
            shutil.copy2(path, staging / path.name)
    if issue_body is not None:
        shutil.copy2(issue_body, staging / record["issue_spec"]["file"])
    (staging / "record.json").write_text(
        json.dumps(record, indent=2, ensure_ascii=False) + "\n"
    )
    os.rename(staging, final)
    return final


# The text under the heading of the block, per `[project] language`.
OPEN_TEXT = {
    "nl": (
        "Ronde {n} haalde de rondegrens met deze punten open. "
        "Ze zijn aanvaard als `accepted_open`;",
        "de volgende fase of de PR-review weegt ze opnieuw.",
    ),
    "en": (
        "Round {n} reached the round limit with these points open. "
        "They are accepted as `accepted_open`;",
        "the next phase or the PR review weighs them again.",
    ),
}
CLOSING_KEYWORD = re.compile(
    r"(?i)\b(close[sd]?|fix(?:e[sd])?|resolve[sd]?)(:?\s+)#(\d+)"
)
FENCE = re.compile(r"^\s{0,3}(`{3,}|~{3,})")


def open_points(attempt_dir: pathlib.Path) -> list[dict]:
    """The BLOCKER/MAJOR of the last round without a closing decision, in ID order."""
    kind = _meta(attempt_dir)["kind"]
    rounds = _rounds(attempt_dir)
    if not rounds:
        raise ValueError(f"no round-<n>.json in {attempt_dir}")
    last = max(rounds)
    findings = json.loads(rounds[last].read_text())["findings"]
    by_id = _decisions(attempt_dir, last, len(findings), kind)
    points = []
    for index, f in enumerate(findings, start=1):
        fid = f"r{last}-f{index}"
        decision = by_id.get(fid, {}).get("decision")
        if (
            f["severity"] in ("BLOCKER", "MAJOR")
            and decision not in CLOSED_BY_KIND[kind]
        ):
            points.append(
                {
                    "finding_id": fid,
                    "severity": f["severity"],
                    "section": f.get("plan_section") or f.get("spec_section") or "?",
                    "problem": f.get("problem", ""),
                    "decision": decision,
                }
            )
    return points


def _accepted_rows(attempt_dir: pathlib.Path) -> tuple[int, list[dict], list[dict]]:
    """Plan of `accept_open` without writing: (round, new sidecar rows, open points after)."""
    points = open_points(attempt_dir)
    last = max(_rounds(attempt_dir))
    sidecar = attempt_dir / f"round-{last}.decisions.json"
    rows: list[dict] = json.loads(sidecar.read_text()) if sidecar.is_file() else []
    by_id = {r["finding_id"]: r for r in rows}
    for point in points:
        row = by_id.get(point["finding_id"])
        if row is None:
            rows.append(
                {
                    "finding_id": point["finding_id"],
                    "decision": "accepted_open",
                    "reason": "round_limit; without an earlier decision",
                }
            )
        elif row["decision"] != "accepted_open":
            old = f"was {row['decision']}" + (
                f": {row['reason']}" if row.get("reason") else ""
            )
            row["decision"] = "accepted_open"
            row["reason"] = f"round_limit; {old}"
    return last, rows, [{**p, "decision": "accepted_open"} for p in points]


def _write_sidecar(attempt_dir: pathlib.Path, n: int, rows: list[dict]) -> None:
    (attempt_dir / f"round-{n}.decisions.json").write_text(
        json.dumps(rows, indent=2, ensure_ascii=False) + "\n"
    )


def accept_open(attempt_dir: pathlib.Path) -> tuple[int, list[dict]]:
    """Rewrite the sidecar of the last round: every open point becomes `accepted_open`."""
    last, rows, points = _accepted_rows(attempt_dir)
    if points:
        _write_sidecar(attempt_dir, last, rows)
    return last, points


def render_open(n_round: int, points: list[dict], language: str | None = None) -> str:
    """The markdown block `heading:open_after_review` in `language` (default `[project]
    language`); closing words with `#N` are neutralised."""
    if language is None:
        language = load(config_path()).language
    first, second = OPEN_TEXT[language]
    lines = [
        headings.heading("open_after_review", language),
        "",
        first.format(n=n_round),
        second,
        "",
    ]
    for p in points:
        problem = CLOSING_KEYWORD.sub(r"\1\2issue \3", " ".join(p["problem"].split()))
        lines.append(
            f"- **{p['finding_id']}** ({p['severity']}, {p['section']}): {problem}"
        )
    return "\n".join(lines) + "\n"


def open_section(text: str) -> tuple[int, int] | None:
    """The span of the block `heading:open_after_review` (every variant), if it is the last
    `## ` section.

    Lines inside a code fence (``` or ~~~) do not count as a heading. The span runs from
    the heading to the end of `text`.
    """
    fence: str | None = None
    last_heading: tuple[int, str] | None = None
    offset = 0
    for line in text.splitlines(keepends=True):
        m = FENCE.match(line)
        if fence is not None:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence):
                fence = None
        elif m:
            fence = m.group(1)
        elif line.startswith("## "):
            last_heading = (offset, line.rstrip())
        offset += len(line)
    if last_heading is None or last_heading[1] not in headings.variants("open_after_review"):
        return None
    return last_heading[0], len(text)


def _with_open_block(text: str, block: str) -> str:
    span = open_section(text)
    if span is not None:
        return text[: span[0]] + block
    return text.rstrip() + "\n\n" + block if text.strip() else block


def upsert_open_block(doc: pathlib.Path, block: str) -> None:
    """Replace the existing block in `doc`, or append it with one blank line before it."""
    doc.write_text(_with_open_block(doc.read_text(), block))


def _plans_dir() -> str:
    """`paths.plans`, with a single trailing `/`."""
    return str(lookup(load(config_path()), "paths.plans")).strip("/") + "/"


def _spec_path() -> re.Pattern[str]:
    """A path under `paths.specs` to a `.md` file."""
    specs = str(lookup(load(config_path()), "paths.specs")).strip("/")
    return re.compile(re.escape(specs) + r"/[^\s)\]>`'\"#]+\.md")


SPEC_LINK = re.compile(r"\]\((?!https?:)([^)\s]+\.md)\)")


def _spec_docs(plan: pathlib.Path, text: str, repo: pathlib.Path) -> list[pathlib.Path]:
    """The specs that the `**Spec:**` line of a plan names and that exist under `repo`."""
    spec_line = next((r for r in text.splitlines() if r.startswith("**Spec:**")), "")
    candidates = [repo / m for m in _spec_path().findall(spec_line)]
    candidates += [plan.parent / m for m in SPEC_LINK.findall(spec_line)]
    found: list[pathlib.Path] = []
    for path in candidates:
        path = path.resolve()
        if path.is_relative_to(repo.resolve()) and path.exists() and path not in found:
            found.append(path)
    return found


def _rel(doc: pathlib.Path, repo: pathlib.Path) -> str:
    try:
        return doc.resolve().relative_to(repo.resolve()).as_posix()
    except ValueError:
        return str(doc)


def pr_open_points(docs: list[pathlib.Path], repo: pathlib.Path) -> str:
    """The blocks `heading:open_after_review` of `docs` (and of the specs of their plans)
    for the PR body.

    A missing document is skipped; any other `OSError`, a
    `UnicodeDecodeError` (not a text file) or a `ConfigError` propagates.
    No block: empty string.
    """
    plans_dir = _plans_dir()
    sections: list[str] = []
    seen: set[pathlib.Path] = set()
    queue = list(docs)
    while queue:
        doc = queue.pop(0)
        key = doc.resolve()
        if key in seen:
            continue
        seen.add(key)
        try:
            text = doc.read_text()
        except FileNotFoundError:
            continue
        span = open_section(text)
        if span is not None:
            bullets = [r for r in text[span[0] :].splitlines() if r.startswith("- ")]
            sections.append("\n".join([f"From {_rel(doc, repo)}:", *bullets]))
        if _rel(doc, repo).startswith(plans_dir):
            queue[:0] = _spec_docs(doc, text, repo)
    if not sections:
        return ""
    return "## Open review points\n\n" + "\n\n".join(sections) + "\n"


def _parse(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="plan_review_record.py")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start")
    s.add_argument("--kind", choices=KINDS, default="plan")
    s.add_argument("doc")
    a = sub.add_parser("accept-open")
    a.add_argument("attempt_dir", type=pathlib.Path)
    a.add_argument("--doc", required=True, type=pathlib.Path)
    o = sub.add_parser("pr-open-points")
    o.add_argument("docs", nargs="+", type=pathlib.Path)
    w = sub.add_parser("write")
    w.add_argument("attempt_dir", type=pathlib.Path)
    w.add_argument("--outcome", required=True, choices=OUTCOMES)
    w.add_argument("--review-mode", required=True, choices=REVIEW_MODES)
    w.add_argument("--spec", action="append", default=[])
    w.add_argument("--issue", type=int)
    w.add_argument("--issue-body", type=pathlib.Path)
    w.add_argument("--issue-fetched-at")
    w.add_argument("--final-sha")
    w.add_argument("--level")
    w.add_argument("--reviewer-model")
    w.add_argument("--shadow-model")
    w.add_argument("--skill-dir", default=None)
    args = p.parse_args(argv)
    if args.cmd == "write":
        issue_args = (args.issue, args.issue_body, args.issue_fetched_at)
        if any(a is not None for a in issue_args) and None in issue_args:
            p.error("--issue, --issue-body and --issue-fetched-at go together")
    return args


def _run_accept_open(attempt_dir: pathlib.Path, doc: pathlib.Path) -> int:
    """First read and render, then the sidecar, then the document; an error before that writes nothing."""
    try:
        last, rows, points = _accepted_rows(attempt_dir)
        if not points:
            print(
                "no open BLOCKER/MAJOR in the last round: nothing to accept",
                file=sys.stderr,
            )
            return 2
        block = render_open(last, points)
        new_text = _with_open_block(doc.read_text(), block)
        _write_sidecar(attempt_dir, last, rows)
        doc.write_text(new_text)
    except (OSError, ValueError, KeyError, ConfigError) as exc:
        print(f"ACCEPT-OPEN FAILED: {exc}", file=sys.stderr)
        return 4
    sys.stdout.write(block)
    return 0


def _run_pr_open_points(docs: list[pathlib.Path], repo: pathlib.Path) -> int:
    try:
        section = pr_open_points(docs, repo)
    except (OSError, UnicodeDecodeError, ConfigError) as exc:
        print(f"PR-OPEN-POINTS FAILED: {exc}", file=sys.stderr)
        return 4
    sys.stdout.write(section)
    return 0


def _start_attempt(args: argparse.Namespace, repo: pathlib.Path) -> int:
    tmp_root = pathlib.Path(os.environ.get("TMPDIR") or "/tmp")
    try:
        attempt = start(args.doc, repo, tmp_root, kind=args.kind)
    except ValueError as exc:
        print(f"start failed: {exc}", file=sys.stderr)
        return 2
    reserve(attempt)
    print(attempt)
    return 0


def _document_command(args: argparse.Namespace, repo: pathlib.Path) -> int:
    """The subcommands that only read or write documents: any host, no host check."""
    if args.cmd == "accept-open":
        return _run_accept_open(args.attempt_dir, args.doc)
    return _run_pr_open_points(args.docs, repo)


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)
    try:
        repo = repo_root()
    except ConfigError as exc:
        print(f"plan_review_record: {exc}", file=sys.stderr)
        return 2

    if args.cmd == "start":
        return _start_attempt(args, repo)

    if args.cmd in ("accept-open", "pr-open-points"):
        return _document_command(args, repo)

    try:
        kind = _meta(args.attempt_dir).get("kind", "plan")
    except (OSError, ValueError, KeyError):
        kind = "plan"

    if kind == "plan":
        if not args.spec and args.issue is None:
            print("give at least one --spec or --issue", file=sys.stderr)
            return 2
    elif args.spec or args.issue is not None:
        print(
            f"--spec/--issue do not belong to --kind spec: {args.attempt_dir} is a "
            "spec review, which has no separate specs or issue",
            file=sys.stderr,
        )
        return 2

    if (reason := host_refusal(kind)) is not None:
        print(reason, file=sys.stderr)
        return 3

    try:
        issue = None
        if args.issue is not None:
            issue = {
                "issue": args.issue,
                "fetched_at": args.issue_fetched_at,
                "sha256": _sha256(args.issue_body.read_bytes()),
                "file": f"issue-{args.issue}.md",
            }
        final_sha = args.final_sha or _git(repo, "rev-parse", "HEAD").strip()
        codex_home = pathlib.Path(
            os.environ.get("CODEX_SPECREVIEW_HOME")
            or pathlib.Path.home() / ".codex-specreview"
        )
        record = build_record(
            args.attempt_dir,
            repo=repo,
            outcome=args.outcome,
            review_mode=args.review_mode,
            specs=args.spec,
            final_sha=final_sha,
            level=args.level,
            reviewer_model=args.reviewer_model,
            skill_dir=args.skill_dir or DEFAULT_SKILL_DIRS[kind],
            codex_home=codex_home,
            issue=issue,
            shadow_model=args.shadow_model,
        )
        data_root = data_dir()
        final = write(args.attempt_dir, record, data_root, args.issue_body)
    except (
        OSError,
        ValueError,
        KeyError,
        subprocess.CalledProcessError,
        RuntimeError,
    ) as exc:
        print(
            f"{kind.upper()}-REVIEW-RECORD FAILED: {exc}. Only the staging directory "
            "remains; the handoff carries on.",
            file=sys.stderr,
        )
        return 4
    print(final)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

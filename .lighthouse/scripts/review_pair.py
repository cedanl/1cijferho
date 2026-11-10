# lighthouse: core@v0.8.1
"""The paired round-1 shadow run of the adversarial plan and spec review.

In round 1 a shadow reviewer from the same model family runs alongside Codex, on
the same SHA and with the same prompt. The main thread decides blind on an A/B table;
only `merge` unblinds. Two subcommands, called by the operator of
`review-plan` or `review-spec`:

    review_pair.py blind <ATTEMPT_DIR>
        First checks that the work tree has not changed since `start` (`HEAD` is
        `reviewed_sha` from `attempt.json`, `git status --porcelain` empty), then that
        `round-1.codex.json` and `round-1.fallback.json` match the
        `findings.schema.json` of the skill for `kind`. Draws once who is A
        (`pair.json`, reused afterwards) and writes and prints `round-1.blind.md`:
        the findings of both as `A-f<i>`/`B-f<i>`, without source.

    review_pair.py merge <ATTEMPT_DIR>
        Reads `pair.json`, the two raw files and the blind decisions
        `round-1.blind-decisions.json` (`[{"finding_id", "also"?, "decision",
        "reason"}]`), and writes the canonical `round-1.json` with
        `round-1.decisions.json` and `round-1.sources.json`: the sidecars that
        `plan_review_record.py write` takes along to the record. Prints the unblinded
        table.

Exit: 0 ok | 2 wrong usage | 4 `SHADOW-PAIR FAILED: <reason>`
— a changed work tree, a missing or invalid file, or blind
decisions that do not match. On 4 the subcommand writes no output file.
The review then carries on without a pair: the review is the product, the pair
measurement material.

The tally (`tally`) stays with the source repo: no skill calls it.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import secrets
import subprocess
import sys
from typing import Any


def _record_module() -> Any:
    """Load `plan_review_record` as a neighbouring file, without bending `sys.path`."""
    path = pathlib.Path(__file__).resolve().parent / "plan_review_record.py"
    spec = importlib.util.spec_from_file_location("plan_review_record", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_rec = _record_module()
REVIEWERS = ("codex", "fallback")
SEVERITY_RANK = {sev: rank for rank, sev in enumerate(_rec.SEVERITIES)}
MINOR_DECISIONS = ("minor_yes", "minor_no")
BLIND_DECISIONS = "round-1.blind-decisions.json"
JSON_TYPES = {"string": str, "array": list, "object": dict}
BLIND_HEADER = (
    "| ID | Sev | Cat | Section | Problem | Failure | Evidence | Proposed fix |\n"
    "|----|-----|-----|---------|---------|---------|----------|--------------|\n"
)
MERGE_HEADER = "| ID | Source | Sev | Decision | Reason |\n|----|--------|-----|----------|--------|\n"


class PairError(ValueError):
    """A pair that does not match; `main` turns it into exit 4."""


def _tree_state(root: pathlib.Path | None = None) -> tuple[str, bool]:
    """`HEAD` and whether the work tree is clean; untracked files count."""
    root = _rec.repo_root() if root is None else root
    head = _rec._git(root, "rev-parse", "HEAD").strip()
    status = _rec._git(root, "status", "--porcelain", "--untracked-files=normal")
    return head, not status.strip()


def _coin() -> bool:
    """True = Codex is A."""
    return secrets.randbelow(2) == 1


def _cell(text: str) -> str:
    """One table cell: a pipe or line break from reviewer text does not shift a column."""
    return (
        text.replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\r\n", " ")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _violations(obj: object, node: dict) -> list[str]:
    """What is wrong with `obj` against one object node of the schema, without `jsonschema`.

    Only what the findings schemas use: `required`, `additionalProperties:
    false`, per property `type` and `enum`, and for an array one level of `items.type`.
    Field names come from the schema.
    """
    if not isinstance(obj, dict):
        return [f"no object but {type(obj).__name__}"]
    props = node.get("properties", {})
    errors = [f"misses {key!r}" for key in node.get("required", []) if key not in obj]
    if node.get("additionalProperties") is False:
        errors += [f"unknown field {key!r}" for key in obj if key not in props]
    for key, value in obj.items():
        spec = props.get(key, {})
        want = JSON_TYPES.get(spec.get("type"))
        if want and not isinstance(value, want):
            errors.append(f"{key!r} is no {spec['type']}")
        elif "enum" in spec and value not in spec["enum"]:
            errors.append(f"{key!r} = {value!r} is not in {spec['enum']}")
        elif isinstance(value, list):
            item_type = spec.get("items", {}).get("type")
            want_item = JSON_TYPES.get(item_type)
            if want_item and not all(isinstance(v, want_item) for v in value):
                errors.append(f"{key!r} holds an item that is no {item_type}")
    return errors


def _raw_violations(doc: object, schema: dict) -> list[str]:
    errors = _violations(doc, schema)
    if errors or not isinstance(doc, dict):
        return errors
    items = schema["properties"]["findings"]["items"]
    for index, finding in enumerate(doc["findings"], start=1):
        errors += [f"finding {index}: {e}" for e in _violations(finding, items)]
    return errors


def _load_raw(attempt_dir: pathlib.Path, reviewer: str, schema: dict) -> dict:
    name = f"round-1.{reviewer}.json"
    path = attempt_dir / name
    if not path.is_file():
        raise PairError(f"{name} is missing")
    try:
        doc = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise PairError(f"{name}: {exc}") from exc
    errors = _raw_violations(doc, schema)
    if errors:
        raise PairError(
            f"{name} does not match findings.schema.json: {'; '.join(errors)}"
        )
    return doc


def _load_raws(attempt_dir: pathlib.Path, kind: str) -> dict[str, dict]:
    schema_path = _rec.repo_root() / _rec.DEFAULT_SKILL_DIRS[kind] / "findings.schema.json"
    schema = json.loads(schema_path.read_text())
    return {r: _load_raw(attempt_dir, r, schema) for r in REVIEWERS}


def _check_tree(meta: dict) -> None:
    if "reviewed_sha" not in meta:
        raise PairError("attempt.json misses reviewed_sha")
    head, clean = _tree_state()
    if head != meta["reviewed_sha"] or not clean:
        raise PairError("work tree changed since start or not clean")


def _pair(attempt_dir: pathlib.Path) -> dict[str, str]:
    """The pairing from `pair.json`; never redrawn."""
    path = attempt_dir / "pair.json"
    if not path.is_file():
        raise PairError("pair.json is missing: run `blind` first")
    pair = json.loads(path.read_text())
    if not isinstance(pair, dict) or sorted(pair.items()) not in (
        [("A", "codex"), ("B", "fallback")],
        [("A", "fallback"), ("B", "codex")],
    ):
        raise PairError("pair.json invalid: no valid A/B pairing")
    return pair


def _ensure_pair(attempt_dir: pathlib.Path) -> dict[str, str]:
    if not (attempt_dir / "pair.json").exists():
        a, b = REVIEWERS if _coin() else REVIEWERS[::-1]
        (attempt_dir / "pair.json").write_text(json.dumps({"A": a, "B": b}) + "\n")
    return _pair(attempt_dir)


def _blind_index(pair: dict[str, str], raws: dict[str, dict]) -> dict[str, tuple]:
    """Blind ID → (reviewer, position from 1, finding), in order A then B."""
    return {
        f"{letter}-f{i}": (pair[letter], i, finding)
        for letter in ("A", "B")
        for i, finding in enumerate(raws[pair[letter]]["findings"], start=1)
    }


def _blind_table(index: dict[str, tuple], kind: str) -> str:
    def order(item: tuple) -> tuple:
        blind_id, (_, i, finding) = item
        return SEVERITY_RANK[finding["severity"]], blind_id[0], i

    rows = []
    for blind_id, (_, _, f) in sorted(index.items(), key=order):
        cells = [
            blind_id,
            f["severity"],
            f.get("cat", "-"),
            f[f"{kind}_section"],
            f["problem"],
            f["failure_scenario"],
            f["evidence"],
            f["proposed_fix"],
        ]
        rows.append("| " + " | ".join(_cell(c) for c in cells) + " |\n")
    return BLIND_HEADER + "".join(rows)


def blind(attempt_dir: pathlib.Path) -> str:
    meta = _rec._meta(attempt_dir)
    _check_tree(meta)
    raws = _load_raws(attempt_dir, meta["kind"])
    pair = _ensure_pair(attempt_dir)
    table = _blind_table(_blind_index(pair, raws), meta["kind"])
    (attempt_dir / "round-1.blind.md").write_text(table)
    return table


def _load_blind_decisions(attempt_dir: pathlib.Path) -> list[dict]:
    path = attempt_dir / BLIND_DECISIONS
    if not path.is_file():
        raise PairError(f"{BLIND_DECISIONS} is missing")
    rows = json.loads(path.read_text())
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise PairError(f"{BLIND_DECISIONS}: expected a list of objects")
    return rows


def _covered(row: dict, index: dict[str, tuple]) -> list[str]:
    """The blind IDs that one decision covers; `also` points to the other letter."""
    fid, also = row.get("finding_id"), row.get("also")
    ids = [i for i in (fid, also) if i is not None]
    if fid is None or any(i not in index for i in ids):
        raise PairError(f"unknown or missing blind ID in {row!r}")
    if len(ids) == 2 and ids[0][0] == ids[1][0]:
        raise PairError(f"{fid} also {also}: `also` points to the same reviewer")
    return sorted(ids, key=lambda i: (i[0], index[i][1]))


def _check_decision(row: dict, severity: str, kind: str) -> None:
    decision, fid = row.get("decision"), row.get("finding_id")
    allowed = [d for d in _rec.DECISIONS_BY_KIND[kind] if d != "accepted_open"]
    if decision not in allowed:
        raise PairError(f"{fid}: decision {decision!r} does not belong to {kind}")
    if (severity == "MINOR") != (decision in MINOR_DECISIONS):
        raise PairError(f"{fid}: decision {decision!r} does not fit {severity}")
    if not isinstance(row.get("reason"), str):
        raise PairError(f"{fid}: `reason` is missing or no text")


def _groups(rows: list[dict], index: dict[str, tuple], kind: str) -> list[dict]:
    """One group per merged finding, in the order of `round-1.json`."""
    seen: set[str] = set()
    groups = []
    for row in rows:
        ids = _covered(row, index)
        double = seen.intersection(ids)
        if double:
            raise PairError(f"{min(double)} is covered more than once")
        seen.update(ids)
        by_source = {index[i][0]: (index[i][1], index[i][2]) for i in ids}
        severity = min(
            (f["severity"] for _, f in by_source.values()),
            key=SEVERITY_RANK.__getitem__,
        )
        _check_decision(row, severity, kind)
        groups.append(
            {"ids": ids, "by_source": by_source, "severity": severity, "row": row}
        )
    missing = [i for i in index if i not in seen]
    if missing:
        raise PairError(f"no decision for {', '.join(missing)}")
    return sorted(groups, key=_merge_order)


def _merge_order(group: dict) -> tuple[int, int]:
    """First Codex in its own order, then fallback without a Codex partner."""
    if "codex" in group["by_source"]:
        return 0, group["by_source"]["codex"][0]
    return 1, group["by_source"]["fallback"][0]


def _status(findings: list[dict]) -> str:
    severities = {f["severity"] for f in findings}
    if "BLOCKER" in severities:
        return "Blockers found"
    return "Majors only" if "MAJOR" in severities else "Clean"


def _merged_round(raws: dict[str, dict], groups: list[dict]) -> dict:
    findings = []
    for g in groups:
        source = "codex" if "codex" in g["by_source"] else "fallback"
        findings.append({**g["by_source"][source][1], "severity": g["severity"]})
    codex, fallback = raws["codex"], raws["fallback"]
    return {
        "review_markdown": (
            f"## Codex\n\n{codex['review_markdown']}\n\n"
            f"## Fallback\n\n{fallback['review_markdown']}"
        ),
        "status": _status(findings),
        "findings": findings,
        "top3": codex["top3"],
        "verdict": codex["verdict"],
        "could_not_verify": list(
            dict.fromkeys(codex["could_not_verify"] + fallback["could_not_verify"])
        ),
    }


def _source_rows(groups: list[dict]) -> list[dict]:
    return [
        {
            "finding_id": f"r1-f{n}",
            "source": "both"
            if len(g["by_source"]) == 2
            else next(iter(g["by_source"])),
            "blind_ids": g["ids"],
            "severity_by_source": {
                r: g["by_source"][r][1]["severity"]
                for r in REVIEWERS
                if r in g["by_source"]
            },
        }
        for n, g in enumerate(groups, start=1)
    ]


def _write_json(path: pathlib.Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


def merge(attempt_dir: pathlib.Path) -> str:
    kind = _rec._meta(attempt_dir)["kind"]
    pair = _pair(attempt_dir)
    raws = _load_raws(attempt_dir, kind)
    groups = _groups(_load_blind_decisions(attempt_dir), _blind_index(pair, raws), kind)
    sources = _source_rows(groups)
    decisions = [
        {
            "finding_id": s["finding_id"],
            "decision": g["row"]["decision"],
            "reason": g["row"]["reason"],
        }
        for s, g in zip(sources, groups, strict=True)
    ]
    # Build everything first, then write: if `_merged_round` fails, no sidecar is
    # left behind that `plan_review_record.py write` would mistake for a pair.
    merged = _merged_round(raws, groups)
    _write_json(attempt_dir / "round-1.decisions.json", decisions)
    _write_json(attempt_dir / "round-1.sources.json", sources)
    _write_json(attempt_dir / "round-1.json", merged)
    rows = [
        f"| {s['finding_id']} | {s['source']} | {g['severity']} | "
        f"{_cell(g['row']['decision'])} | {_cell(g['row']['reason'])} |\n"
        for s, g in zip(sources, groups, strict=True)
    ]
    return MERGE_HEADER + "".join(rows)


def _run_pair(args: argparse.Namespace) -> int:
    try:
        table = args.func(args.attempt_dir)
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.CalledProcessError,
        _rec.ConfigError,
    ) as exc:
        print(f"SHADOW-PAIR FAILED: {exc}", file=sys.stderr)
        return 4
    print(table, end="")
    return 0


def _parse(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="review_pair.py")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, func in (("blind", blind), ("merge", merge)):
        s = sub.add_parser(name)
        s.add_argument("attempt_dir", type=pathlib.Path)
        s.set_defaults(func=func, run=_run_pair)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(argv)
    return args.run(args)


if __name__ == "__main__":
    raise SystemExit(main())

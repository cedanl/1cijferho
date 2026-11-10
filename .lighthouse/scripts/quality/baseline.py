# lighthouse: python@v0.8.1
"""Loading and validating quality-baseline.toml.

Strict on purpose. A syntactically valid TOML with a typo in it is
more dangerous than a broken one: it loads, and the gate silently measures something other
than intended. `C902 = 64` instead of `C901` would set the counter for C901 to zero
and effectively disable that rule.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

# The eight zero rules. They are in pyproject.toml [tool.ruff.lint] extend-select and
# do NOT belong in the baseline: zero is a limit, not a level.
ZERO_RULES: tuple[str, ...] = (
    "E722",
    "B006",
    "F403",
    "F811",
    "B023",
    "PT012",
    "PGH003",
    "PGH004",
)

# The seven counters. [ruff] must contain exactly these seven — no more, no
# fewer, because a missing key is a disabled rule.
COUNTED_RULES: tuple[str, ...] = (
    "C901",
    "TRY400",
    "F401",
    "F841",
    "S110",
    "S112",
    "PT011",
)

# The one counter in [complexipy]: the number of functions above cognitive 15.
COMPLEXIPY_KEY = "cognitive_over_15"


class BaselineError(ValueError):
    """The baseline is unusable. Fail closed: the gate goes red."""


@dataclass(frozen=True)
class Baseline:
    counts: dict[str, int]
    min_diff_pct: int
    # The measured global src coverage, pinned as a floor. Unlike
    # min_diff_pct (policy per PR) this is a level: it comes from a measurement and
    # creeps up as soon as new code lifts the coverage.
    min_total_pct: int
    exclude: tuple[str, ...]
    # The number of functions with cognitive complexity > 15 (complexipy).
    # Last field, with a default, so existing Baseline(...) constructions
    # keep working; load() does require the section in the file.
    cognitive_over_15: int = 0


def load(path: Path, repo_root: Path) -> Baseline:
    """Read and validate the baseline, or raise BaselineError with the reason in it."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise BaselineError(f"{path} is missing") from exc
    except OSError as exc:
        raise BaselineError(f"{path} is unreadable: {exc}") from exc
    try:
        raw = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise BaselineError(f"{path} is unreadable: {exc}") from exc

    unknown = sorted(set(raw) - {"ruff", "coverage", "complexipy"})
    if unknown:
        raise BaselineError(f"unknown sections in {path.name}: {unknown}")
    for section in ("ruff", "coverage", "complexipy"):
        if section not in raw:
            raise BaselineError(f"section [{section}] is missing in {path.name}")

    coverage = _coverage_section(raw["coverage"])
    return Baseline(
        counts=_counts(raw["ruff"]),
        min_diff_pct=_percentage(coverage, "min_diff_pct"),
        min_total_pct=_percentage(coverage, "min_total_pct"),
        exclude=_exclude(coverage, repo_root),
        cognitive_over_15=_complexipy_counter(raw["complexipy"]),
    )


def _counts(section: object) -> dict[str, int]:
    if not isinstance(section, dict):
        raise BaselineError("[ruff] is not a table")
    keys = set(section)

    baselined_zero = sorted(keys & set(ZERO_RULES))
    if baselined_zero:
        raise BaselineError(
            f"zero rules do not belong in the baseline: {baselined_zero} — "
            "zero is a limit, not a level, so there is no bump possible"
        )
    # extra before missing: on a typo (C902 instead of C901) the message must
    # name both the unknown name and the expected name. If missing were checked
    # first, the absent C901 would be reported without ever
    # showing that C902 is the (wrong) cause.
    extra = sorted(keys - set(COUNTED_RULES))
    if extra:
        raise BaselineError(
            f"[ruff] names unknown rules: {extra} — expected exactly {list(COUNTED_RULES)}"
        )
    missing = sorted(set(COUNTED_RULES) - keys)
    if missing:
        raise BaselineError(f"[ruff] lacks counter rules: {missing}")

    counts: dict[str, int] = {}
    for rule in COUNTED_RULES:
        value = section[rule]
        # type() and not isinstance(): bool inherits from int, so `C901 = true` would
        # pass an isinstance check and then count as counter 1.
        if type(value) is not int:
            raise BaselineError(f"{rule} is not an integer: {value!r}")
        if value < 0:
            raise BaselineError(f"{rule} is negative: {value}")
        counts[rule] = value
    return counts


def _complexipy_counter(section: object) -> int:
    """The one counter of [complexipy]: `cognitive_over_15`, an integer >= 0.

    Required, like the ruff counters: a missing key would be a
    disabled counter.
    """
    if not isinstance(section, dict):
        raise BaselineError("[complexipy] is not a table")
    extra = sorted(set(section) - {COMPLEXIPY_KEY})
    if extra:
        raise BaselineError(
            f"[complexipy] names unknown keys: {extra} — expected exactly "
            f"[{COMPLEXIPY_KEY!r}]"
        )
    if COMPLEXIPY_KEY not in section:
        raise BaselineError(f"[complexipy] lacks {COMPLEXIPY_KEY}")
    value = section[COMPLEXIPY_KEY]
    # type() and not isinstance(): bool inherits from int.
    if type(value) is not int:
        raise BaselineError(f"{COMPLEXIPY_KEY} is not an integer: {value!r}")
    if value < 0:
        raise BaselineError(f"{COMPLEXIPY_KEY} is negative: {value}")
    return value


def _coverage_section(section: object) -> dict:
    if not isinstance(section, dict):
        raise BaselineError("[coverage] is not a table")
    unknown = sorted(set(section) - {"min_diff_pct", "min_total_pct", "exclude"})
    if unknown:
        raise BaselineError(f"unknown keys in [coverage]: {unknown}")
    return section


def _percentage(section: dict, key: str) -> int:
    """A required integer percentage in [0, 100].

    Required and not defaulted: a missing key would become a disabled
    floor, and that is exactly the silent pass this gate must not have
    anywhere.
    """
    if key not in section:
        raise BaselineError(f"coverage.{key} is missing")
    value = section[key]
    # type() and not isinstance(): bool inherits from int.
    if type(value) is not int:
        raise BaselineError(f"coverage.{key} is not an integer: {value!r}")
    if not 0 <= value <= 100:
        raise BaselineError(f"coverage.{key} is outside [0, 100]: {value}")
    return value


def _exclude(section: dict, repo_root: Path) -> tuple[str, ...]:
    value = section.get("exclude", [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise BaselineError(f"coverage.exclude is not a list of strings: {value!r}")
    for prefix in value:
        if not (repo_root / prefix).exists():
            raise BaselineError(
                f"coverage.exclude points to nothing: {prefix} — an exclusion "
                "that matches nothing silently does nothing"
            )
    return tuple(value)

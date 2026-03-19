# -----------------------------------------------------------------------------
# Organization: CEDA
# License: MIT
# -----------------------------------------------------------------------------
"""
Validates that column values in a converted CSV match the allowed values
defined in the bestandsbeschrijving (variable_metadata.json).
"""

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple

import polars as pl


def _to_snake(name: str) -> str:
    """snake_case conversion matching converter_headers.normalize_name exactly."""
    # Strip accents the same way as converter_headers.strip_accents
    normalized = unicodedata.normalize("NFKD", name)
    s = normalized.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _build_lookup(variables: List[Dict[str, Any]]) -> Dict[str, set]:
    """
    Build a dict mapping normalised column name -> set of allowed string values.
    Only includes variables that have an exhaustive key-value `values` spec.

    Skips:
    - Fields with 'reference' or 'list' values (non-concrete)
    - Fields where any value description contains ' > ' — DUO notation for
      "value X for case Y > value Z for all other cases", meaning the list is
      not exhaustive and we cannot reliably validate against it.
    """
    lookup: Dict[str, set] = {}
    for var in variables:
        values = var.get("values", {})
        if not values:
            continue
        # Skip non-concrete value specs
        if "reference" in values or "list" in values:
            continue
        # Skip non-exhaustive fields: DUO uses ' > ' in descriptions to indicate
        # conditional/open-ended documentation (e.g. "0000 voor overige inschrijvingen")
        if any(" > " in str(v) for v in values.values()):
            continue
        # Skip open-ended fields: [gevuld] means "any non-empty value is valid"
        if any(re.match(r"^\[gevuld\]$", str(k).strip(), re.IGNORECASE) for k in values.keys()):
            continue
        # Skip range fields: " t/m " in keys or value descriptions indicates a numeric range
        if any(" t/m " in str(k) for k in values.keys()):
            continue
        if any(" t/m " in str(v) for v in values.values()):
            continue
        # Skip fields that reference an external file for remaining values
        if any("Zie bestand" in str(v) for v in values.values()):
            continue

        allowed_clean = set()
        for k in values.keys():
            key = str(k).strip()
            # Handle compound keys like "[leeg] of 0" — split and add each part
            if re.search(r"\[leeg\]", key, re.IGNORECASE) and " of " in key:
                for part in key.split(" of "):
                    part = part.strip()
                    if re.match(r"^\[.*\]$", part):
                        allowed_clean.add("")
                    else:
                        allowed_clean.add(part)
                        # Also add zero-stripped version for numeric values
                        if part.isdigit():
                            allowed_clean.add(part.lstrip("0") or "0")
            elif re.match(r"^\[.*\]$", key):
                allowed_clean.add("")
            else:
                allowed_clean.add(key)
                # Also add zero-stripped version so "01" matches "1" in data
                if key.isdigit():
                    allowed_clean.add(key.lstrip("0") or "0")

        norm_name = _to_snake(var["name"])
        lookup[norm_name] = allowed_clean
    return lookup


def validate_column_values(
    data_file_path: str | Path,
    variable_metadata_path: str | Path,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate column values in a CSV file against allowed values from metadata.

    Args:
        data_file_path: Path to a converted CSV file (semicolon-separated, UTF-8).
        variable_metadata_path: Path to variable_metadata.json.

    Returns:
        Tuple of (success, results) where results contains per-column outcomes.
    """
    results: Dict[str, Any] = {
        "columns_checked": 0,
        "columns_failed": 0,
        "column_results": [],
        "total_issues": 0,
    }

    # Load metadata
    try:
        with open(variable_metadata_path, encoding="utf-8") as f:
            variables = json.load(f)
    except Exception as e:
        results["load_error"] = str(e)
        results["total_issues"] += 1
        return False, results

    lookup = _build_lookup(variables)
    if not lookup:
        results["warning"] = "No columns with concrete allowed values found in metadata."
        return True, results

    # Load CSV
    try:
        df = pl.read_csv(data_file_path, separator=";", encoding="utf-8", infer_schema_length=0)
    except Exception as e:
        results["load_error"] = str(e)
        results["total_issues"] += 1
        return False, results

    # Normalise CSV column names for matching
    csv_cols = {_to_snake(c): c for c in df.columns}

    for norm_name, allowed in lookup.items():
        original_col = csv_cols.get(norm_name)
        if original_col is None:
            continue  # column not in this file, skip silently

        results["columns_checked"] += 1

        # Get unique values, cast to string, strip whitespace
        unique_vals = (
            df.select(pl.col(original_col).cast(pl.Utf8).str.strip_chars())
            .unique()
            .to_series()
            .to_list()
        )
        # Treat null as empty string
        unique_vals_str = {v if v is not None else "" for v in unique_vals}

        invalid = unique_vals_str - allowed
        # Remove empty string from invalid if [leeg]/empty is already in allowed or
        # if there are null-derived empties not relevant to validation
        # (Only flag truly unexpected values)
        invalid_reported = {v for v in invalid if v != "" or "" not in allowed}

        col_result: Dict[str, Any] = {
            "column": original_col,
            "allowed_values": sorted(allowed),
            "invalid_values": sorted(invalid_reported),
            "status": "ok" if not invalid_reported else "failed",
        }

        if invalid_reported:
            results["columns_failed"] += 1
            results["total_issues"] += 1

        results["column_results"].append(col_result)

    success = results["total_issues"] == 0
    return success, results


def validate_column_values_folder(
    output_dir: str | Path,
    variable_metadata_path: str | Path,
) -> Dict[str, Any]:
    """
    Run validate_column_values on all non-decoded, non-enriched CSV files in output_dir.

    Returns a summary dict keyed by filename.
    """
    output_dir = Path(output_dir)
    summary: Dict[str, Any] = {}

    csv_files = [
        f for f in output_dir.glob("*.csv")
        if not f.name.endswith("_decoded.csv") and not f.name.endswith("_enriched.csv")
    ]

    for csv_file in csv_files:
        success, results = validate_column_values(csv_file, variable_metadata_path)
        summary[csv_file.name] = {"success": success, "results": results}

    return summary


def read_value_validation_log(log_path: str | Path) -> List[Dict[str, Any]]:
    """
    Read (5b)_value_validation_log_latest.json and return a flat list of failing columns.

    Returns list of dicts with keys: file, column, invalid_values.
    Returns empty list if log does not exist or has no failures.
    """
    log_path = Path(log_path)
    if not log_path.exists():
        return []
    with open(log_path, encoding="utf-8") as f:
        data = json.load(f)
    failures = []
    for fname, file_results in data.get("details", {}).items():
        for col in file_results.get("column_results", []):
            if col.get("status") == "failed":
                failures.append({
                    "file": fname,
                    "column": col["column"],
                    "invalid_values": col["invalid_values"],
                })
    return failures

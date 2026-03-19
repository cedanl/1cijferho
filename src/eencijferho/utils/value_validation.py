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
from pathlib import Path
from typing import Any, Dict, List, Tuple

import polars as pl


def _to_snake(name: str) -> str:
    """Rough snake_case conversion matching converter_headers logic."""
    s = name.lower().strip()
    s = re.sub(r"[\s\-/]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _build_lookup(variables: List[Dict[str, Any]]) -> Dict[str, set]:
    """
    Build a dict mapping normalised column name -> set of allowed string values.
    Only includes variables that have concrete key-value `values` (not references/lists).
    """
    lookup: Dict[str, set] = {}
    for var in variables:
        values = var.get("values", {})
        if not values:
            continue
        # Skip non-concrete value specs
        if "reference" in values or "list" in values:
            continue
        allowed = {str(k).strip() for k in values.keys()}
        # Also allow empty/blank as some fields have "[leeg]" noted in description
        # We keep only the normalised key without brackets
        allowed_clean = set()
        for v in allowed:
            if v.lower().startswith("[") and v.lower().endswith("]"):
                allowed_clean.add("")  # empty string represents [leeg]
            else:
                allowed_clean.add(v)
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

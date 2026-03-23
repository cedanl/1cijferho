import json
import os
import re as _re
from typing import Optional


_PLACEHOLDER_KEYS = frozenset({"[leeg]", "[gevuld]"})


def _has_real_mappings(values: dict) -> bool:
    """Return True when values contains at least one non-placeholder code→label pair."""
    return any(str(k).strip() not in _PLACEHOLDER_KEYS for k in values)


def get_available_decode_columns(dec_metadata_json_path: str) -> list[str]:
    """Return all column names that can be decoded via Dec_* lookup tables.

    Reads the decoding_variables from each table in the Dec bestandsbeschrijving
    JSON produced by the extract step.  Returns an empty list when the file is
    absent or unreadable.

    Args:
        dec_metadata_json_path: Path to Bestandsbeschrijving_Dec-bestanden*.json.

    Returns:
        Sorted list of unique column names available for Dec decoding.
    """
    if not dec_metadata_json_path or not os.path.exists(dec_metadata_json_path):
        return []
    try:
        with open(dec_metadata_json_path, encoding="utf-8") as f:
            meta = json.load(f)
        seen: set[str] = set()
        for table in meta.get("tables", []):
            for var in table.get("decoding_variables", []):
                if var and var not in seen:
                    seen.add(var)
        return sorted(seen)
    except Exception:
        return []


def get_decode_column_info(dec_metadata_json_path: str) -> dict[str, list[str]]:
    """Return what label columns each decodable column adds.

    Returns a dict mapping each decoding variable name to the list of label
    column names that the corresponding Dec table adds.  Used in the UI to
    show users what they get when they select a column.

    Returns an empty dict when the file is absent or unreadable.
    """
    if not dec_metadata_json_path or not os.path.exists(dec_metadata_json_path):
        return {}
    try:
        with open(dec_metadata_json_path, encoding="utf-8") as f:
            meta = json.load(f)

        def _parse_col_name(row: str) -> str:
            """Extract column name from a content row like 'Naam land   5   40'."""
            return _re.sub(r"\s+\d+\s+\d+\s*$", "", row).strip()

        result: dict[str, list[str]] = {}
        for table in meta.get("tables", []):
            content = table.get("content", [])
            dec_vars = table.get("decoding_variables", [])
            if not dec_vars and len(content) > 1:
                dec_vars = [_parse_col_name(content[1])]
            # content[0] = header, content[1] = code col, content[2:] = label cols
            label_cols = [_parse_col_name(row) for row in content[2:] if row.strip()]
            for var in dec_vars:
                if var:
                    result.setdefault(var, label_cols)
        return result
    except Exception:
        return {}


def get_enrich_variable_info(variable_metadata_json_path: str) -> dict[str, dict[str, str]]:
    """Return a sample of code→label mappings for each enrichable variable.

    Returns a dict mapping variable name to a dict of up to 3 code→label
    pairs as an example.  Used in the UI to show users what substitution
    will be applied.

    Returns an empty dict when the file is absent or unreadable.
    """
    if not variable_metadata_json_path or not os.path.exists(variable_metadata_json_path):
        return {}
    try:
        with open(variable_metadata_json_path, encoding="utf-8") as f:
            items = json.load(f)
        result: dict[str, dict[str, str]] = {}
        for item in items:
            name = item.get("name")
            values = item.get("values") or {}
            if not name or not isinstance(values, dict):
                continue
            if not _has_real_mappings(values):
                continue
            real_items = [(k, v) for k, v in values.items() if str(k).strip() not in _PLACEHOLDER_KEYS]
            sample = {str(k): str(v) for k, v in real_items[:3]}
            result[name] = sample
        return result
    except Exception:
        return {}


def get_available_enrich_variables(variable_metadata_json_path: str) -> list[str]:
    """Return all variable names available for label enrichment.

    Reads the variable names from variable_metadata.json produced by the
    extract step.  Variables that only have placeholder keys (``[leeg]``,
    ``[gevuld]``) are excluded — these carry no usable code→label substitution.
    Returns an empty list when the file is absent or unreadable.

    Args:
        variable_metadata_json_path: Path to variable_metadata.json.

    Returns:
        Sorted list of unique variable names available for enrichment.
    """
    if not variable_metadata_json_path or not os.path.exists(variable_metadata_json_path):
        return []
    try:
        with open(variable_metadata_json_path, encoding="utf-8") as f:
            items = json.load(f)
        return sorted(
            item["name"]
            for item in items
            if item.get("name") and _has_real_mappings(item.get("values") or {})
        )
    except Exception:
        return []

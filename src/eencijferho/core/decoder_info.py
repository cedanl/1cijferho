import re as _re

from eencijferho.io.decorators import with_storage


_PLACEHOLDER_KEYS = frozenset({"[leeg]", "[gevuld]"})


def _has_real_mappings(values: dict) -> bool:
    """Return True when values contains at least one non-placeholder code→label pair."""
    return any(str(k).strip() not in _PLACEHOLDER_KEYS for k in values)


def _vakken_decode_fields(storage, dec_meta: dict, vakken_metadata_json_path: str | None) -> dict[str, list[str]]:
    """Vakken fields with a decode rule -> label columns of their Dec table.

    Returns an empty dict when no Vakken JSON is given. Read errors propagate
    to the caller, which treats them like an unreadable Dec JSON.
    """
    if not vakken_metadata_json_path or not storage.exists(vakken_metadata_json_path):
        return {}
    from eencijferho.core.decoder import (
        _parse_layout_rows,
        build_dec_layouts,
        parse_vakken_layout,
    )
    vakken_meta = storage.read_json(vakken_metadata_json_path)
    dec_layouts = build_dec_layouts(dec_meta, vakken_meta)
    result: dict[str, list[str]] = {}
    for table in vakken_meta.get("tables", []):
        content = table.get("content", [])
        if not content or "opmerking" not in content[0].lower():
            continue
        for field in parse_vakken_layout(content):
            if not field["dec_table"]:
                continue
            dec_rows = _parse_layout_rows(dec_layouts.get(field["dec_table"], []))
            n_keys = 2 if field["composite"] else 1
            result.setdefault(field["name"], [r["name"] for r in dec_rows[n_keys:]])
    return result


@with_storage
def get_available_decode_columns(
    storage,
    dec_metadata_json_path: str,
    vakken_metadata_json_path: str | None = None,
) -> list[str]:
    """Return all column names that can be decoded via Dec_* lookup tables.

    Reads the decoding_variables from each table in the Dec bestandsbeschrijving
    JSON produced by the extract step.  Returns an empty list when the file is
    absent or unreadable.

    Args:
        dec_metadata_json_path: Path to Bestandsbeschrijving_Dec-bestanden*.json.
        vakken_metadata_json_path: Optional path to the Vakkenbestanden JSON;
            its fields with a decode rule (e.g. Vakcode) are added.

    Returns:
        Sorted list of unique column names available for Dec decoding.
    """
    if not dec_metadata_json_path or not storage.exists(dec_metadata_json_path):
        return []
    try:
        meta = storage.read_json(dec_metadata_json_path)
        seen: set[str] = set()
        for table in meta.get("tables", []):
            for var in table.get("decoding_variables", []):
                if var and var not in seen:
                    seen.add(var)
        seen.update(_vakken_decode_fields(storage, meta, vakken_metadata_json_path))
        return sorted(seen)
    except Exception:
        return []


@with_storage
def get_decode_column_info(
    storage,
    dec_metadata_json_path: str,
    vakken_metadata_json_path: str | None = None,
) -> dict[str, list[str]]:
    """Return what label columns each decodable column adds.

    Returns a dict mapping each decoding variable name to the list of label
    column names that the corresponding Dec table adds.  Used in the UI to
    show users what they get when they select a column. With
    ``vakken_metadata_json_path`` the Vakkenbestanden fields are included.

    Returns an empty dict when the file is absent or unreadable.
    """
    if not dec_metadata_json_path or not storage.exists(dec_metadata_json_path):
        return {}
    try:
        meta = storage.read_json(dec_metadata_json_path)

        def _parse_col_name(row: str) -> str:
            """Extract column name from a content row like 'Naam land   5   40'."""
            return _re.sub(r"\s+\d+\s+\d+$", "", row).strip()

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
        for var, label_cols in _vakken_decode_fields(storage, meta, vakken_metadata_json_path).items():
            result.setdefault(var, label_cols)
        return result
    except Exception:
        return {}


@with_storage
def get_enrich_variable_info(storage, variable_metadata_json_path: str) -> dict[str, dict[str, str]]:
    """Return a sample of code→label mappings for each enrichable variable.

    Returns a dict mapping variable name to a dict of up to 3 code→label
    pairs as an example.  Used in the UI to show users what substitution
    will be applied.

    Returns an empty dict when the file is absent or unreadable.
    """
    if not variable_metadata_json_path or not storage.exists(variable_metadata_json_path):
        return {}
    try:
        items = storage.read_json(variable_metadata_json_path)
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


@with_storage
def get_available_enrich_variables(storage, variable_metadata_json_path: str) -> list[str]:
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
    if not variable_metadata_json_path or not storage.exists(variable_metadata_json_path):
        return []
    try:
        items = storage.read_json(variable_metadata_json_path)
        return sorted(
            item["name"]
            for item in items
            if item.get("name") and _has_real_mappings(item.get("values") or {})
        )
    except Exception:
        return []

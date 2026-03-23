import difflib
import json
import os
import re as _re

import polars as pl

from eencijferho.utils.converter_headers import clean_header_name, normalize_name
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Public: load helpers
# ---------------------------------------------------------------------------


def load_variable_mappings(
    variable_metadata_path: Optional[str] = None,
    naming_func: Optional[Callable[[str], str]] = None,
) -> dict[str, dict[str, Any]]:
    """
    Loads variable-level value mappings from a variable metadata JSON file.

    Args:
        variable_metadata_path: Path to variable_metadata.json. Falls back to
            the default location inside data/00-metadata/json/ when None.
        naming_func: Optional column name normalizer.

    Returns:
        Mapping of normalized variable names to ``{"orig_name": ..., "mapping": {...}}``.
        Returns an empty dict when the file is absent or unreadable.
    """
    candidates = []
    if variable_metadata_path:
        candidates.append(variable_metadata_path)
    candidates.append(
        os.path.join(os.getcwd(), "data", "00-metadata", "json", "variable_metadata.json")
    )
    path = next((p for p in candidates if os.path.exists(p)), None)
    if path is None:
        return {}

    try:
        from eencijferho.utils.sanitize_variable_metadata import sanitize_variable_metadata_json
        sanitize_variable_metadata_json(path)
    except Exception as e:
        print(f"[decoder] Waarschuwing: kon variable metadata niet opschonen {path}: {e}")

    try:
        with open(path, encoding="utf-8") as f:
            items = json.load(f)
    except Exception as e:
        print(f"[decoder] Waarschuwing: kon variable metadata niet laden {path}: {e}")
        return {}

    maps: dict[str, dict[str, Any]] = {}
    for item in items:
        name = item.get("name")
        values = item.get("values") or {}
        if not name or not isinstance(values, dict):
            continue
        norm = normalize_name(name, naming_func)
        entry: dict[str, Any] = {}
        for k, v in values.items():
            key = k.strip() if isinstance(k, str) else str(k)
            entry[key] = v
            if isinstance(key, str) and key.upper() != key:
                entry[key.upper()] = v
            if isinstance(key, str) and key.isdigit():
                entry[key.zfill(2)] = v
                try:
                    entry[int(key)] = v
                except Exception:
                    pass
            else:
                try:
                    int_key = int(key)
                    if int_key not in entry:
                        entry[int_key] = v
                except Exception:
                    pass
        if entry:
            maps[norm] = {"orig_name": name, "mapping": entry}

    if maps:
        print(f"[decoder] {len(maps)} variabele-mappings geladen uit {path}")
    return maps


def load_dec_tables_from_metadata(
    metadata_json_path: str,
    dec_output_dir: str,
    naming_func: Optional[Callable[[str], str]] = None,
) -> dict[str, pl.DataFrame]:
    """
    Loads Dec_* tables as Polars DataFrames based on metadata JSON.

    Args:
        metadata_json_path: Path to the metadata JSON file.
        dec_output_dir: Directory containing Dec_* CSV files.
        naming_func: Optional column name normalizer.

    Returns:
        Mapping of table titles to DataFrames. Missing files are skipped.

    Example:
        >>> dec_tables = load_dec_tables_from_metadata(
        ...     "data/00-metadata/json/Bestandsbeschrijving_Dec-bestanden.json",
        ...     "data/02-output/DEMO",
        ... )
    """
    with open(metadata_json_path, encoding="utf-8") as f:
        meta = json.load(f)

    dec_tables: dict[str, pl.DataFrame] = {}
    for table in meta["tables"]:
        dec_file = table["table_title"].replace(".asc", ".csv")
        dec_path = os.path.join(dec_output_dir, dec_file)

        schema_overrides: dict[str, Any] = {}
        content = table.get("content", [])
        if len(content) >= 2:
            code_col = content[1].split("  ")[0].strip()
            schema_overrides[code_col] = pl.String
            if len(content) > 2:
                code_col2 = content[2].split("  ")[0].strip()
                schema_overrides[code_col2] = pl.String

        try:
            if schema_overrides:
                df = pl.read_csv(
                    dec_path,
                    separator=";",
                    encoding="utf8",
                    schema_overrides=schema_overrides,
                )
            else:
                df = pl.read_csv(dec_path, separator=";", encoding="utf8")
            dec_tables[table["table_title"]] = df
        except Exception:
            try:
                if schema_overrides:
                    df = pl.read_csv(
                        dec_path,
                        separator=";",
                        encoding="utf8",
                        quote_char=None,
                        schema_overrides=schema_overrides,
                    )
                else:
                    df = pl.read_csv(dec_path, separator=";", encoding="utf8", quote_char=None)
                dec_tables[table["table_title"]] = df
            except Exception as e:
                print(f"[decoder] Kon {dec_file} niet laden: {e}")

    return dec_tables


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _load_meta(metadata_json_path: str) -> dict:
    with open(metadata_json_path, encoding="utf-8") as f:
        return json.load(f)


def _normalize_df(
    df: pl.DataFrame,
    naming_func: Optional[Callable] = None,
) -> tuple[pl.DataFrame, dict[str, str], list[str]]:
    """Normalize column names and cast all values to stripped strings.

    Returns:
        (norm_df, norm_map, orig_columns)
        norm_map maps normalized_name → original_name.
    """
    from eencijferho.utils.converter_headers import strip_accents

    orig_columns = list(df.columns)
    norm_map = {normalize_name(col, naming_func): col for col in orig_columns}
    norm_df = df.rename({v: k for k, v in norm_map.items()})
    for col in norm_df.columns:
        norm_df = norm_df.with_columns(
            pl.col(col).cast(pl.Utf8).str.strip_chars().alias(col)
        )
    return norm_df, norm_map, orig_columns


def _normalize_dec_table(
    dec_table: pl.DataFrame,
    code_col_norm: str,
    naming_func: Optional[Callable] = None,
) -> pl.DataFrame:
    """Normalize Dec table column names and strip leading zeros from the code column."""
    from eencijferho.utils.converter_headers import strip_accents

    join_df = dec_table.rename(
        {c: normalize_name(strip_accents(c), naming_func) for c in dec_table.columns}
    )
    if code_col_norm in join_df.columns:
        join_df = join_df.with_columns(
            pl.col(code_col_norm)
            .cast(pl.Utf8)
            .str.strip_chars_start("0")
            .str.replace("^$", "0")
            .str.strip_chars()
            .alias(code_col_norm)
        )
    return join_df


def _apply_single_dec_join(
    result_df: pl.DataFrame,
    join_df: pl.DataFrame,
    var_norm: str,
    code_col_norm: str,
    is_composite: bool,
    code_col2_norm: Optional[str],
    naming_func: Optional[Callable] = None,
) -> pl.DataFrame:
    """Apply one Dec table join (simple or composite key) to result_df."""
    from eencijferho.utils.converter_headers import strip_accents

    result_df = result_df.with_columns(
        pl.col(var_norm)
        .cast(pl.Utf8)
        .str.strip_chars_start("0")
        .str.replace("^$", "0")
        .str.strip_chars()
        .alias(var_norm)
    )
    try:
        if is_composite:
            dec_cols = [
                c for c in join_df.columns
                if c != code_col_norm and c != code_col2_norm
            ]
            joined = result_df.join(
                join_df,
                left_on=[code_col_norm, var_norm],
                right_on=[code_col_norm, code_col2_norm],
                how="left",
            )
        else:
            dec_cols = [c for c in join_df.columns if c != code_col_norm]
            joined = result_df.join(
                join_df, left_on=var_norm, right_on=code_col_norm, how="left"
            )

        for col in dec_cols:
            new_col = f"{var_norm}__{normalize_name(strip_accents(col), naming_func)}"
            result_df = result_df.with_columns(joined[col].alias(new_col))

        if dec_cols:
            unmatched = joined.filter(
                pl.col(var_norm).is_not_null() & pl.col(dec_cols[0]).is_null()
            )
            if unmatched.height > 0:
                sample = unmatched[var_norm].unique().to_list()[:5]
                print(f"[decoder] Niet-gematchte codes voor {var_norm}: {sample}")
    except Exception as e:
        print(f"[decoder] Fout bij Dec-join voor {var_norm}: {e}")

    return result_df


def _apply_dec_tables(
    result_df: pl.DataFrame,
    meta: dict,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Optional[Callable] = None,
    decode_columns: Optional[list[str]] = None,
) -> pl.DataFrame:
    """Apply all Dec table joins defined in metadata to result_df.

    Fixes a previous bug where an outer loop shadowed itself, causing each
    table's join to be applied N times instead of once.

    Args:
        decode_columns: When not None, only decode variables whose name
            appears in this list (matched after normalization).
    """
    from eencijferho.utils.converter_headers import strip_accents

    allowed: Optional[set[str]] = None
    if decode_columns is not None:
        allowed = {normalize_name(strip_accents(c), naming_func) for c in decode_columns}

    for table in meta["tables"]:
        dec_vars = table.get("decoding_variables", [])
        dec_table = dec_tables.get(table["table_title"])
        content = table.get("content", [])

        # Fallback: derive decoding variable from first content row
        if not dec_vars and len(content) > 1:
            dec_vars = [content[1].split("  ")[0].strip()]

        if not dec_vars or dec_table is None or len(content) < 2:
            continue

        if allowed is not None:
            dec_vars = [v for v in dec_vars if normalize_name(strip_accents(v), naming_func) in allowed]
        if not dec_vars:
            continue

        code_col_raw = content[1].split("  ")[0].strip()
        code_col_norm = normalize_name(strip_accents(code_col_raw), naming_func)
        join_df = _normalize_dec_table(dec_table, code_col_norm, naming_func)

        # Special column-name fallbacks for known Dec tables
        title_lower = table["table_title"].lower()
        if title_lower.startswith("dec_landcode") and code_col_norm not in join_df.columns:
            if "code_land" in join_df.columns:
                code_col_norm = "code_land"
            else:
                continue
        if title_lower.startswith("dec_nationaliteitscode") and code_col_norm not in join_df.columns:
            if "code_nationaliteit" in join_df.columns:
                code_col_norm = "code_nationaliteit"
            else:
                continue

        # Detect composite key: second content row carries "}" marker
        is_composite = False
        code_col2_norm = None
        if len(content) > 2 and "}" in content[2]:
            code_col2_raw = content[2].split("  ")[0].strip()
            code_col2_norm = normalize_name(strip_accents(code_col2_raw), naming_func)
            if (
                code_col2_norm in join_df.columns
                and code_col_norm in join_df.columns
                and code_col_norm in result_df.columns
            ):
                is_composite = True
                join_df = join_df.with_columns(
                    pl.col(code_col2_norm)
                    .cast(pl.Utf8)
                    .str.strip_chars_start("0")
                    .str.replace("^$", "0")
                    .str.strip_chars()
                    .alias(code_col2_norm)
                )

        for var in dec_vars:
            var_norm = normalize_name(strip_accents(var), naming_func)
            if var_norm not in result_df.columns:
                closest = difflib.get_close_matches(var_norm, result_df.columns, n=1, cutoff=0.8)
                if closest:
                    var_norm = closest[0]
                else:
                    print(f"[decoder] Sla '{var}' over — niet gevonden in DataFrame.")
                    continue

            result_df = _apply_single_dec_join(
                result_df, join_df, var_norm, code_col_norm,
                is_composite, code_col2_norm, naming_func,
            )

    return result_df


def _apply_vakken_patch(
    result_df: pl.DataFrame,
    meta: dict,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Optional[Callable] = None,
) -> pl.DataFrame:
    """Apply Vakkenbestanden decoding via 'te decoderen met Dec_X' in Opmerking column."""
    for table in meta["tables"]:
        if table.get("decoding_variables", []):
            continue  # Already handled by _apply_dec_tables

        content = table.get("content", [])
        if not content or len(content) < 2:
            continue

        header_row = next(
            (i for i, row in enumerate(content) if "opmerking" in row.lower()), None
        )
        if header_row is None:
            continue

        headers = [h.strip().lower() for h in content[header_row].split()]
        col_idx = {h: i for i, h in enumerate(headers)}

        for row in content[header_row + 1:]:
            parts = row.split(None, len(headers) - 1)
            if len(parts) < len(headers):
                continue

            naam = parts[col_idx.get("naam", 0)]
            opm = parts[col_idx.get("opmerking", -1)] if "opmerking" in col_idx else ""

            composite, dec_table_title = _parse_vakken_opmerking(opm)
            if not dec_table_title:
                continue

            dec_table = dec_tables.get(dec_table_title)
            if dec_table is None:
                print(f"[decoder][vakken] Dec-tabel niet geladen: {dec_table_title}")
                continue

            dec_content = next(
                (t.get("content", []) for t in meta["tables"] if t["table_title"] == dec_table_title),
                None,
            )
            if not dec_content or len(dec_content) < 2:
                continue

            dec_code_col_norm = normalize_name(dec_content[1].split()[0], naming_func)
            join_df = dec_table.rename(
                {c: normalize_name(c, naming_func) for c in dec_table.columns}
            )
            var_norm = normalize_name(naam, naming_func)

            if composite:
                result_df = _apply_vakken_composite(
                    result_df, join_df, var_norm, composite,
                    dec_code_col_norm, dec_content, dec_table_title, naming_func,
                )
            else:
                result_df = _apply_vakken_simple(
                    result_df, join_df, var_norm, dec_code_col_norm, naam, dec_table_title,
                )

    return result_df


def _parse_vakken_opmerking(opm: str) -> tuple[Optional[str], Optional[str]]:
    """Parse an Opmerking value for decode instructions.

    Returns (composite_col, dec_table_title) or (None, None) if no instruction found.
    """
    opm_lower = opm.lower()
    if "in combinatie met" in opm_lower and "te decoderen met dec_" in opm_lower:
        m = _re.search(
            r"in combinatie met ([A-Za-z0-9_]+) te decoderen met (Dec_[A-Za-z0-9_]+)\.asc",
            opm, _re.IGNORECASE,
        )
        if m:
            return m.group(1), m.group(2) + ".asc"
    elif "te decoderen met dec_" in opm_lower:
        m = _re.search(r"te decoderen met (Dec_[A-Za-z0-9_]+)\.asc", opm)
        if m:
            return None, m.group(1) + ".asc"
    return None, None


def _apply_vakken_simple(
    result_df: pl.DataFrame,
    join_df: pl.DataFrame,
    var_norm: str,
    dec_code_col_norm: str,
    naam: str,
    dec_table_title: str,
) -> pl.DataFrame:
    """Apply a simple (single-key) Vakkenbestanden join."""
    if var_norm not in result_df.columns:
        closest = difflib.get_close_matches(var_norm, result_df.columns, n=1)
        print(f"[decoder][vakken] Sla '{naam}' over — niet in DataFrame. Dichtste: {closest}")
        return result_df

    result_df = result_df.with_columns(
        pl.col(var_norm).cast(pl.Utf8).str.strip_chars().alias(var_norm)
    )
    if dec_code_col_norm in join_df.columns:
        join_df = join_df.with_columns(
            pl.col(dec_code_col_norm).cast(pl.Utf8).str.strip_chars().alias(dec_code_col_norm)
        )
    try:
        dec_cols = [c for c in join_df.columns if c != dec_code_col_norm]
        joined = result_df.join(
            join_df, left_on=var_norm, right_on=dec_code_col_norm, how="left"
        )
        for col in dec_cols:
            result_df = result_df.with_columns(joined[col].alias(f"{var_norm}__{col}"))
        if dec_cols:
            unmatched = joined.filter(
                pl.col(var_norm).is_not_null() & pl.col(dec_cols[0]).is_null()
            )
            if unmatched.height > 0:
                print(f"[decoder][vakken] Niet-gematcht voor {naam}: {unmatched[var_norm].unique().to_list()[:5]}")
    except Exception as e:
        print(f"[decoder][vakken] Fout bij join voor {naam}: {e}")
    return result_df


def _apply_vakken_composite(
    result_df: pl.DataFrame,
    join_df: pl.DataFrame,
    var_norm: str,
    composite: str,
    dec_code_col_norm: str,
    dec_content: list,
    dec_table_title: str,
    naming_func: Optional[Callable] = None,
) -> pl.DataFrame:
    """Apply a composite-key Vakkenbestanden join."""
    composite_norm = normalize_name(composite, naming_func)
    if len(dec_content) <= 2:
        print(f"[decoder][vakken] Geen tweede sleutelkolom voor samengestelde join in {dec_table_title}")
        return result_df

    dec_code_col2_norm = normalize_name(dec_content[2].split()[0], naming_func)

    for col in [var_norm, composite_norm]:
        if col not in result_df.columns:
            closest = difflib.get_close_matches(col, result_df.columns, n=1)
            print(f"[decoder][vakken] Sla {col} over (samengesteld) — niet in DataFrame. Dichtste: {closest}")
            return result_df
        result_df = result_df.with_columns(
            pl.col(col).cast(pl.Utf8).str.zfill(2).str.strip_chars().alias(col)
        )
    for col in [dec_code_col_norm, dec_code_col2_norm]:
        if col in join_df.columns:
            join_df = join_df.with_columns(
                pl.col(col).cast(pl.Utf8).str.zfill(2).str.strip_chars().alias(col)
            )
    try:
        dec_cols = [
            c for c in join_df.columns
            if c not in (dec_code_col_norm, dec_code_col2_norm)
        ]
        joined = result_df.join(
            join_df,
            left_on=[var_norm, composite_norm],
            right_on=[dec_code_col_norm, dec_code_col2_norm],
            how="left",
        )
        for col in dec_cols:
            result_df = result_df.with_columns(joined[col].alias(f"{var_norm}__{col}"))
        if dec_cols:
            unmatched = joined.filter(
                pl.col(var_norm).is_not_null()
                & pl.col(composite_norm).is_not_null()
                & pl.col(dec_cols[0]).is_null()
            )
            if unmatched.height > 0:
                pairs = list(zip(
                    unmatched[var_norm].unique().to_list()[:5],
                    unmatched[composite_norm].unique().to_list()[:5],
                ))
                print(f"[decoder][vakken] Niet-gematchte samengestelde codes: {pairs}")
    except Exception as e:
        print(f"[decoder][vakken] Fout bij samengestelde join voor {var_norm}: {e}")
    return result_df


def _apply_variable_mappings(
    result_df: pl.DataFrame,
    variable_metadata_path: Optional[str],
    naming_func: Optional[Callable],
    norm_map: dict[str, str],
    orig_columns: list[str],
    enrich_variables: Optional[list[str]] = None,
) -> pl.DataFrame:
    """Apply variable-level code→label mappings from variable_metadata.json.

    When variable_metadata_path is None or the file is absent, returns result_df unchanged.
    This is the step that differentiates decode_fields from decode_fields_dec_only.

    Args:
        enrich_variables: When not None, only apply mappings for variables
            whose name appears in this list (matched after normalization).
    """
    allowed_enrich: Optional[set[str]] = None
    if enrich_variables is not None:
        allowed_enrich = {normalize_name(v, naming_func) for v in enrich_variables}

    try:
        var_maps = load_variable_mappings(variable_metadata_path, naming_func=naming_func)
        if not var_maps:
            return result_df
        if allowed_enrich is not None:
            var_maps = {k: v for k, v in var_maps.items() if k in allowed_enrich}
        if not var_maps:
            return result_df
        print(f"[decoder] {len(var_maps)} variabele-mappings toepassen...")
        for var_norm, info in var_maps.items():
            mapping = info.get("mapping") if isinstance(info, dict) else info
            orig_name = info.get("orig_name") if isinstance(info, dict) else None
            chosen_col = _resolve_mapping_column(
                var_norm, orig_name, result_df, norm_map, orig_columns, naming_func
            )
            if chosen_col is None:
                continue
            result_df = _apply_single_mapping(result_df, chosen_col, mapping, var_norm, orig_name)
    except Exception as e:
        print(f"[decoder] Fout bij toepassen variabele-mappings: {e}")
    return result_df


def _resolve_mapping_column(
    var_norm: str,
    orig_name: Optional[str],
    result_df: pl.DataFrame,
    norm_map: dict[str, str],
    orig_columns: list[str],
    naming_func: Optional[Callable],
) -> Optional[str]:
    """Find the best matching column in result_df for a variable mapping."""
    if var_norm in result_df.columns:
        return var_norm

    candidates = list(result_df.columns) + list(norm_map.keys())
    for orig in orig_columns:
        try:
            candidates.append(normalize_name(clean_header_name(orig), naming_func))
        except Exception:
            pass

    closest = difflib.get_close_matches(var_norm, candidates, n=3)
    if not closest:
        print(f"[decoder] Mapping voor '{var_norm}' (orig: '{orig_name}') — kolom niet gevonden.")
        return None

    pick = next((c for c in closest if c in result_df.columns), None)
    if pick is None and closest[0] in norm_map:
        pick = closest[0]
    if pick is None:
        pick = closest[0]
    print(f"[decoder] Mapping voor '{var_norm}' → dichtste match '{pick}'")
    return pick


def _apply_single_mapping(
    result_df: pl.DataFrame,
    chosen_col: str,
    mapping: dict,
    var_norm: str,
    orig_name: Optional[str],
) -> pl.DataFrame:
    """Replace values in chosen_col using code→label mapping."""
    try:
        result_df = result_df.with_columns(
            pl.col(chosen_col).cast(pl.Utf8).str.strip_chars().alias(chosen_col)
        )
        src_vals = result_df[chosen_col].to_list()
        lower_map = {k.lower(): v for k, v in mapping.items() if isinstance(k, str)}
        mapped_vals = []
        unmapped_seen: set = set()
        total_non_null = mapped_count = 0

        for v in src_vals:
            if v is None:
                mapped_vals.append(None)
                continue
            s = str(v).strip()
            total_non_null += 1

            if s == "":
                found = next(
                    (mapping[k] for k in mapping if isinstance(k, str) and "leeg" in k.lower()),
                    None,
                )
                mapped_vals.append(found if found is not None else s)
                if found is not None:
                    mapped_count += 1
                elif s not in unmapped_seen:
                    unmapped_seen.add(s)
                continue

            found = None
            for variant in (s, s.upper(), s.zfill(2)):
                if variant in mapping:
                    found = mapping[variant]
                    break
            if found is None:
                try:
                    found = mapping.get(int(s))
                except (ValueError, TypeError):
                    pass
            if found is None:
                found = lower_map.get(s.lower())

            mapped_vals.append(found if found is not None else s)
            if found is not None:
                mapped_count += 1
            elif s not in unmapped_seen:
                unmapped_seen.add(s)

        try:
            result_df = result_df.with_columns(pl.Series(mapped_vals).alias(chosen_col))
        except Exception:
            result_df = result_df.with_columns(
                pl.Series(mapped_vals).cast(pl.Utf8).alias(chosen_col)
            )
        print(
            f"[decoder] '{chosen_col}' (orig: '{orig_name}'): "
            f"{mapped_count}/{total_non_null} gemapt, niet-gemapt: {list(unmapped_seen)[:5]}"
        )
    except Exception as e:
        print(f"[decoder] Fout bij mapping voor {var_norm}: {e}")
    return result_df


# ---------------------------------------------------------------------------
# Public: decode functions
# ---------------------------------------------------------------------------


def decode_fields_dec_only(
    df: pl.DataFrame,
    metadata_json_path: str,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Optional[Callable[[str], str]] = None,
    decode_columns: Optional[list[str]] = None,
) -> pl.DataFrame:
    """
    Decode fields using only Dec_* lookup tables (no variable_metadata enrichment).

    Use this when variable_metadata.json is unavailable or when the caller
    wants to explicitly skip label substitution (e.g. for VAKHAVW files where
    no variable_metadata mappings exist).

    Args:
        df: Input DataFrame with coded fields.
        metadata_json_path: Path to Bestandsbeschrijving_Dec-bestanden JSON.
        dec_tables: Dec_* DataFrames keyed by table title.
        naming_func: Optional column name normalizer.
        decode_columns: When not None, only decode these column names.
            Use decoder_info.get_available_decode_columns() to discover valid names.

    Returns:
        DataFrame with Dec-decoded columns appended.
    """
    meta = _load_meta(metadata_json_path)
    norm_df, norm_map, _orig = _normalize_df(df, naming_func)
    result_df = _apply_dec_tables(norm_df, meta, dec_tables, naming_func, decode_columns)
    result_df = _apply_vakken_patch(result_df, meta, dec_tables, naming_func)
    return result_df.rename({k: v for k, v in norm_map.items() if k in result_df.columns})


def decode_fields(
    df: pl.DataFrame,
    metadata_json_path: str,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Optional[Callable[[str], str]] = None,
    variable_metadata_path: Optional[str] = None,
    decode_columns: Optional[list[str]] = None,
    enrich_variables: Optional[list[str]] = None,
) -> pl.DataFrame:
    """
    Decode fields using Dec_* tables, then apply variable_metadata label substitution.

    For datasets that have no variable_metadata mappings (e.g. VAKHAVW), the
    result is identical to decode_fields_dec_only.  Callers can detect this by
    comparing the two outputs and skip writing a redundant _enriched file.

    Args:
        df: Input DataFrame with coded fields.
        metadata_json_path: Path to Bestandsbeschrijving_Dec-bestanden JSON.
        dec_tables: Dec_* DataFrames keyed by table title.
        naming_func: Optional column name normalizer.
        variable_metadata_path: Path to variable_metadata.json. Falls back to
            the default location when None.
        decode_columns: When not None, only decode these column names via
            Dec_* tables.  Use decoder_info.get_available_decode_columns() to discover valid names.
        enrich_variables: When not None, only apply variable_metadata labels
            for these variable names.  Use decoder_info.get_available_enrich_variables() to
            discover valid names.

    Returns:
        DataFrame with Dec-decoded columns appended and variable-level labels
        substituted where variable_metadata provides mappings.
    """
    meta = _load_meta(metadata_json_path)
    norm_df, norm_map, orig_columns = _normalize_df(df, naming_func)
    result_df = _apply_dec_tables(norm_df, meta, dec_tables, naming_func, decode_columns)
    result_df = _apply_vakken_patch(result_df, meta, dec_tables, naming_func)
    result_df = _apply_variable_mappings(
        result_df, variable_metadata_path, naming_func, norm_map, orig_columns, enrich_variables
    )
    return result_df.rename({k: v for k, v in norm_map.items() if k in result_df.columns})


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def clean_for_latin1(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replace characters outside latin-1 range in all string columns.

    Replaces fraction slash (U+2044) with '/' and all other non-latin-1
    characters with '?'.

    Args:
        df: Input DataFrame to sanitize.

    Returns:
        DataFrame with non-latin-1 characters replaced.
    """
    non_latin1_regex = r"[^\x00-\xFF]"
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col)
                .str.replace_all("⁄", "/")
                .str.replace_all(non_latin1_regex, "?")
                .alias(col)
            )
    return df

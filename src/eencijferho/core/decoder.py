import json
import logging
import os
import re as _re

import polars as pl

from eencijferho.utils.converter_headers import clean_header_name, normalize_name, strip_accents
from eencijferho.utils.run_log import log_failed, log_skipped, log_unexpected
from eencijferho.io.decorators import with_storage
from eencijferho.core.dec_layouts import supplementary_tables
from eencijferho.core.decode_report import (
    DEC_TABLE_NOT_LOADED,
    NO_LABEL_COLUMNS,
    NOT_IN_DECODE_SCOPE,
    DecodeReport,
)
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


def _report_file(report: DecodeReport | None) -> str | None:
    """Name of the file being decoded, for the run log; None without a report."""
    return report.current_file if report is not None else None


# ---------------------------------------------------------------------------
# Name canonicalisation
# ---------------------------------------------------------------------------

# Known corrupt spellings in DUO metadata names, repaired before a name is
# normalised. Each entry is one observed corruption of one word; anything not
# listed here is left as it is (and then simply does not match a column).
# "vóór" arrives with each accented letter replaced by U+FFFD, either as the
# character itself or as its UTF-8 bytes read as Latin-1 ("ï¿½").
_NAME_REPAIRS: tuple[tuple[_re.Pattern[str], str], ...] = (
    (_re.compile(r"\b([vV])(?:\ufffd\ufffd|\u00ef\u00bf\u00bd\u00ef\u00bf\u00bd)r\b"), r"\1óór"),
)


def _repair_name(raw: str) -> str:
    """Apply the explicit repair table ``_NAME_REPAIRS`` to a metadata name."""
    for pattern, replacement in _NAME_REPAIRS:
        raw = pattern.sub(replacement, raw)
    return raw


def _canonical_name(raw: str, naming_func: Callable[[str], str] | None = None) -> str:
    """Canonical column name of a Dec/variable-metadata name.

    Repairs known corrupt words, strips accents and normalises, so that
    "Hoogste vooropleiding vóór het HO" and its corrupt forms all become
    ``hoogste_vooropleiding_voor_het_ho``.
    """
    return normalize_name(strip_accents(_repair_name(raw)), naming_func)


# ---------------------------------------------------------------------------
# Public: load helpers
# ---------------------------------------------------------------------------


@with_storage
def load_variable_mappings(
    storage,
    variable_metadata_path: str | None = None,
    naming_func: Callable[[str], str] | None = None,
    report: DecodeReport | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Loads variable-level value mappings from a variable metadata JSON file.

    Args:
        variable_metadata_path: Path to variable_metadata.json. Falls back to
            the default location inside data/00-metadata/json/ when None.
        naming_func: Optional column name normalizer.
        report: Optional collector for the decode log.

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
    path = next((p for p in candidates if storage.exists(p)), None)
    if path is None:
        if report is not None:
            report.record_variable_metadata(variable_metadata_path, "missing", 0)
        return {}

    try:
        items = storage.read_json(path)
    except Exception as e:  # catch-all: any storage backend or JSON error; the run goes on without mappings
        log_failed(
            logger, "variable_metadata_load", e, file=path, show_message=True,
            detail=f"[decoder] Waarschuwing: kon variable metadata niet laden {path}",
        )
        if report is not None:
            report.record_variable_metadata(path, "unreadable", 0)
        return {}

    maps: dict[str, dict[str, Any]] = {}
    for item in items:
        name = item.get("name")
        values = item.get("values") or {}
        if not name or not isinstance(values, dict):
            continue
        norm = _canonical_name(name, naming_func)
        entry: dict[str | int, Any] = {}
        for k, v in values.items():
            key = k.strip() if isinstance(k, str) else str(k)
            entry[key] = v
            if isinstance(key, str) and key.upper() != key:
                entry[key.upper()] = v
            if isinstance(key, str) and key.isdigit():
                entry[key.zfill(2)] = v
                try:
                    entry[int(key)] = v
                except ValueError:
                    pass  # a digit int() does not read (e.g. "²"): the string keys suffice
            else:
                try:
                    int_key = int(key)
                    if int_key not in entry:
                        entry[int_key] = v
                except ValueError:
                    pass  # not a number: the string keys suffice
        if entry:
            maps[norm] = {"orig_name": name, "mapping": entry}

    if maps:
        print(f"[decoder] {len(maps)} variabele-mappings geladen uit {path}")
    if report is not None:
        report.record_variable_metadata(path, "loaded", len(maps))
    return maps


def _read_dec_csv(storage, dec_path: str) -> pl.DataFrame | None:
    """Read a Dec CSV with every column as a string; None when it cannot be read.

    Dec labels can be codes too (an ISCED rubriek such as 031103) and must
    keep their leading zeros. Falls back to reading without quote parsing.
    Polars errors can quote cell values: the run log gets the error type only.
    """
    name = os.path.basename(dec_path)
    try:
        return storage.read_dataframe(dec_path, infer_schema_length=0)
    except Exception as e:  # catch-all: any read error gets the retry without quote parsing
        log_skipped(logger, "dec_load", "retry_without_quote_parsing", file=name, error=e)
    try:
        return storage.read_dataframe(
            dec_path, infer_schema_length=0, quote_char=None,
        )
    except Exception as e:  # catch-all: missing file, storage or parse error; the table is not loaded
        log_failed(logger, "dec_load", e, file=name, detail=f"[decoder] Kon {name} niet laden")
        return None


@with_storage
def load_dec_tables_from_metadata(
    storage,
    metadata_json_path: str,
    dec_output_dir: str,
    naming_func: Callable[[str], str] | None = None,
    vakken_metadata_json: str | None = None,
    report: DecodeReport | None = None,
) -> dict[str, pl.DataFrame]:
    """
    Loads Dec_* tables as Polars DataFrames based on metadata JSON.

    Args:
        metadata_json_path: Path to the metadata JSON file.
        dec_output_dir: Directory containing Dec_* CSV files.
        naming_func: Optional column name normalizer.
        vakken_metadata_json: Optional path to the Vakkenbestanden JSON. When
            given, the Dec tables its layout refers to are loaded too
            (Dec_vakcode, and Dec_brinnummer / Dec_brinvestigingsnummer from
            the inferred layouts in ``dec_layouts``), keyed by ``Dec_X.asc``.
        report: Optional collector for the decode log; records every table
            that could not be loaded.

    Returns:
        Mapping of table titles to DataFrames with all columns as strings.
        Missing files are skipped.

    Example:
        >>> dec_tables = load_dec_tables_from_metadata(
        ...     "data/00-metadata/json/Bestandsbeschrijving_Dec-bestanden.json",
        ...     "data/02-output/DEMO",
        ... )
    """
    meta = storage.read_json(metadata_json_path)

    dec_tables: dict[str, pl.DataFrame] = {}
    for table in meta["tables"]:
        dec_path = os.path.join(dec_output_dir, table["table_title"].replace(".asc", ".csv"))
        df = _read_dec_csv(storage, dec_path)
        if df is not None:
            dec_tables[table["table_title"]] = df
        elif report is not None:
            reason = "file_not_found" if not storage.exists(dec_path) else "read_error"
            report.record_dec_table_not_loaded(table["table_title"], reason)

    if report is not None:
        report.record_dec_tables_loaded(len(meta["tables"]), len(dec_tables))

    if vakken_metadata_json:
        # Only the Dec files the Vakken layout names; a CSV that is not there
        # (file not delivered) is skipped without noise.
        for title in build_dec_layouts(storage.read_json(vakken_metadata_json)):
            dec_path = os.path.join(dec_output_dir, title.replace(".asc", ".csv"))
            if title in dec_tables or not storage.exists(dec_path):
                continue
            df = _read_dec_csv(storage, dec_path)
            if df is not None:
                dec_tables[title] = df

    return dec_tables


@with_storage
def find_vakken_metadata_json(storage, json_dir: str) -> str | None:
    """Path of the Bestandsbeschrijving_Vakkenbestanden*.json in json_dir, or None."""
    if not json_dir:
        return None
    matches = sorted(storage.list_files(f"{json_dir}/Bestandsbeschrijving_Vakkenbestanden*.json"))
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


@with_storage
def _load_meta(storage, metadata_json_path: str) -> dict:
    return storage.read_json(metadata_json_path)


def _normalize_df(
    df: pl.DataFrame,
    naming_func: Callable | None = None,
) -> tuple[pl.DataFrame, dict[str, str], list[str]]:
    """Normalize column names and cast all values to stripped strings.

    Returns:
        (norm_df, norm_map, orig_columns)
        norm_map maps normalized_name → original_name.
    """


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
    naming_func: Callable | None = None,
) -> pl.DataFrame:
    """Normalize Dec table column names and trim whitespace in the code column.

    The code itself is left as it is: matching happens on a temporary key built
    by ``_join_key``, so data and Dec codes are normalised by the same rule.
    """
    join_df = dec_table.rename(
        {c: normalize_name(strip_accents(c), naming_func) for c in dec_table.columns}
    )
    if code_col_norm in join_df.columns:
        join_df = join_df.with_columns(
            pl.col(code_col_norm).cast(pl.Utf8).str.strip_chars().alias(code_col_norm)
        )
    return join_df


_CODE_WIDTH_RE = _re.compile(r"^\s*\S.*?\s{2,}\d+\s+(\d+)(?:\s|$)")


def _code_width(content_row: str | None) -> int | None:
    """Field width ("Aantal posities") of a code row in the file description.

    Rows look like ``"Code land     1     4"`` (name, start position, width,
    optionally followed by a ``}`` key marker). Returns None when the row
    carries no width.
    """
    if not content_row:
        return None
    m = _CODE_WIDTH_RE.match(content_row)
    if not m:
        return None
    width = int(m.group(1))
    return width if width > 0 else None


def _key_width(meta_width: int | None, *columns: pl.Series) -> int | None:
    """Width to pad a join key to.

    The width from the file description wins. Without it, fall back to the
    longest code found on either side of the join, so both sides are padded
    to one common width.
    """
    if meta_width:
        return meta_width
    maxima = [
        col.cast(pl.Utf8).str.strip_chars().str.len_chars().max()
        for col in columns
    ]
    lengths = [n for n in maxima if isinstance(n, int) and n]
    return max(lengths) if lengths else None


def _join_key(expr: pl.Expr, width: int | None) -> pl.Expr:
    """The one normalisation rule for matching a code to a Dec table.

    Trim whitespace and left-pad with zeros to ``width``, so ``411``, ``0411``
    and ``00411`` all match at width 5 and ``2UC`` matches ``02UC`` at width 4.
    An empty code becomes null, so it never matches a real ``0000`` (Dec tables
    such as Dec_landcode carry an empty sentinel row next to ``0000``).

    Use it only for temporary key columns: a data column is never overwritten.
    """
    code = expr.cast(pl.Utf8).str.strip_chars()
    code = pl.when(code != "").then(code)  # empty -> null
    if width:
        code = code.str.zfill(width)
    return code


def _keyed_lookup(
    result_df: pl.DataFrame,
    join_df: pl.DataFrame,
    pairs: list[tuple[str, str, int | None]],
    dec_cols: list[str],
    log_name: str,
    log_prefix: str = "[decoder]",
) -> tuple[pl.DataFrame, list[str]]:
    """Left-join the Dec label columns onto result_df through temporary keys.

    Args:
        pairs: ``(data_col, dec_col, meta_width)`` per key column.
        dec_cols: Dec label columns to fetch.

    Returns:
        ``(joined, label_cols)``: a frame with result_df's height and order,
        holding only the temporary key and label columns (named
        ``label_cols``, in the order of ``dec_cols``). result_df itself is not
        changed, so no temporary column can end up in the output.
    """
    key_cols = [f"__dec_key_{i}" for i in range(len(pairs))]
    label_cols = [f"__dec_label_{i}" for i in range(len(dec_cols))]
    left_keys, right_keys = [], []
    for key, (data_col, dec_col, meta_width) in zip(key_cols, pairs):
        width = _key_width(meta_width, join_df[dec_col], result_df[data_col])
        left_keys.append(_join_key(pl.col(data_col), width).alias(key))
        right_keys.append(_join_key(pl.col(dec_col), width).alias(key))

    left = result_df.select(left_keys)
    right = (
        join_df.select(
            right_keys + [pl.col(c).alias(n) for c, n in zip(dec_cols, label_cols)]
        )
        .drop_nulls(subset=key_cols)
    )
    deduped = right.unique(subset=key_cols, keep="first", maintain_order=True)
    if deduped.height < right.height:
        print(
            f"{log_prefix} Dubbele codes in Dec-tabel voor {log_name}: "
            f"{right.height - deduped.height} rij(en) genegeerd"
        )
    joined = left.join(deduped, on=key_cols, how="left", maintain_order="left")
    return joined, label_cols


def _apply_single_dec_join(
    result_df: pl.DataFrame,
    join_df: pl.DataFrame,
    var_norm: str,
    code_col_norm: str,
    is_composite: bool,
    code_col2_norm: str | None,
    naming_func: Callable | None = None,
    code_width: int | None = None,
    code2_width: int | None = None,
    report: DecodeReport | None = None,
    dec_table_title: str | None = None,
) -> pl.DataFrame:
    """Apply one Dec table join (simple or composite key) to result_df.

    The data column is matched through a temporary key and never rewritten:
    a code is an identifier, and DUO writes it fixed width (00411 is not 411).

    Args:
        code_width: Width of ``code_col_norm`` from the file description.
        code2_width: Width of ``code_col2_norm`` (composite key only).
        report: Optional collector for the decode log.
        dec_table_title: Dec table title, used in the decode log.
    """
    try:
        if is_composite:
            if code_col2_norm is None:
                # The caller sets is_composite only with a second code column.
                # TypeError: what join_df[None] raised here before.
                raise TypeError("samengestelde sleutel zonder tweede codekolom")
            dec_cols = [
                c for c in join_df.columns
                if c != code_col_norm and c != code_col2_norm
            ]
            pairs = [
                (code_col_norm, code_col_norm, code_width),
                (var_norm, code_col2_norm, code2_width),
            ]
        else:
            dec_cols = [c for c in join_df.columns if c != code_col_norm]
            pairs = [(var_norm, code_col_norm, code_width)]

        joined, label_cols = _keyed_lookup(result_df, join_df, pairs, dec_cols, var_norm)

        result_df = result_df.with_columns(
            joined[label].alias(
                f"{var_norm}__{normalize_name(strip_accents(col), naming_func)}"
            )
            for col, label in zip(dec_cols, label_cols)
        )

        if label_cols:
            unmatched = result_df.filter(
                pl.col(var_norm).is_not_null() & joined[label_cols[0]].is_null()
            )
            if unmatched.height > 0:
                sample = unmatched[var_norm].unique().to_list()[:5]
                print(f"[decoder] Niet-gematchte codes voor {var_norm}: {sample}")
                if report is not None:
                    keys = [code_col_norm, var_norm] if is_composite else [var_norm]
                    report.record_unmatched(keys, dec_table_title or "", unmatched)
        elif report is not None:
            report.record_skip(var_norm, NO_LABEL_COLUMNS)
    except Exception as e:  # catch-all: one failed join leaves the column undecoded, not the run
        log_failed(
            logger, "dec_join", e, file=_report_file(report), column=var_norm,
            detail=f"[decoder] Fout bij Dec-join voor {var_norm}",
        )
        if report is not None:
            report.record_error(var_norm, "dec_join", e)

    return result_df


def _apply_dec_tables(
    result_df: pl.DataFrame,
    meta: dict,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Callable | None = None,
    decode_columns: list[str] | None = None,
    report: DecodeReport | None = None,
) -> pl.DataFrame:
    """Apply all Dec table joins defined in metadata to result_df.

    Fixes a previous bug where an outer loop shadowed itself, causing each
    table's join to be applied N times instead of once.

    Args:
        decode_columns: When not None, only decode variables whose name
            appears in this list (matched after normalization).
        report: Optional collector for the decode log.
    """
    allowed: set[str] | None = None
    if decode_columns is not None:
        allowed = {_canonical_name(c, naming_func) for c in decode_columns}

    for table in meta["tables"]:
        dec_vars = table.get("decoding_variables", [])
        dec_table = dec_tables.get(table["table_title"])
        content = table.get("content", [])

        # Fallback: derive decoding variable from first content row
        if not dec_vars and len(content) > 1:
            dec_vars = [content[1].split("  ")[0].strip()]

        if not dec_vars or len(content) < 2:
            continue

        if allowed is not None:
            if report is not None:
                for v in dec_vars:
                    v_norm = _canonical_name(v, naming_func)
                    if v_norm not in allowed:
                        report.record_skip(v_norm, NOT_IN_DECODE_SCOPE)
            dec_vars = [v for v in dec_vars if _canonical_name(v, naming_func) in allowed]
        if not dec_vars:
            continue

        if dec_table is None:
            if report is not None:
                report.record_dec_table_not_loaded(table["table_title"], "not_loaded")
                for v in dec_vars:
                    report.record_skip(
                        normalize_name(strip_accents(v), naming_func), DEC_TABLE_NOT_LOADED
                    )
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

        for var in dec_vars:
            var_norm = _canonical_name(var, naming_func)
            if var_norm not in result_df.columns:
                print(f"[decoder] Sla '{var}' over — kolom '{var_norm}' niet gevonden in DataFrame.")
                if report is not None:
                    report.record_not_found(var, table["table_title"])
                continue

            result_df = _apply_single_dec_join(
                result_df, join_df, var_norm, code_col_norm,
                is_composite, code_col2_norm, naming_func,
                code_width=_code_width(content[1]),
                code2_width=_code_width(content[2]) if is_composite else None,
                report=report,
                dec_table_title=table["table_title"],
            )

    return result_df


# A layout row: name, start position, width and an optional remark, e.g.
# "Vestigingsnummer VO-vestiging    17    2    in combinatie met Brinnummer ...".
# The name ends at the first run of two or more spaces before the numbers, so
# names with single spaces and digits ("Gemeentecode per 1 januari 2025") stay whole.
_LAYOUT_ROW_RE = _re.compile(
    r"^\s*(?P<name>\S.*?)\s{2,}(?P<start>\d+)\s+(?P<width>\d+)(?:\s+(?P<remark>\S.*?))?\s*$"
)
_DEC_TITLE_RE = _re.compile(r"Dec_[\w-]+\.asc", _re.IGNORECASE)


def _dec_title(table_title: str) -> str | None:
    """The ``Dec_X.asc`` file a table title describes, or None.

    ``"Decodeertabel vakcode (Dec_vakcode.asc)"`` -> ``"Dec_vakcode.asc"``.
    """
    m = _DEC_TITLE_RE.search(table_title or "")
    return m.group(0) if m else None


def _parse_layout_rows(content: list[str]) -> list[dict[str, Any]]:
    """Parse the field rows of a layout table (content[0] is the header).

    Returns one dict per field: ``name``, ``start``, ``width``, ``remark``.
    """
    rows = []
    for line in content[1:]:
        m = _LAYOUT_ROW_RE.match(line)
        if not m:
            continue
        rows.append({
            "name": m.group("name").strip(),
            "start": int(m.group("start")),
            "width": int(m.group("width")),
            "remark": (m.group("remark") or "").strip(),
        })
    return rows


def _resolve_field_reference(
    reference: str,
    field_names: list[str],
    exclude: str | None = None,
    naming_func: Callable | None = None,
) -> str | None:
    """Find the layout field a short reference points to.

    DUO writes "in combinatie met Brinnummer" where the field is called
    "Brinnummer VO-instelling": an exact (normalised) match wins, otherwise the
    one field whose normalised name starts with the reference. Returns None
    when no field or more than one field fits.
    """
    ref = normalize_name(strip_accents(reference), naming_func)
    names = [n for n in field_names if n != exclude]
    norms = {n: normalize_name(strip_accents(n), naming_func) for n in names}
    exact = [n for n in names if norms[n] == ref]
    if len(exact) == 1:
        return exact[0]
    prefix = [n for n in names if norms[n].startswith(ref + "_")]
    return prefix[0] if len(prefix) == 1 else None


def parse_vakken_layout(
    content: list[str],
    naming_func: Callable | None = None,
) -> list[dict[str, Any]]:
    """Parse a Vakkenbestanden layout table, decode rules included.

    The decode rules sit in the ``Opmerking`` column ("te decoderen met
    Dec_X.asc", optionally "in combinatie met <veld>").

    Returns one dict per field: ``name``, ``start``, ``width``, ``remark``,
    ``dec_table`` (``Dec_X.asc`` or None) and ``composite`` (the full name of
    the partner field of a composite key, or None).
    """
    rows = _parse_layout_rows(content)
    names = [r["name"] for r in rows]
    fields = []
    for row in rows:
        reference, dec_table = _parse_vakken_opmerking(row["remark"])
        partner = None
        if reference:
            partner = _resolve_field_reference(reference, names, row["name"], naming_func)
            if partner is None:
                print(
                    f"[decoder][vakken] Samengesteld veld '{reference}' bij "
                    f"'{row['name']}' niet gevonden in de lay-out."
                )
        fields.append({**row, "dec_table": dec_table, "composite": partner})
    return fields


def build_dec_layouts(*metas: dict | None) -> dict[str, list[str]]:
    """Map ``Dec_X.asc`` -> layout content, from all known descriptions.

    Reads the tables of each metadata dict (Dec-bestanden, Vakkenbestanden)
    whose title names a Dec file, then adds the inferred layouts from
    ``dec_layouts`` for files no DUO description covers. The first
    description of a file wins.
    """
    layouts: dict[str, list[str]] = {}
    for meta in metas:
        for table in (meta or {}).get("tables", []):
            title = _dec_title(table.get("table_title", ""))
            if title and title not in layouts and table.get("content"):
                layouts[title] = table["content"]
    for title, table in supplementary_tables().items():
        layouts.setdefault(title, table["content"])
    return layouts


def _apply_vakken_layout(
    result_df: pl.DataFrame,
    vakken_meta: dict,
    dec_layouts: dict[str, list[str]],
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Callable | None = None,
    decode_columns: list[str] | None = None,
    report: DecodeReport | None = None,
) -> pl.DataFrame:
    """Decode the fields of a Vakkenbestanden layout (e.g. VAKHAVW).

    Every field whose Opmerking says "te decoderen met Dec_X.asc" is joined
    to that Dec table; "in combinatie met <veld>" makes it a composite key.
    A frame that has none of the fields to decode (an EV file) is returned
    unchanged.

    Args:
        decode_columns: When not None, only decode fields whose name appears
            in this list (matched after normalization).
        report: Optional collector for the decode log.
    """
    allowed: set[str] | None = None
    if decode_columns is not None:
        allowed = {_canonical_name(c, naming_func) for c in decode_columns}

    for table in vakken_meta.get("tables", []):
        content = table.get("content", [])
        if not content or "opmerking" not in content[0].lower():
            continue
        fields = parse_vakken_layout(content, naming_func)
        decode_cols = {
            _canonical_name(f["name"], naming_func)
            for f in fields if f["dec_table"]
        }
        if not decode_cols & set(result_df.columns):
            continue  # not a Vakken file (e.g. EV): nothing to decode here

        for field in fields:
            dec_table_title = field["dec_table"]
            if not dec_table_title:
                continue
            var_norm = _canonical_name(field["name"], naming_func)
            if allowed is not None and var_norm not in allowed:
                if report is not None:
                    report.record_skip(var_norm, NOT_IN_DECODE_SCOPE)
                continue

            dec_table = dec_tables.get(dec_table_title)
            dec_content = dec_layouts.get(dec_table_title)
            if dec_table is None or not dec_content or len(dec_content) < 2:
                print(f"[decoder][vakken] Dec-tabel niet geladen: {dec_table_title}")
                if report is not None and dec_table is None:
                    report.record_dec_table_not_loaded(dec_table_title, "not_loaded")
                    report.record_skip(var_norm, DEC_TABLE_NOT_LOADED)
                continue

            dec_rows = _parse_layout_rows(dec_content)
            if not dec_rows:
                print(f"[decoder][vakken] Geen lay-out voor {dec_table_title}")
                continue
            dec_code_col_norm = normalize_name(strip_accents(dec_rows[0]["name"]), naming_func)
            join_df = _normalize_dec_table(dec_table, dec_code_col_norm, naming_func)

            if field["composite"]:
                result_df = _apply_vakken_composite(
                    result_df, join_df, var_norm, field["composite"],
                    dec_code_col_norm, dec_content, dec_table_title, naming_func,
                    report=report,
                )
            else:
                result_df = _apply_vakken_simple(
                    result_df, join_df, var_norm, dec_code_col_norm,
                    field["name"], dec_table_title, dec_content, report=report,
                )

    return result_df


def _parse_vakken_opmerking(opm: str) -> tuple[str | None, str | None]:
    """Parse an Opmerking value for decode instructions.

    Returns (composite_col, dec_table_title) or (None, None) if no instruction found.
    ``composite_col`` is the field reference as DUO writes it ("Brinnummer");
    ``parse_vakken_layout`` resolves it to the full field name.
    """
    opm_lower = opm.lower()
    if "in combinatie met" in opm_lower and "te decoderen met dec_" in opm_lower:
        m = _re.search(
            r"in combinatie met (.+?) te decoderen met (Dec_[\w-]+)\.asc",
            opm, _re.IGNORECASE,
        )
        if m:
            return m.group(1).strip(), m.group(2) + ".asc"
    elif "te decoderen met dec_" in opm_lower:
        m = _re.search(r"te decoderen met (Dec_[\w-]+)\.asc", opm, _re.IGNORECASE)
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
    dec_content: list | None = None,
    report: DecodeReport | None = None,
) -> pl.DataFrame:
    """Apply a simple (single-key) Vakkenbestanden join.

    Matches through a temporary key (see ``_join_key``); the data column is
    left as it is.
    """
    if var_norm not in result_df.columns:
        print(f"[decoder][vakken] Sla '{naam}' over — kolom '{var_norm}' niet gevonden in DataFrame.")
        if report is not None:
            report.record_not_found(naam, dec_table_title)
        return result_df

    try:
        dec_cols = [c for c in join_df.columns if c != dec_code_col_norm]
        width = _code_width(dec_content[1]) if dec_content and len(dec_content) > 1 else None
        joined, label_cols = _keyed_lookup(
            result_df, join_df, [(var_norm, dec_code_col_norm, width)],
            dec_cols, naam, log_prefix="[decoder][vakken]",
        )
        result_df = result_df.with_columns(
            joined[label].alias(f"{var_norm}__{col}")
            for col, label in zip(dec_cols, label_cols)
        )
        if label_cols:
            unmatched = result_df.filter(
                pl.col(var_norm).is_not_null() & joined[label_cols[0]].is_null()
            )
            if unmatched.height > 0:
                print(f"[decoder][vakken] Niet-gematcht voor {naam}: {unmatched[var_norm].unique().to_list()[:5]}")
                if report is not None:
                    report.record_unmatched([var_norm], dec_table_title, unmatched)
        elif report is not None:
            report.record_skip(var_norm, NO_LABEL_COLUMNS)
    except Exception as e:  # catch-all: one failed join leaves the column undecoded, not the run
        log_failed(
            logger, "dec_join", e, file=_report_file(report), column=var_norm,
            detail=f"[decoder][vakken] Fout bij join voor {naam}",
        )
        if report is not None:
            report.record_error(var_norm, "dec_join", e)
    return result_df


def _apply_vakken_composite(
    result_df: pl.DataFrame,
    join_df: pl.DataFrame,
    var_norm: str,
    composite: str,
    dec_code_col_norm: str,
    dec_content: list,
    dec_table_title: str,
    naming_func: Callable | None = None,
    report: DecodeReport | None = None,
) -> pl.DataFrame:
    """Apply a composite-key Vakkenbestanden join.

    ``var_norm`` and ``composite`` (the partner field, as named in the layout)
    are paired with the two key columns of the Dec table by name: a data
    column "brinnummer_vo_instelling" belongs to key "Brinnummer". When the
    names do not tell, ``var_norm`` takes the first key and the partner the
    second. Label columns are named after ``var_norm``.

    Matches through temporary keys (see ``_join_key``); the data columns are
    left as they are.
    """
    composite_norm = _canonical_name(composite, naming_func)
    if len(dec_content) <= 2:
        print(f"[decoder][vakken] Geen tweede sleutelkolom voor samengestelde join in {dec_table_title}")
        return result_df

    dec_rows = _parse_layout_rows(dec_content)
    if len(dec_rows) >= 2:
        dec_code_col2_norm = normalize_name(strip_accents(dec_rows[1]["name"]), naming_func)
    else:
        dec_code_col2_norm = normalize_name(dec_content[2].split()[0], naming_func)

    for col in [var_norm, composite_norm]:
        if col not in result_df.columns:
            print(f"[decoder][vakken] Sla {var_norm} over (samengesteld) — kolom '{col}' niet gevonden in DataFrame.")
            if report is not None:
                report.record_not_found(col, dec_table_title)
            return result_df

    def _belongs(data_col: str, key_col: str) -> bool:
        return data_col == key_col or data_col.startswith(key_col + "_")

    first_width, second_width = _code_width(dec_content[1]), _code_width(dec_content[2])
    if _belongs(composite_norm, dec_code_col_norm) and not _belongs(var_norm, dec_code_col_norm):
        pairs = [
            (composite_norm, dec_code_col_norm, first_width),
            (var_norm, dec_code_col2_norm, second_width),
        ]
    else:
        pairs = [
            (var_norm, dec_code_col_norm, first_width),
            (composite_norm, dec_code_col2_norm, second_width),
        ]
    try:
        dec_cols = [
            c for c in join_df.columns
            if c not in (dec_code_col_norm, dec_code_col2_norm)
        ]
        joined, label_cols = _keyed_lookup(
            result_df, join_df, pairs, dec_cols, var_norm, log_prefix="[decoder][vakken]",
        )
        result_df = result_df.with_columns(
            joined[label].alias(f"{var_norm}__{col}")
            for col, label in zip(dec_cols, label_cols)
        )
        if label_cols:
            unmatched = result_df.filter(
                pl.col(var_norm).is_not_null()
                & pl.col(composite_norm).is_not_null()
                & joined[label_cols[0]].is_null()
            )
            if unmatched.height > 0:
                pairs = list(zip(
                    unmatched[var_norm].unique().to_list()[:5],
                    unmatched[composite_norm].unique().to_list()[:5],
                ))
                print(f"[decoder][vakken] Niet-gematchte samengestelde codes: {pairs}")
                if report is not None:
                    report.record_unmatched([composite_norm, var_norm], dec_table_title, unmatched)
        elif report is not None:
            report.record_skip(var_norm, NO_LABEL_COLUMNS)
    except Exception as e:  # catch-all: one failed join leaves the column undecoded, not the run
        log_failed(
            logger, "dec_join", e, file=_report_file(report), column=var_norm,
            detail=f"[decoder][vakken] Fout bij samengestelde join voor {var_norm}",
        )
        if report is not None:
            report.record_error(var_norm, "dec_join", e)
    return result_df


def _apply_vakken(
    result_df: pl.DataFrame,
    dec_meta: dict,
    vakken_metadata_json: str | None,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Callable | None = None,
    decode_columns: list[str] | None = None,
    report: DecodeReport | None = None,
) -> pl.DataFrame:
    """Decode the Vakkenbestanden fields when a Vakken JSON is given."""
    if not vakken_metadata_json:
        return result_df
    vakken_meta = _load_meta(vakken_metadata_json)
    dec_layouts = build_dec_layouts(dec_meta, vakken_meta)
    return _apply_vakken_layout(
        result_df, vakken_meta, dec_layouts, dec_tables, naming_func, decode_columns,
        report=report,
    )


def _apply_variable_mappings(
    result_df: pl.DataFrame,
    variable_metadata_path: str | None,
    naming_func: Callable | None,
    norm_map: dict[str, str],
    orig_columns: list[str],
    enrich_variables: list[str] | None = None,
    report: DecodeReport | None = None,
) -> pl.DataFrame:
    """Apply variable-level code→label mappings from variable_metadata.json.

    When variable_metadata_path is None or the file is absent, returns result_df unchanged.
    This is the step that differentiates decode_fields from decode_fields_dec_only.

    Args:
        enrich_variables: When not None, only apply mappings for variables
            whose name appears in this list (matched after normalization).
        report: Optional collector for the decode log.
    """
    allowed_enrich: set[str] | None = None
    if enrich_variables is not None:
        allowed_enrich = {_canonical_name(v, naming_func) for v in enrich_variables}

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
                var_norm, orig_name, result_df, norm_map, naming_func,
                report=report,
            )
            if chosen_col is None:
                continue
            result_df = _apply_single_mapping(
                result_df, chosen_col, mapping, var_norm, orig_name, report=report,
            )
    except Exception as e:  # catch-all: per-column errors are handled in _apply_single_mapping
        log_unexpected(
            logger, "variable_mapping", e, file=_report_file(report),
            detail="[decoder] Fout bij toepassen variabele-mappings",
        )
    return result_df


def _resolve_mapping_column(
    var_norm: str,
    orig_name: str | None,
    result_df: pl.DataFrame,
    norm_map: dict[str, str],
    naming_func: Callable | None,
    report: DecodeReport | None = None,
) -> str | None:
    """Find the column in result_df that a variable mapping applies to.

    Only exact matches count: ``var_norm`` itself, or the one column whose
    original header, cleaned and normalised, equals ``var_norm``. Anything
    else is logged and skipped, never guessed.
    """
    if var_norm in result_df.columns:
        return var_norm

    matches = []
    for col, orig in norm_map.items():
        if col in result_df.columns and normalize_name(clean_header_name(orig), naming_func) == var_norm:
            matches.append(col)

    if len(matches) == 1:
        return matches[0]
    if matches:
        print(
            f"[decoder] Mapping voor '{var_norm}' (orig: '{orig_name}') — "
            f"meerdere kolommen passen ({matches}), overgeslagen."
        )
        return None
    print(f"[decoder] Mapping voor '{var_norm}' (orig: '{orig_name}') — kolom niet gevonden.")
    if report is not None:
        report.record_not_found(orig_name or var_norm, "variable_metadata")
    return None


def _apply_single_mapping(
    result_df: pl.DataFrame,
    chosen_col: str,
    mapping: dict,
    var_norm: str,
    orig_name: str | None,
    report: DecodeReport | None = None,
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
        int_fallback_seen: set = set()
        total_non_null = mapped_count = int_fallback_count = 0

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
                if found is not None:
                    # Only the numeric value matched (00411 -> 411): log it, so a
                    # fixed-width code resolving through int() is never silent.
                    int_fallback_count += 1
                    int_fallback_seen.add(s)
            if found is None:
                found = lower_map.get(s.lower())

            mapped_vals.append(found if found is not None else s)
            if found is not None:
                mapped_count += 1
            elif s not in unmapped_seen:
                unmapped_seen.add(s)

        try:
            result_df = result_df.with_columns(pl.Series(mapped_vals).alias(chosen_col))
        except (TypeError, pl.exceptions.PolarsError) as e:
            logger.debug(
                "outcome=retry step=variable_mapping column=%s error=%s: cast naar String",
                chosen_col, type(e).__name__,
            )
            result_df = result_df.with_columns(
                pl.Series(mapped_vals).cast(pl.Utf8).alias(chosen_col)
            )
        print(
            f"[decoder] '{chosen_col}' (orig: '{orig_name}'): "
            f"{mapped_count}/{total_non_null} gemapt, niet-gemapt: {list(unmapped_seen)[:5]}"
        )
        if int_fallback_count:
            print(
                f"[decoder] '{chosen_col}': {int_fallback_count} waarde(n) alleen via "
                f"numerieke sleutel gemapt, bijv. {sorted(int_fallback_seen)[:5]}"
            )
        if report is not None:
            report.record_mapping(
                chosen_col, orig_name or var_norm, mapped_count, total_non_null, unmapped_seen,
                int_fallback_count, int_fallback_seen,
            )
    except Exception as e:  # catch-all: one failed mapping leaves the column unmapped, not the run
        log_failed(
            logger, "variable_mapping", e, file=_report_file(report), column=chosen_col,
            detail=f"[decoder] Fout bij mapping voor {var_norm}",
        )
        if report is not None:
            report.record_error(chosen_col, "variable_mapping", e)
    return result_df


# ---------------------------------------------------------------------------
# Public: decode functions
# ---------------------------------------------------------------------------


def decode_fields_dec_only(
    df: pl.DataFrame,
    metadata_json_path: str,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Callable[[str], str] | None = None,
    decode_columns: list[str] | None = None,
    vakken_metadata_json: str | None = None,
    report: DecodeReport | None = None,
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
        vakken_metadata_json: Optional path to the Vakkenbestanden JSON. When
            given, the fields of its layout that carry a decode rule
            ("te decoderen met Dec_X.asc") are decoded as well.
        report: Optional collector for the decode log (see
            :class:`eencijferho.core.decode_report.DecodeReport`).

    Returns:
        DataFrame with Dec-decoded columns appended.
    """
    meta = _load_meta(metadata_json_path)
    norm_df, norm_map, orig_columns = _normalize_df(df, naming_func)
    if report is not None:
        report.start_pass(norm_map, orig_columns)
    result_df = _apply_dec_tables(norm_df, meta, dec_tables, naming_func, decode_columns, report=report)
    result_df = _apply_vakken(
        result_df, meta, vakken_metadata_json, dec_tables, naming_func, decode_columns,
        report=report,
    )
    if report is not None:
        report.record_coverage(norm_df.columns, result_df.columns)
    return result_df.rename({k: v for k, v in norm_map.items() if k in result_df.columns})


def decode_fields(
    df: pl.DataFrame,
    metadata_json_path: str,
    dec_tables: dict[str, pl.DataFrame],
    naming_func: Callable[[str], str] | None = None,
    variable_metadata_path: str | None = None,
    decode_columns: list[str] | None = None,
    enrich_variables: list[str] | None = None,
    vakken_metadata_json: str | None = None,
    report: DecodeReport | None = None,
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
        vakken_metadata_json: Optional path to the Vakkenbestanden JSON; see
            decode_fields_dec_only.
        report: Optional collector for the decode log (see
            :class:`eencijferho.core.decode_report.DecodeReport`).

    Returns:
        DataFrame with Dec-decoded columns appended and variable-level labels
        substituted where variable_metadata provides mappings.
    """
    meta = _load_meta(metadata_json_path)
    norm_df, norm_map, orig_columns = _normalize_df(df, naming_func)
    if report is not None:
        report.start_pass(norm_map, orig_columns)
    result_df = _apply_dec_tables(norm_df, meta, dec_tables, naming_func, decode_columns, report=report)
    result_df = _apply_vakken(
        result_df, meta, vakken_metadata_json, dec_tables, naming_func, decode_columns,
        report=report,
    )
    if report is not None:
        report.record_coverage(norm_df.columns, result_df.columns)
    result_df = _apply_variable_mappings(
        result_df, variable_metadata_path, naming_func, norm_map, orig_columns, enrich_variables,
        report=report,
    )
    return result_df.rename({k: v for k, v in norm_map.items() if k in result_df.columns})

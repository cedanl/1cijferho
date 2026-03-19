# -----------------------------------------------------------------------------
# Organization: CEDA
# License: MIT
# -----------------------------------------------------------------------------
"""
Validates that column values in a converted main CSV match the valid codes
defined in the DEC (decoder) CSV files.

The mapping between DEC files and main file columns is parsed from the
bestandsbeschrijving .txt file via the "Ten behoeve van de decodering van"
sections.

Two mapping types are supported:

  simple    - single-column lookup, e.g.:
                "Ten behoeve van de decodering van de velden:"
                "* Geboorteland"
              Valid codes = first column of DEC CSV.

  composite - two-column key lookup, e.g.:
                "Ten behoeve van de decodering van instellingscode
                 in combinatie met het veld"
                "* Vestigingsnummer"
              Valid pairs = (col1, col2) of DEC CSV.
              Anchor col = the column named in the "van <X> in combinatie" part.
              Each target col is the second key column validated against the anchor.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import polars as pl

from eencijferho.utils.converter_headers import normalize_name as _to_snake


def parse_dec_mapping(dec_txt_path: str | Path) -> Dict[str, Dict[str, Any]]:
    """
    Parse the bestandsbeschrijving .txt file to build a mapping of
    DEC filename -> validation spec.

    Returns a dict where each value is one of:

        {"type": "simple",    "columns": ["Geboorteland", ...]}
        {"type": "composite", "anchor": "instellingscode",
                              "targets": ["Vestigingsnummer", ...]}

    Simple mappings come from "Ten behoeve van de decodering van de veld(en):"
    Composite mappings come from "Ten behoeve van de decodering van <X>
        in combinatie met het veld"
    """
    with open(dec_txt_path, encoding="utf-8", errors="replace") as f:
        lines = [ln.rstrip("\n") for ln in f]

    mapping: Dict[str, Dict[str, Any]] = {}
    current_dec: str | None = None

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Detect DEC filename header: "Dec_xxx.asc" followed by a separator line
        if re.match(r"^Dec_\S+\.asc$", line, re.IGNORECASE):
            next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""
            if re.match(r"^[=\-]+$", next_line):
                current_dec = line.replace(".asc", "")
                i += 2
                continue

        if current_dec:
            # --- Composite key: "Ten behoeve van de decodering van <X> in combinatie met het veld" ---
            composite_match = re.match(
                r"^Ten behoeve van de decodering van (.+?) in combinatie met het veld",
                line,
                re.IGNORECASE,
            )
            if composite_match:
                anchor_raw = composite_match.group(1).strip()
                i += 1
                targets: List[str] = []
                while i < len(lines):
                    col_line = lines[i].strip()
                    if col_line.startswith("*"):
                        target_name = col_line.lstrip("* ").strip()
                        targets.append(target_name)
                        i += 1
                    elif col_line == "":
                        i += 1
                        break
                    else:
                        break
                if targets:
                    mapping[current_dec] = {
                        "type": "composite",
                        "anchor": anchor_raw,
                        "targets": targets,
                    }
                continue

            # --- Simple key: "Ten behoeve van de decodering van de veld(en):" ---
            if re.match(r"^Ten behoeve van de decodering van", line, re.IGNORECASE):
                i += 1
                columns: List[str] = []
                while i < len(lines):
                    col_line = lines[i].strip()
                    if col_line.startswith("*"):
                        col_name = col_line.lstrip("* ").strip()
                        # Skip lines that still contain a '+' (e.g. Dec_vestnr_ho_compleet
                        # uses "Instellingscode + Vestigingsnummer van ..." as a single line)
                        if "+" not in col_name:
                            columns.append(col_name)
                        i += 1
                    elif col_line == "":
                        i += 1
                        break
                    else:
                        break
                if columns:
                    mapping[current_dec] = {"type": "simple", "columns": columns}
                continue

        i += 1

    return mapping


def _resolve_col(
    col_name: str,
    csv_cols: Dict[str, str],
) -> str | None:
    """
    Look up the original CSV column for a DEC txt column name.

    Handles normal snake_case conversion and falls back to a wildcard regex
    match for names that contain U+FFFD replacement characters (encoding
    artefacts in the DEC txt file where e.g. "ó" in "vóór" was lost).
    """
    norm = _to_snake(col_name)
    if norm in csv_cols:
        return csv_cols[norm]

    # Fallback for U+FFFD corruption
    if "\ufffd" in col_name:
        parts = re.split(r"\ufffd+", col_name)
        snake_parts = [_to_snake(p) for p in parts if p.strip()]
        snake_parts = [p for p in snake_parts if p]
        if snake_parts:
            pattern = re.compile(r".+".join(re.escape(p) for p in snake_parts))
            for csv_snake, csv_original in csv_cols.items():
                if pattern.fullmatch(csv_snake):
                    return csv_original

    return None


def validate_with_dec_files(
    main_csv_path: str | Path,
    dec_csv_dir: str | Path,
    mapping: Dict[str, Dict[str, Any]],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate columns in a main CSV against valid codes from DEC CSV files.

    Args:
        main_csv_path: Path to the converted main CSV (semicolon-separated, UTF-8).
        dec_csv_dir: Directory containing converted DEC CSV files.
        mapping: Output of parse_dec_mapping().

    Returns:
        Tuple of (success, results) with per-column outcomes.
    """
    results: Dict[str, Any] = {
        "columns_checked": 0,
        "columns_failed": 0,
        "column_results": [],
        "total_issues": 0,
    }

    try:
        df = pl.read_csv(main_csv_path, separator=";", encoding="utf-8", infer_schema_length=0)
    except Exception as e:
        results["load_error"] = str(e)
        results["total_issues"] += 1
        return False, results

    # Build lookup: snake_case column name -> original column name
    csv_cols = {_to_snake(c): c for c in df.columns}

    dec_csv_dir = Path(dec_csv_dir)

    for dec_name, spec in mapping.items():
        # Locate the DEC CSV file
        dec_csv = dec_csv_dir / f"{dec_name}.csv"
        if not dec_csv.exists():
            matches = list(dec_csv_dir.glob(f"{dec_name}.csv"))
            if not matches:
                continue
            dec_csv = matches[0]

        try:
            dec_df = pl.read_csv(dec_csv, separator=";", encoding="utf-8", infer_schema_length=0)
        except Exception:
            continue

        if spec["type"] == "simple":
            # Valid codes = all values in first column of DEC CSV
            valid_codes: Set[str] = set(
                dec_df.select(pl.col(dec_df.columns[0]).cast(pl.Utf8).str.strip_chars())
                .to_series()
                .drop_nulls()
                .to_list()
            )
            valid_codes.add("")  # always allow empty/null

            for col_name in spec["columns"]:
                original_col = _resolve_col(col_name, csv_cols)
                if original_col is None:
                    continue

                results["columns_checked"] += 1
                unique_vals = (
                    df.select(pl.col(original_col).cast(pl.Utf8).str.strip_chars())
                    .unique()
                    .to_series()
                    .to_list()
                )
                unique_vals_str = {v if v is not None else "" for v in unique_vals}
                invalid = sorted(unique_vals_str - valid_codes)

                col_result: Dict[str, Any] = {
                    "column": original_col,
                    "dec_file": dec_csv.name,
                    "invalid_values": invalid,
                    "status": "ok" if not invalid else "failed",
                }
                if invalid:
                    results["columns_failed"] += 1
                    results["total_issues"] += 1
                results["column_results"].append(col_result)

        elif spec["type"] == "composite":
            # Valid pairs = (col1_value, col2_value) from first two DEC CSV columns
            if len(dec_df.columns) < 2:
                continue

            col1, col2 = dec_df.columns[0], dec_df.columns[1]
            valid_pairs: Set[Tuple[str, str]] = set(
                zip(
                    dec_df[col1].cast(pl.Utf8).str.strip_chars().to_list(),
                    dec_df[col2].cast(pl.Utf8).str.strip_chars().to_list(),
                )
            )
            # Always allow (empty, empty)
            valid_pairs.add(("", ""))

            anchor_original = _resolve_col(spec["anchor"], csv_cols)
            if anchor_original is None:
                continue  # anchor column not in this file

            for target_name in spec["targets"]:
                target_original = _resolve_col(target_name, csv_cols)
                if target_original is None:
                    continue

                results["columns_checked"] += 1

                pairs_in_csv = set(
                    zip(
                        (
                            df.select(pl.col(anchor_original).cast(pl.Utf8).str.strip_chars())
                            .to_series()
                            .to_list()
                        ),
                        (
                            df.select(pl.col(target_original).cast(pl.Utf8).str.strip_chars())
                            .to_series()
                            .to_list()
                        ),
                    )
                )

                invalid_pairs = sorted(
                    (a or "", b or "") for a, b in pairs_in_csv
                    if (a or "", b or "") not in valid_pairs
                )

                col_result = {
                    "column": f"{anchor_original} + {target_original}",
                    "dec_file": dec_csv.name,
                    "invalid_values": [f"({a}, {b})" for a, b in invalid_pairs],
                    "status": "ok" if not invalid_pairs else "failed",
                }
                if invalid_pairs:
                    results["columns_failed"] += 1
                    results["total_issues"] += 1
                results["column_results"].append(col_result)

    success = results["total_issues"] == 0
    return success, results


def validate_with_dec_files_folder(
    output_dir: str | Path,
    dec_txt_path: str | Path,
) -> Dict[str, Any]:
    """
    Run DEC validation on all non-decoded, non-enriched CSV files in output_dir.

    Returns a summary dict keyed by filename.
    """
    output_dir = Path(output_dir)
    mapping = parse_dec_mapping(dec_txt_path)

    summary: Dict[str, Any] = {}

    main_csvs = [
        f for f in output_dir.glob("*.csv")
        if not f.name.startswith("Dec_")
        and not f.name.endswith("_decoded.csv")
        and not f.name.endswith("_enriched.csv")
        and not f.name.endswith("_encrypted.csv")
    ]

    for csv_file in main_csvs:
        success, results = validate_with_dec_files(csv_file, output_dir, mapping)
        summary[csv_file.name] = {"success": success, "results": results}

    return summary


def read_dec_validation_log(log_path: str | Path) -> List[Dict[str, Any]]:
    """
    Read the dec_validation_log JSON and return a flat list of failing columns.

    Returns list of dicts with keys: file, column, dec_file, invalid_values.
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
                    "dec_file": col.get("dec_file", ""),
                    "invalid_values": col["invalid_values"],
                })
    return failures

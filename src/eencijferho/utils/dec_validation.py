# -----------------------------------------------------------------------------
# Organization: CEDA
# License: MIT
# -----------------------------------------------------------------------------
"""
Validates that column values in a converted main CSV match the valid codes
defined in the DEC (decoder) CSV files.

The mapping between DEC files and main file columns is parsed from the
bestandsbeschrijving .txt file via the "Ten behoeve van de decodering van
de velden:" sections.
"""

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import polars as pl


def _to_snake(name: str) -> str:
    """snake_case conversion matching converter_headers.normalize_name exactly."""
    normalized = unicodedata.normalize("NFKD", name)
    s = normalized.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def parse_dec_mapping(dec_txt_path: str | Path) -> Dict[str, List[str]]:
    """
    Parse the bestandsbeschrijving .txt file to build a mapping of
    DEC filename -> list of main file column names it validates.

    Only includes simple single-column mappings. Composite key mappings
    (e.g. "Instellingscode + Vestigingsnummer") are skipped.

    Returns dict like:
        {"Dec_landcode": ["Geboorteland", "Geboorteland ouder 1", ...], ...}
    """
    with open(dec_txt_path, encoding="latin1") as f:
        lines = [ln.rstrip("\n") for ln in f]

    mapping: Dict[str, List[str]] = {}
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

        # Detect "Ten behoeve van de decodering van de veld(en):"
        if current_dec and re.match(r"^Ten behoeve van de decodering van", line, re.IGNORECASE):
            i += 1
            columns: List[str] = []
            while i < len(lines):
                col_line = lines[i].strip()
                if col_line.startswith("*"):
                    col_name = col_line.lstrip("* ").strip()
                    # Skip composite mappings (contain '+')
                    if "+" not in col_name:
                        columns.append(col_name)
                    i += 1
                elif col_line == "":
                    i += 1
                    break
                else:
                    break
            if columns:
                mapping[current_dec] = columns
            continue

        i += 1

    return mapping


def validate_with_dec_files(
    main_csv_path: str | Path,
    dec_csv_dir: str | Path,
    mapping: Dict[str, List[str]],
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

    for dec_name, col_names in mapping.items():
        # Find the DEC CSV — match on snake_case filename
        dec_csv = dec_csv_dir / f"{dec_name}.csv"
        if not dec_csv.exists():
            # Try case-insensitive match
            matches = list(dec_csv_dir.glob(f"{dec_name}.csv"))
            if not matches:
                continue  # DEC file not converted, skip silently
            dec_csv = matches[0]

        # Load valid codes from the first column of the DEC CSV
        try:
            dec_df = pl.read_csv(dec_csv, separator=";", encoding="utf-8", infer_schema_length=0)
        except Exception:
            continue

        valid_codes: Set[str] = set(
            dec_df.select(pl.col(dec_df.columns[0]).cast(pl.Utf8).str.strip_chars())
            .to_series()
            .drop_nulls()
            .to_list()
        )
        # Always allow empty/null (represented as empty string)
        valid_codes.add("")

        for col_name in col_names:
            norm = _to_snake(col_name)
            original_col = csv_cols.get(norm)
            if original_col is None:
                continue  # column not in this file

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
    Read (5c)_dec_validation_log_latest.json and return a flat list of failing columns.

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

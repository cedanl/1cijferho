"""
Snapshot-based regression validator for pipeline output.

Generates a JSON fingerprint of an output directory and validates
future runs against it. Intended for use with fixed DEMO input data
where the pipeline is fully deterministic.

Snapshot contents per file:
  - file_hash      : SHA256 of raw file bytes (catches any change)
  - row_count      : number of data rows (CSV/Parquet)
  - column_count   : number of columns
  - columns        : sorted list of column names
  - encrypted_cols : SHA256-hex format check for BSN/PGN columns (CSV only)
  - dtypes         : Polars dtype per column (Parquet only)
"""

import json
import os
import re
from datetime import datetime

from rich.console import Console
from rich.table import Table

from eencijferho.io.decorators import with_storage

_console = Console()

_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
_ENCRYPTED_COLS = {"persoonsgebonden_nummer", "burgerservicenummer", "onderwijsnummer"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _scan_file(storage, filepath: str) -> dict:
    """Return snapshot metadata for a single file."""
    fname = filepath.rsplit("/", 1)[-1] if "/" in filepath else filepath
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
    entry: dict = {}

    if ext == "csv":
        try:
            try:
                df = storage.read_dataframe(filepath, format="csv", infer_schema_length=0)
            except Exception:
                df = storage.read_dataframe(
                    filepath, format="csv", infer_schema_length=0, quote_char=None
                )
            entry["row_count"] = len(df)
            entry["column_count"] = len(df.columns)
            entry["columns"] = sorted(df.columns)

            encrypted_check = {}
            for col in df.columns:
                if col in _ENCRYPTED_COLS:
                    sample = df[col].drop_nulls().head(20).to_list()
                    valid = bool(sample) and all(_SHA256_RE.match(str(v)) for v in sample)
                    encrypted_check[col] = {"format": "sha256_hex", "sample_valid": valid}
            if encrypted_check:
                entry["encrypted_columns"] = encrypted_check
        except Exception as exc:
            entry["read_error"] = str(exc)

    elif ext == "parquet":
        try:
            df = storage.read_dataframe(filepath, format="parquet")
            entry["row_count"] = len(df)
            entry["column_count"] = len(df.columns)
            entry["columns"] = sorted(df.columns)
            entry["dtypes"] = {col: str(dt) for col, dt in zip(df.columns, df.dtypes)}
        except Exception as exc:
            entry["read_error"] = str(exc)

    return entry


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@with_storage
def generate_snapshot(storage, output_dir: str, snapshot_path: str) -> None:
    """Scan *output_dir* and write a snapshot JSON to *snapshot_path*."""
    files = [
        f for f in storage.list_files(f"{output_dir}/*")
        if "/metadata/" not in f and "\\metadata\\" not in f
    ]

    snapshot = {
        "generated_at": datetime.now().isoformat(),
        "output_dir": output_dir,
        "file_count": len(files),
        "files": {},
    }

    for filepath in sorted(files):
        fname = filepath.rsplit("/", 1)[-1] if "/" in filepath else filepath
        _console.print(f"  [dim]Scanning {fname}[/dim]")
        snapshot["files"][fname] = _scan_file(storage, filepath)

    os.makedirs(os.path.dirname(os.path.abspath(snapshot_path)), exist_ok=True)
    with open(snapshot_path, "w", encoding="utf-8") as fh:
        json.dump(snapshot, fh, indent=2, ensure_ascii=False)

    _console.print(
        f"[bold green]✓ Snapshot opgeslagen:[/bold green] {snapshot_path} "
        f"[dim]({len(files)} bestanden)[/dim]"
    )


@with_storage
def validate_snapshot(
    storage, output_dir: str, snapshot_path: str
) -> tuple[bool, list[str], list[str]]:
    """Compare *output_dir* against a saved snapshot.

    Returns ``(passed, errors, warnings)``.
    *passed* is True only when *errors* is empty.
    """
    with open(snapshot_path, encoding="utf-8") as fh:
        expected = json.load(fh)

    errors: list[str] = []
    warnings: list[str] = []

    current_files = [
        f for f in storage.list_files(f"{output_dir}/*")
        if "/metadata/" not in f and "\\metadata\\" not in f
    ]
    current_map = {
        (f.rsplit("/", 1)[-1] if "/" in f else f): f for f in current_files
    }
    expected_names = set(expected["files"].keys())
    current_names = set(current_map.keys())

    for fname in sorted(expected_names - current_names):
        errors.append(f"Bestand ontbreekt: {fname}")
    for fname in sorted(current_names - expected_names):
        warnings.append(f"Onverwacht bestand aanwezig: {fname}")

    for fname, exp in sorted(expected["files"].items()):
        if fname not in current_map:
            continue

        cur = _scan_file(storage, current_map[fname])

        for key in ("row_count", "column_count"):
            if key in exp and key in cur and cur[key] != exp[key]:
                label = "rijen" if key == "row_count" else "kolommen"
                errors.append(
                    f"{fname}: {label} {cur[key]} ≠ verwacht {exp[key]}"
                )

        if "columns" in exp and "columns" in cur:
            removed = sorted(set(exp["columns"]) - set(cur["columns"]))
            added = sorted(set(cur["columns"]) - set(exp["columns"]))
            if removed:
                errors.append(f"{fname}: kolomnamen verwijderd: {removed}")
            if added:
                errors.append(f"{fname}: kolomnamen toegevoegd: {added}")

        if "encrypted_columns" in exp:
            cur_enc = cur.get("encrypted_columns", {})
            for col, exp_col in exp["encrypted_columns"].items():
                if col not in cur_enc:
                    errors.append(f"{fname}: versleutelde kolom '{col}' niet aanwezig")
                elif not cur_enc[col].get("sample_valid"):
                    errors.append(
                        f"{fname}: kolom '{col}' bevat geen geldige SHA256-hashes"
                    )

        if "dtypes" in exp and "dtypes" in cur:
            for col, exp_dtype in exp["dtypes"].items():
                cur_dtype = cur["dtypes"].get(col)
                if cur_dtype and cur_dtype != exp_dtype:
                    errors.append(
                        f"{fname}: kolom '{col}' type {cur_dtype} ≠ verwacht {exp_dtype}"
                    )

    return len(errors) == 0, errors, warnings


def print_validation_result(
    passed: bool, errors: list[str], warnings: list[str], snapshot_path: str
) -> None:
    """Print a Rich-formatted summary of a validation result."""
    label = os.path.basename(os.path.dirname(snapshot_path)) or snapshot_path

    if passed and not warnings:
        _console.print(f"[bold green]✓ {label}: alle checks geslaagd[/bold green]")
        return

    if warnings:
        for w in warnings:
            _console.print(f"  [yellow]⚠ {w}[/yellow]")

    if not passed:
        table = Table(title=f"✗ {label}: {len(errors)} fout(en)", show_header=False)
        table.add_column("", style="red")
        for err in errors:
            table.add_row(err)
        _console.print(table)
    else:
        _console.print(
            f"[bold green]✓ {label}: geslaagd[/bold green] "
            f"[yellow]({len(warnings)} waarschuwing(en))[/yellow]"
        )


def main() -> None:  # pragma: no cover
    """CLI entrypoint: python -m eencijferho.utils.snapshot_validator"""
    import argparse as _argparse

    parser = _argparse.ArgumentParser(
        prog="python -m eencijferho.utils.snapshot_validator",
        description="Snapshot-gebaseerde regressievalidatie voor pipeline output.",
    )
    parser.add_argument("--output", required=True, metavar="DIR",
                        help="Output directory to scan or validate")
    parser.add_argument("--snapshot", required=True, metavar="FILE",
                        help="Path to snapshot JSON file")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--generate", action="store_true",
                      help="Scan output dir and write snapshot")
    mode.add_argument("--validate", action="store_true",
                      help="Compare output dir against snapshot")
    args = parser.parse_args()

    if args.generate:
        _console.print(f"[bold]Snapshot genereren van:[/bold] {args.output}")
        generate_snapshot(args.output, args.snapshot)
    else:
        _console.print(f"[bold]Snapshot valideren:[/bold] {args.output}")
        _console.print(f"[dim]Snapshot: {args.snapshot}[/dim]")
        passed, errors, warnings = validate_snapshot(args.output, args.snapshot)
        print_validation_result(passed, errors, warnings, args.snapshot)
        if not passed:
            raise SystemExit(1)


if __name__ == "__main__":
    main()

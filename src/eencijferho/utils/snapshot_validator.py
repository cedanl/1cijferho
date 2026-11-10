"""
Snapshot-based regression validator for pipeline output.

Generates a JSON fingerprint of an output directory and validates
future runs against it. Intended for use with fixed DEMO input data
where the pipeline is fully deterministic.

Snapshot contents per file:
  - file_hash      : SHA256 of the file bytes (version 2; not for Parquet). For CSV
                     the bytes are normalised from CRLF to LF and then both hashed
                     and parsed for the counts and column names, so a checkout or
                     editor that rewrites line endings does not count as a change.
                     The pipeline itself only writes LF.
  - data_hash      : SHA256 over schema and cell values (version 2; Parquet only)
  - row_count      : number of data rows (CSV/Parquet)
  - column_count   : number of columns
  - columns        : sorted list of column names
  - dtypes         : Polars dtype per column (Parquet only)

Parquet is not hashed on its raw bytes: the footer records the writer and
library version, and compression or row-group layout may change between
Polars releases and platforms without any change in the data. ``data_hash``
covers the column names and dtypes in order plus every cell value, through a
length-prefixed serialization in which null and the empty string differ.
Values are hashed through their String cast; the pipeline writes String-only
Parquet, so no float formatting is involved.

``generate_snapshot`` writes ``snapshot_version: 2``. In a version 2 snapshot
every CSV entry must carry ``file_hash`` and every Parquet entry ``data_hash``;
a missing hash fails validation. A legacy snapshot without
``snapshot_version`` (version 1) has no hashes; it is still accepted, but
only the schema is checked and every file gets a warning.

An unreadable file is recorded as ``read_error`` with only the exception type,
so no row content ends up in a snapshot or a CI log.
"""

import hashlib
import io
import json
import logging
import os
from datetime import datetime

import polars as pl
from rich.console import Console
from rich.table import Table

from eencijferho.io.decorators import with_storage
from eencijferho.utils.run_log import log_failed

_console = Console()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_error(exc: Exception) -> str:
    """Describe a read failure without the exception text, which may echo row values."""
    return f"{type(exc).__name__} bij het lezen"


def _update_framed(digest, data: bytes) -> None:
    """Feed *data* into *digest* with a length prefix, so field boundaries are unambiguous."""
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)


def _normalise_csv(raw: bytes) -> bytes:
    """CSV bytes with CRLF normalised to LF."""
    return raw.replace(b"\r\n", b"\n")


def _read_csv(data: bytes, **kwargs) -> pl.DataFrame:
    """Parse ``;``-separated CSV bytes with every column as String."""
    return pl.read_csv(io.BytesIO(data), separator=";", infer_schema_length=0, **kwargs)


def _data_hash(df: pl.DataFrame) -> str:
    """SHA256 over schema and cell values, independent of the Parquet file layout.

    Per column, in order: the null mask, the byte length of each value and the
    concatenated values, each length-prefixed. Null and "" therefore differ
    (mask), and so do ``["ab", ""]`` and ``["a", "b"]`` (lengths).
    """
    digest = hashlib.sha256()
    schema = [[name, str(dtype)] for name, dtype in df.schema.items()]
    _update_framed(digest, json.dumps(schema, ensure_ascii=False).encode("utf-8"))
    _update_framed(digest, str(df.height).encode("ascii"))
    for name in df.columns:
        col = df.get_column(name).cast(pl.String)
        nulls = col.is_null().cast(pl.UInt8).cast(pl.String).str.join("").item()
        lengths = col.str.len_bytes().fill_null(0).cast(pl.String).str.join(",").item()
        values = col.fill_null("").str.join("").item()
        _update_framed(digest, nulls.encode("ascii"))
        _update_framed(digest, lengths.encode("ascii"))
        _update_framed(digest, values.encode("utf-8"))
    return digest.hexdigest()


def _hash_key(fname: str) -> str:
    """Name of the content hash a snapshot entry for *fname* carries."""
    return "data_hash" if fname.lower().endswith(".parquet") else "file_hash"


def _scan_file(storage, filepath: str) -> dict:
    """Return snapshot metadata for a single file."""
    fname = os.path.basename(filepath)
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
    entry: dict = {}

    if ext == "parquet":
        try:
            df = storage.read_dataframe(filepath, format="parquet")
            entry["data_hash"] = _data_hash(df)
            entry["row_count"] = len(df)
            entry["column_count"] = len(df.columns)
            entry["columns"] = sorted(df.columns)
            entry["dtypes"] = {col: str(dt) for col, dt in zip(df.columns, df.dtypes)}
        except Exception as exc:  # catch-all: an unreadable file is a read_error, which fails validation
            log_failed(logger, "snapshot_scan", exc, file=fname)
            entry = {"read_error": _read_error(exc)}
        return entry

    try:
        raw = storage.read_bytes(filepath)
    except Exception as exc:  # catch-all: an unreadable file is a read_error, which fails validation
        log_failed(logger, "snapshot_scan", exc, file=fname)
        entry["read_error"] = _read_error(exc)
        return entry

    if ext != "csv":
        entry["file_hash"] = hashlib.sha256(raw).hexdigest()
        return entry

    # Hash and parse the same normalised bytes, so a CRLF copy of a file yields
    # the same hash and the same column names (no trailing "\r") as the LF original.
    data = _normalise_csv(raw)
    entry["file_hash"] = hashlib.sha256(data).hexdigest()
    try:
        try:
            df = _read_csv(data)
        except pl.exceptions.PolarsError:
            # The converter does not escape quotes in values (yet), so a Dec table
            # value like '"de Bontekoe" NTC-VO' is only readable without quote parsing.
            df = _read_csv(data, quote_char=None)
        entry["row_count"] = len(df)
        entry["column_count"] = len(df.columns)
        entry["columns"] = sorted(df.columns)
    except Exception as exc:  # catch-all: an unreadable file is a read_error, which fails validation
        log_failed(logger, "snapshot_scan", exc, file=fname)
        entry["read_error"] = _read_error(exc)

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
        "snapshot_version": 2,
        "generated_at": datetime.now().isoformat(),
        "output_dir": output_dir,
        "file_count": len(files),
        "files": {},
    }

    for filepath in sorted(files):
        fname = os.path.basename(filepath)
        _console.print(f"  [dim]Scanning {fname}[/dim]")
        entry = _scan_file(storage, filepath)
        if "read_error" in entry:
            raise ValueError(
                f"Kan geen snapshot maken van onleesbaar bestand {fname}: {entry['read_error']}"
            )
        snapshot["files"][fname] = entry

    os.makedirs(os.path.dirname(os.path.abspath(snapshot_path)), exist_ok=True)
    with open(snapshot_path, "w", encoding="utf-8") as fh:
        json.dump(snapshot, fh, indent=2, ensure_ascii=False)

    _console.print(
        f"[bold green]✓ Snapshot opgeslagen:[/bold green] {snapshot_path} "
        f"[dim]({len(files)} bestanden)[/dim]"
    )


@with_storage
def validate_snapshot(
    storage, output_dir: str, snapshot_path: str, *, strict_files: bool = False,
) -> tuple[bool, list[str], list[str]]:
    """Compare *output_dir* against a saved snapshot.

    Returns ``(passed, errors, warnings)``.
    *passed* is True only when *errors* is empty. Unreadable files always fail.
    With ``strict_files=True`` an unexpected file fails instead of warning; the
    CI workflows pass ``--strict-files`` against the version 2 baselines.
    A legacy snapshot without hashes still checks schema, with a warning.
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
        os.path.basename(f): f for f in current_files
    }
    expected_names = set(expected["files"].keys())
    current_names = set(current_map.keys())

    for fname in sorted(expected_names - current_names):
        errors.append(f"Bestand ontbreekt: {fname}")
    for fname in sorted(current_names - expected_names):
        (errors if strict_files else warnings).append(f"Onverwacht bestand aanwezig: {fname}")

    for fname, exp in sorted(expected["files"].items()):
        if fname not in current_map:
            continue

        if "read_error" in exp:
            errors.append(f"{fname}: snapshot bevat een leesfout; maak een geldige baseline")
            continue
        cur = _scan_file(storage, current_map[fname])
        if "read_error" in cur:
            errors.append(f"{fname}: bestand onleesbaar: {cur['read_error']}")
            continue
        hash_key = _hash_key(fname)
        hash_label = "data-hash" if hash_key == "data_hash" else "SHA256"
        if hash_key in exp:
            if cur.get(hash_key) != exp[hash_key]:
                errors.append(f"{fname}: inhoud gewijzigd ({hash_label} verschilt)")
        elif expected.get("snapshot_version", 1) >= 2:
            errors.append(f"{fname}: {hash_label} ontbreekt in de snapshot")
        else:
            warnings.append(f"{fname}: oude snapshot zonder inhoudhash; alleen schema wordt gecontroleerd")

        for key in ("row_count", "column_count"):
            if key in exp and cur.get(key) != exp[key]:
                label = "rijen" if key == "row_count" else "kolommen"
                errors.append(
                    f"{fname}: {label} {cur.get(key)} ≠ verwacht {exp[key]}"
                )

        if "columns" in exp and "columns" in cur:
            removed = sorted(set(exp["columns"]) - set(cur["columns"]))
            added = sorted(set(cur["columns"]) - set(exp["columns"]))
            if removed:
                errors.append(f"{fname}: kolomnamen verwijderd: {removed}")
            if added:
                errors.append(f"{fname}: kolomnamen toegevoegd: {added}")

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
    parser.add_argument("--strict-files", action="store_true",
                        help="Onverwachte bestanden zijn een fout in plaats van een waarschuwing")
    args = parser.parse_args()

    if args.generate:
        _console.print(f"[bold]Snapshot genereren van:[/bold] {args.output}")
        generate_snapshot(args.output, args.snapshot)
    else:
        _console.print(f"[bold]Snapshot valideren:[/bold] {args.output}")
        _console.print(f"[dim]Snapshot: {args.snapshot}[/dim]")
        passed, errors, warnings = validate_snapshot(
            args.output, args.snapshot, strict_files=args.strict_files
        )
        print_validation_result(passed, errors, warnings, args.snapshot)
        if not passed:
            raise SystemExit(1)


if __name__ == "__main__":
    main()

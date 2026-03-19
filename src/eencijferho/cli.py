# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
CLI entrypoint for the eencijferho package.

All commands require --input (source data directory) and --output (destination
directory).  Metadata (JSON, Excel files, logs) is always written to a
``metadata/`` subdirectory inside --output, so the full directory layout after
a complete pipeline run looks like:

    <output>/
        metadata/
            json/     ← extracted JSON from .txt files
            logs/     ← processing logs
            *.xlsx    ← Excel bestandsbeschrijvingen
        *.csv / *.parquet  ← converted data files

Usage:
    eencijferho extract          --input data/01-input  --output data/02-output
    eencijferho validate         --input data/01-input  --output data/02-output
    eencijferho convert          --input data/01-input  --output data/02-output
    eencijferho pipeline         --input data/01-input  --output data/02-output
    eencijferho validate-output  --input data/01-input  --output data/02-output
"""

import argparse
import os
from typing import Tuple
from rich.console import Console
from rich.panel import Panel
from eencijferho.core.extractor import (
    process_txt_folder,
    write_variable_metadata,
    process_json_folder,
)
from eencijferho.utils.extractor_validation import validate_metadata_folder
from eencijferho.utils.converter_match import match_files
from eencijferho.core.pipeline import run_turbo_convert_pipeline
import eencijferho.utils.value_validation as vv

_console = Console()


def _resolve_dirs(output_dir: str) -> Tuple[str, str, str]:
    """Return (metadata_dir, json_dir, logs_dir) derived from output_dir."""
    metadata_dir = os.path.join(output_dir, "metadata")
    json_dir = os.path.join(metadata_dir, "json")
    logs_dir = os.path.join(metadata_dir, "logs")
    return metadata_dir, json_dir, logs_dir


def cmd_extract(args: argparse.Namespace) -> None:
    """Extract metadata from input files into <output>/metadata/."""
    metadata_dir, json_dir, _ = _resolve_dirs(args.output)
    print(f"[eencijferho] Extracting metadata from: {args.input}")
    print(f"[eencijferho] Writing metadata to:      {metadata_dir}")
    process_txt_folder(args.input, json_output_folder=json_dir)
    write_variable_metadata(input_dir=args.input, json_folder=json_dir)
    process_json_folder(json_input_folder=json_dir, excel_output_folder=metadata_dir)
    print("[eencijferho] Extraction complete.")


def cmd_validate(args: argparse.Namespace) -> None:
    """Validate extracted metadata and match input files."""
    metadata_dir, _, logs_dir = _resolve_dirs(args.output)
    validation_log = os.path.join(logs_dir, "(3)_xlsx_validation_log_latest.json")
    print(f"[eencijferho] Validating metadata in: {metadata_dir}")
    validate_metadata_folder(metadata_folder=metadata_dir)
    match_files(args.input, log_path=validation_log)
    print("[eencijferho] Validation complete.")


def cmd_validate_output(args: argparse.Namespace) -> None:
    """Validate converted output files against variable_metadata.json allowed values."""
    metadata_dir, json_dir, logs_dir = _resolve_dirs(args.output)
    import json, datetime, os as _os

    print(f"[eencijferho] Validating output files in: {args.output}")

    variable_metadata_path = _os.path.join(json_dir, "variable_metadata.json")
    if not _os.path.isfile(variable_metadata_path):
        print("[eencijferho] variable_metadata.json niet gevonden. Voer eerst 'extract' uit.")
        return

    val_summary = vv.validate_column_values_folder(args.output, variable_metadata_path)

    failed_cols = [
        (fname, col["column"], col["invalid_values"])
        for fname, res in val_summary.items()
        for col in res["results"].get("column_results", [])
        if col["status"] == "failed"
    ]

    val_log = {
        "timestamp": datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
        "status": "completed",
        "total_files_checked": len(val_summary),
        "total_failed_columns": len(failed_cols),
        "details": {
            fname: res["results"]
            for fname, res in val_summary.items()
            if res["results"].get("columns_checked", 0) > 0
        },
    }
    _os.makedirs(logs_dir, exist_ok=True)
    log_path = _os.path.join(logs_dir, "(5b)_value_validation_log_latest.json")
    with open(log_path, "w", encoding="utf-8") as fh:
        json.dump(val_log, fh, ensure_ascii=False, indent=2)
    print(f"[eencijferho] Kolomwaarden validatie log opgeslagen: {log_path}")

    failures = vv.read_value_validation_log(log_path)
    if not failures:
        _console.print("[green]Kolomwaarden validatie: alle kolommen OK[/green]")
        return

    lines = ["[bold]Let op: de volgende kolommen bevatten ongeldige waarden:[/bold]"]
    for f in failures:
        vals = ", ".join(str(v) for v in f["invalid_values"][:5])
        if len(f["invalid_values"]) > 5:
            vals += f" ... (+{len(f['invalid_values']) - 5} meer)"
        lines.append(
            f"  • [bold]{f['file']}[/bold] → kolom [yellow]{f['column']}[/yellow]: {vals}"
        )
    _console.print(Panel("\n".join(lines), title="⚠️  Kolomwaarden validatie", border_style="yellow"))


def cmd_convert(args: argparse.Namespace) -> None:
    """Run the full turbo convert pipeline."""
    metadata_dir, _, logs_dir = _resolve_dirs(args.output)
    print(f"[eencijferho] Running turbo convert pipeline: {args.input} → {args.output}")
    log, output_files = run_turbo_convert_pipeline(
        input_dir=args.input,
        output_dir=args.output,
        metadata_dir=metadata_dir,
    )
    print(log)
    print(f"[eencijferho] Output files: {len(output_files)}")
    for f in output_files:
        print(f"  - {f['name']} ({f['size_formatted']})")


def cmd_pipeline(args: argparse.Namespace) -> None:
    """Run the complete end-to-end pipeline (extract → validate → convert)."""
    metadata_dir, json_dir, logs_dir = _resolve_dirs(args.output)
    validation_log = os.path.join(logs_dir, "(3)_xlsx_validation_log_latest.json")

    print(f"[eencijferho] Running full pipeline: {args.input} → {args.output}")
    print(f"[eencijferho] Metadata dir: {metadata_dir}")

    # Step 1: Extract
    print("[eencijferho] Step 1/3: Extracting metadata...")
    process_txt_folder(args.input, json_output_folder=json_dir)
    write_variable_metadata(input_dir=args.input, json_folder=json_dir)
    process_json_folder(json_input_folder=json_dir, excel_output_folder=metadata_dir)

    # Step 2: Validate
    print("[eencijferho] Step 2/3: Validating metadata...")
    validate_metadata_folder(metadata_folder=metadata_dir)
    match_files(args.input, log_path=validation_log)

    # Step 3: Convert
    print("[eencijferho] Step 3/3: Converting files...")
    log, output_files = run_turbo_convert_pipeline(
        input_dir=args.input,
        output_dir=args.output,
        metadata_dir=metadata_dir,
    )
    print(log)
    print(f"[eencijferho] Pipeline complete. Output files: {len(output_files)}")


def main() -> None:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="eencijferho",
        description="eencijferho - 1CijferHO backend processing toolkit",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    _common = argparse.ArgumentParser(add_help=False)
    _common.add_argument("--input", required=True, help="Path to input directory")
    _common.add_argument("--output", required=True, help="Path to output directory")

    subparsers.add_parser(
        "extract",
        parents=[_common],
        help="Extract metadata from input files (writes to <output>/metadata/)",
    )
    subparsers.add_parser(
        "validate",
        parents=[_common],
        help="Validate extracted metadata and match input files",
    )
    subparsers.add_parser(
        "convert",
        parents=[_common],
        help="Run turbo convert pipeline (requires prior extract+validate)",
    )
    subparsers.add_parser(
        "pipeline",
        parents=[_common],
        help="Run complete end-to-end pipeline (extract → validate → convert)",
    )
    subparsers.add_parser(
        "validate-output",
        parents=[_common],
        help="Validate converted output files against allowed values from bestandsbeschrijving (run after pipeline)",
    )

    args = parser.parse_args()

    dispatch = {
        "extract": cmd_extract,
        "validate": cmd_validate,
        "convert": cmd_convert,
        "pipeline": cmd_pipeline,
        "validate-output": cmd_validate_output,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()

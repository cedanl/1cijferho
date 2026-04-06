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
import datetime
import glob as _glob
import json
import os

import polars as pl
from rich.console import Console
from rich.panel import Panel

from eencijferho.core.decoder import (
    decode_fields,
    decode_fields_dec_only,
    load_dec_tables_from_metadata,
    load_variable_mappings,
)
from eencijferho.core.extractor import (
    process_txt_folder,
    write_variable_metadata,
    process_json_folder,
)
from eencijferho.core.pipeline import run_turbo_convert_pipeline
from eencijferho.config import OutputConfig
from eencijferho.utils.converter_headers import clean_header_name, normalize_name
from eencijferho.utils.converter_match import match_files
from eencijferho.utils.extractor_validation import validate_metadata_folder
import eencijferho.utils.value_validation as vv
import eencijferho.utils.dec_validation as dv

_console = Console()


def _resolve_dirs(output_dir: str) -> tuple[str, str, str]:
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
    """Validate converted output files: column values and DEC decoder files."""
    metadata_dir, json_dir, logs_dir = _resolve_dirs(args.output)
    os.makedirs(logs_dir, exist_ok=True)

    print(f"[eencijferho] Validating output files in: {args.output}")

    # --- 1. Value validation (variable_metadata.json) ---
    variable_metadata_path = os.path.join(json_dir, "variable_metadata.json")
    if not os.path.isfile(variable_metadata_path):
        print("[eencijferho] variable_metadata.json niet gevonden, kolomwaarden validatie overgeslagen.")
    else:
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
        val_log_path = os.path.join(logs_dir, "(5b)_value_validation_log_latest.json")
        with open(val_log_path, "w", encoding="utf-8") as fh:
            json.dump(val_log, fh, ensure_ascii=False, indent=2)
        print(f"[eencijferho] Kolomwaarden validatie log opgeslagen: {val_log_path}")

        val_failures = vv.read_value_validation_log(val_log_path)
        if not val_failures:
            _console.print("[green]Kolomwaarden validatie: alle kolommen OK[/green]")
        else:
            lines = ["[bold]Kolomwaarden: ongeldige waarden gevonden:[/bold]"]
            for f in val_failures:
                vals = ", ".join(str(v) for v in f["invalid_values"][:5])
                if len(f["invalid_values"]) > 5:
                    vals += f" ... (+{len(f['invalid_values']) - 5} meer)"
                lines.append(
                    f"  • [bold]{f['file']}[/bold] → kolom [yellow]{f['column']}[/yellow]: {vals}"
                )
            _console.print(Panel("\n".join(lines), title="⚠️  Kolomwaarden validatie", border_style="yellow"))

    # --- 2. DEC validation (Bestandsbeschrijving_Dec-bestanden) ---
    dec_txt_candidates = [
        f for f in os.listdir(args.input)
        if f.startswith("Bestandsbeschrijving_Dec") and f.endswith(".txt")
    ] if os.path.isdir(args.input) else []

    if not dec_txt_candidates:
        print("[eencijferho] Geen Bestandsbeschrijving_Dec*.txt gevonden, DEC validatie overgeslagen.")
    else:
        dec_txt_path = os.path.join(args.input, dec_txt_candidates[0])
        dec_summary = dv.validate_with_dec_files_folder(args.output, dec_txt_path)
        dec_failed = [
            (fname, col["column"], col["dec_file"], col["invalid_values"])
            for fname, res in dec_summary.items()
            for col in res["results"].get("column_results", [])
            if col["status"] == "failed"
        ]
        dec_log = {
            "timestamp": datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "status": "completed",
            "total_files_checked": len(dec_summary),
            "total_failed_columns": len(dec_failed),
            "details": {
                fname: res["results"]
                for fname, res in dec_summary.items()
                if res["results"].get("columns_checked", 0) > 0
            },
        }
        dec_log_path = os.path.join(logs_dir, "(5c)_dec_validation_log_latest.json")
        with open(dec_log_path, "w", encoding="utf-8") as fh:
            json.dump(dec_log, fh, ensure_ascii=False, indent=2)
        print(f"[eencijferho] DEC validatie log opgeslagen: {dec_log_path}")

        dec_failures = dv.read_dec_validation_log(dec_log_path)
        if not dec_failures:
            _console.print("[green]DEC validatie: alle kolommen OK[/green]")
        else:
            lines = ["[bold]DEC: ongeldige waarden gevonden:[/bold]"]
            for f in dec_failures:
                vals = ", ".join(str(v) for v in f["invalid_values"][:5])
                if len(f["invalid_values"]) > 5:
                    vals += f" ... (+{len(f['invalid_values']) - 5} meer)"
                lines.append(
                    f"  • [bold]{f['file']}[/bold] → kolom [yellow]{f['column']}[/yellow]"
                    f" (via {f['dec_file']}): {vals}"
                )
            _console.print(Panel("\n".join(lines), title="⚠️  DEC validatie", border_style="yellow"))


def cmd_decode(args: argparse.Namespace) -> None:
    """Decode CSV files using Dec_* lookup tables (Dec-only, no label substitution)."""
    _, json_dir, _ = _resolve_dirs(args.output)
    dec_json_matches = _glob.glob(
        os.path.join(json_dir, "Bestandsbeschrijving_Dec-bestanden*.json")
    )
    if not dec_json_matches:
        print("[eencijferho] Geen Bestandsbeschrijving_Dec-bestanden JSON gevonden. Eerst 'extract' uitvoeren.")
        return

    dec_metadata_json = dec_json_matches[0]
    dec_tables = load_dec_tables_from_metadata(dec_metadata_json, args.output)

    count = 0
    for fname in os.listdir(args.output):
        if (
            (fname.startswith("EV") or fname.startswith("VAKHAVW"))
            and fname.endswith(".csv")
            and not fname.endswith("_decoded.csv")
        ):
            in_path = os.path.join(args.output, fname)
            df = pl.read_csv(in_path, separator=";", encoding="utf-8")
            decoded_df = decode_fields_dec_only(df, dec_metadata_json, dec_tables)
            out_path = in_path.replace(".csv", "_decoded.csv")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(decoded_df.write_csv(separator=";"))
            print(f"[eencijferho] Gedecodeerd: {fname} → {os.path.basename(out_path)}")
            count += 1
    print(f"[eencijferho] {count} bestand(en) gedecodeerd.")


def cmd_enrich(args: argparse.Namespace) -> None:
    """Apply variable_metadata label substitution to decoded CSV files.

    Skips decode_fields entirely when no variable_metadata mappings apply to
    the columns of a given file (avoids unnecessary computation on large files).
    """
    _, json_dir, _ = _resolve_dirs(args.output)
    dec_json_matches = _glob.glob(
        os.path.join(json_dir, "Bestandsbeschrijving_Dec-bestanden*.json")
    )
    if not dec_json_matches:
        print("[eencijferho] Geen Bestandsbeschrijving_Dec-bestanden JSON gevonden. Eerst 'extract' uitvoeren.")
        return

    dec_metadata_json = dec_json_matches[0]
    dec_tables = load_dec_tables_from_metadata(dec_metadata_json, args.output)
    variable_metadata_json = os.path.join(json_dir, "variable_metadata.json")
    var_maps = load_variable_mappings(variable_metadata_json)

    written = skipped = 0
    for fname in os.listdir(args.output):
        if not (fname.endswith("_decoded.csv") and not fname.endswith("_decoded_encrypted.csv")):
            continue
        in_path = os.path.join(args.output, fname)
        base = in_path.replace("_decoded.csv", ".csv")
        if not os.path.exists(base):
            continue
        main_df = pl.read_csv(base, separator=";", encoding="utf-8")
        normalized_cols = {normalize_name(clean_header_name(c)) for c in main_df.columns}
        if not (var_maps and normalized_cols & set(var_maps.keys())):
            print(f"[eencijferho] Overgeslagen (geen mappings): {fname}")
            skipped += 1
            continue
        decoded_df = pl.read_csv(in_path, separator=";", encoding="utf-8")
        enriched_df = decode_fields(
            main_df, dec_metadata_json, dec_tables,
            variable_metadata_path=variable_metadata_json,
        )
        if enriched_df.equals(decoded_df):
            print(f"[eencijferho] Overgeslagen (identiek aan decoded): {fname}")
            skipped += 1
            continue
        out_path = in_path.replace("_decoded.csv", "_enriched.csv")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(enriched_df.write_csv(separator=";"))
        print(f"[eencijferho] Verrijkt: {fname} → {os.path.basename(out_path)}")
        written += 1
    print(f"[eencijferho] {written} verrijkt, {skipped} overgeslagen.")


def _build_output_config(args: argparse.Namespace) -> OutputConfig:
    """Build an OutputConfig from CLI flags."""
    variants = []
    if not args.skip_decode:
        variants.append("decoded")
        if not args.skip_enrich:
            variants.append("enriched")
    formats = [] if args.skip_parquet else ["parquet"]
    return OutputConfig(
        variants=variants,
        formats=formats,
        encrypt=not args.skip_encrypt,
        column_casing="none" if args.skip_snake_case else "snake_case",
        convert_ev=not args.skip_ev,
        convert_vakhavw=not args.skip_vakhavw,
        decode_columns=args.decode_columns or None,
        enrich_variables=args.enrich_variables or None,
    )


def cmd_convert(args: argparse.Namespace) -> None:
    """Run the full turbo convert pipeline."""
    metadata_dir, _, _ = _resolve_dirs(args.output)
    print(f"[eencijferho] Running turbo convert pipeline: {args.input} → {args.output}")
    log, output_files = run_turbo_convert_pipeline(
        input_dir=args.input,
        output_dir=args.output,
        metadata_dir=metadata_dir,
        output_config=_build_output_config(args),
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
        output_config=_build_output_config(args),
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

    _output_opts = argparse.ArgumentParser(add_help=False)
    _output_opts.add_argument(
        "--skip-decode", action="store_true",
        help="Do not create _decoded CSV variants",
    )
    _output_opts.add_argument(
        "--skip-enrich", action="store_true",
        help="Do not create _enriched CSV variants",
    )
    _output_opts.add_argument(
        "--skip-parquet", action="store_true",
        help="Do not compress output to Parquet",
    )
    _output_opts.add_argument(
        "--skip-encrypt", action="store_true",
        help="Do not encrypt sensitive columns",
    )
    _output_opts.add_argument(
        "--skip-snake-case", action="store_true",
        help="Keep original column names (do not convert to snake_case)",
    )
    _output_opts.add_argument(
        "--skip-ev", action="store_true",
        help="Do not convert EV main data files",
    )
    _output_opts.add_argument(
        "--skip-vakhavw", action="store_true",
        help="Do not convert VAKHAVW main data files",
    )
    _output_opts.add_argument(
        "--decode-columns", nargs="*", metavar="KOLOM", default=None,
        help="Alleen deze kolommen decoderen via Dec_*-opzoekbestanden (standaard: alle). "
             "Gebruik get_available_decode_columns() om geldige namen te achterhalen.",
    )
    _output_opts.add_argument(
        "--enrich-variables", nargs="*", metavar="VARIABELE", default=None,
        help="Alleen deze variabelen verrijken via variable_metadata (standaard: alle). "
             "Gebruik get_available_enrich_variables() om geldige namen te achterhalen.",
    )

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
        "decode",
        parents=[_common],
        help="Decode output CSV files using Dec_* lookup tables (run after convert)",
    )
    subparsers.add_parser(
        "enrich",
        parents=[_common],
        help="Apply variable_metadata labels to decoded files; skips if identical to _decoded",
    )
    subparsers.add_parser(
        "convert",
        parents=[_common, _output_opts],
        help="Run turbo convert pipeline (requires prior extract+validate)",
    )
    subparsers.add_parser(
        "pipeline",
        parents=[_common, _output_opts],
        help="Run complete end-to-end pipeline (extract → validate → convert)",
    )
    subparsers.add_parser(
        "validate-output",
        parents=[_common],
        help="Validate converted output: column values + DEC decoder files (run after pipeline)",
    )

    args = parser.parse_args()

    dispatch = {
        "extract": cmd_extract,
        "validate": cmd_validate,
        "decode": cmd_decode,
        "enrich": cmd_enrich,
        "convert": cmd_convert,
        "pipeline": cmd_pipeline,
        "validate-output": cmd_validate_output,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()

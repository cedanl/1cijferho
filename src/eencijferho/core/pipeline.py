"""
Modular pipeline orchestrator: conversion → decoding → validation → BSN translation (opt.) → header normalization → compression.
"""

import os
from eencijferho.config import OutputConfig, DECODED_SUFFIX, ENRICHED_SUFFIX
import polars as pl
from eencijferho.core import converter, decoder
from eencijferho.core.decode_report import DecodeReport
import eencijferho.utils.converter_validation as cv
import eencijferho.utils.compressor as co
import eencijferho.utils.converter_headers as ch
import eencijferho.utils.translator as tr
from collections.abc import Callable
from typing import Any

from eencijferho.io.decorators import with_storage


def _is_main_csv_file(filename: str) -> bool:
    """Check if filename is a main CSV file (not already decoded/enriched)."""
    is_main_type = filename.startswith("EV") or filename.startswith("VAKHAVW")
    is_csv = filename.endswith(".csv")
    not_processed = not (filename.endswith(DECODED_SUFFIX) or filename.endswith(ENRICHED_SUFFIX))
    return is_main_type and is_csv and not_processed

def _process_enriched_file(
    storage, main_df: pl.DataFrame, filepath: str, filename: str, log: str,
    dec_metadata_json: str, dec_tables: dict, variable_metadata_json: str,
    var_maps: dict, output_config: OutputConfig, dec_only_df: pl.DataFrame | None,
    vakken_metadata_json: str | None = None,
    report: DecodeReport | None = None,
) -> str:
    """Process and write enriched file; return updated log."""
    normalized_cols = {ch.normalize_name(ch.clean_header_name(c)) for c in main_df.columns}

    if not (var_maps and normalized_cols & set(var_maps.keys())):
        log += f"[pipeline] {filename}: geen variable_metadata mappings, _enriched overgeslagen.\n"
        if report is not None:
            report.set_file_status(enriched="skipped_no_mappings")
        return log

    enriched_df = decoder.decode_fields(
        main_df, dec_metadata_json, dec_tables,
        variable_metadata_path=variable_metadata_json,
        decode_columns=output_config.decode_columns,
        enrich_variables=output_config.enrich_variables,
        vakken_metadata_json=vakken_metadata_json,
        report=report,
    )

    if dec_only_df is None or not enriched_df.equals(dec_only_df):
        enriched_file = filepath.replace(".csv", ENRICHED_SUFFIX)
        storage.write_text(enriched_df.write_csv(separator=";"), enriched_file)
        status = "written"
    else:
        log += f"[pipeline] {filename}: _enriched identiek aan _decoded, overgeslagen.\n"
        status = "skipped_identical_to_decoded"
    if report is not None:
        report.set_file_status(enriched=status)

    return log

def _collect_output_files(storage, output_dir: str) -> list[dict[str, Any]]:
    """Collect and format output file information."""
    output_files = []
    all_output = storage.list_files(f"{output_dir}/*")

    for filepath in all_output:
        filename = os.path.basename(filepath)
        try:
            size = len(storage.read_bytes(filepath))
        except Exception:
            size = 0

        output_files.append({
            "name": filename,
            "size": size,
            "size_formatted": f"{size / 1024:.1f} KB",
        })

    return output_files

@with_storage
def run_turbo_convert_pipeline(
    storage,
    input_dir: str = "data/01-input",
    dec_metadata_json: str | None = None,
    output_dir: str = "data/02-output",
    metadata_dir: str | None = None,
    progress_callback: Callable[[int], None] | None = None,
    status_callback: Callable[[str], None] | None = None,
    output_config: OutputConfig | None = None,
    decode_report: DecodeReport | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Run the full turbo-convert pipeline.

    Args:
        input_dir: Folder containing input fixed-width / CSV files.
        dec_metadata_json: Path to the DEC bestandsbeschrijving JSON.  When
            *None* it is derived automatically from *metadata_dir*.
        output_dir: Folder where converted files are written.
        metadata_dir: Folder used for metadata (JSON, Excel, logs).  When
            *None* it defaults to ``data/00-metadata`` (legacy behaviour).
        progress_callback: Optional callable(int) for progress 0-100.
        status_callback: Optional callable(str) for status messages.
        output_config: Controls which output variants are produced.  When
            *None* the defaults from :class:`OutputConfig` are used
            (decoded + enriched + parquet + snake_case).
        decode_report: Collector for the decode log
            ``<metadata_dir>/logs/(7)_decoding_log_latest.json``.  When *None*
            the pipeline uses its own; pass one to read the findings after
            the run (e.g. for ``--strict``).
    """
    if output_config is None:
        output_config = OutputConfig()
    if metadata_dir is None:
        from eencijferho.config import METADATA_DIR
        metadata_dir = METADATA_DIR
    if dec_metadata_json is None:
        json_dir = os.path.join(metadata_dir, "json")
        matches = storage.list_files(f"{json_dir}/Bestandsbeschrijving_Dec-bestanden*.json")
        dec_metadata_json = matches[0] if matches else None
    logs_dir = os.path.join(metadata_dir, "logs")
    # The Vakkenbestanden JSON sits next to the Dec JSON (both from the extract step).
    vakken_metadata_json = decoder.find_vakken_metadata_json(
        os.path.dirname(dec_metadata_json) if dec_metadata_json
        else os.path.join(metadata_dir, "json")
    )

    variable_metadata_json = os.path.join(metadata_dir, "json", "variable_metadata.json")
    log = ""
    # Step 1: Convert files
    if status_callback:
        status_callback("⚡ Bestanden omzetten...")
    log += "[pipeline] Bestanden omzetten...\n"
    match_log_file = os.path.join(logs_dir, "(4)_file_matching_log_latest.json")
    skip_prefixes = []
    if not output_config.convert_ev:
        skip_prefixes.append("EV")
        log += "[pipeline] EV-omzetting overgeslagen.\n"
    if not output_config.convert_vakhavw:
        skip_prefixes.append("VAKHAVW")
        log += "[pipeline] VAKHAVW-omzetting overgeslagen.\n"
    skip_prefixes = skip_prefixes or None
    summary = converter.run_conversions_from_matches(
        input_dir,
        metadata_folder=metadata_dir,
        match_log_file=match_log_file,
        output_folder=output_dir,
        skip_prefixes=skip_prefixes,
    )
    # A match log with layout errors must stop the run, not yield output without EV files.
    if isinstance(summary, dict) and summary.get("match_log_status", "completed") != "completed":
        raise converter.MatchLogNotCompletedError(summary["reason"])
    for item in summary.get("unsupported_files", []) if isinstance(summary, dict) else []:
        log += f"[pipeline] {item['input_file']}: {item['reason']}\n"
    converter.convert_dec_files(input_dir, metadata_folder=metadata_dir, output_folder=output_dir)
    log += "[pipeline] Omzetting voltooid.\n"
    if progress_callback:
        progress_callback(30)
    # Step 2: Decode main files
    do_decode = "decoded" in output_config.variants
    do_enrich = "enriched" in output_config.variants
    if do_decode or do_enrich:
        if status_callback:
            status_callback("🔤 Gedecodeerde bestanden aanmaken...")
        log += "[pipeline] Gedecodeerde bestanden aanmaken...\n"
    dec_dir = output_dir
    decoded_count = 0
    report = decode_report if decode_report is not None else DecodeReport()
    report.decode_enabled = do_decode
    report.enrich_enabled = do_enrich
    report.dec_metadata_file = os.path.basename(dec_metadata_json) if dec_metadata_json else None
    # Only a run that decodes reports its Dec tables and mappings.
    step_report = report if (do_decode or do_enrich) else None

    # Load dec_tables and variable_mappings once — shared across all files
    dec_tables = decoder.load_dec_tables_from_metadata(
        dec_metadata_json, dec_dir, vakken_metadata_json=vakken_metadata_json, report=step_report,
    )
    var_maps = decoder.load_variable_mappings(variable_metadata_json, report=step_report)

    if do_decode or do_enrich:
        if dec_metadata_json is None:
            # Not reached: load_dec_tables_from_metadata above already fails
            # without it (a TypeError from the disk backend).
            raise TypeError("Geen Bestandsbeschrijving_Dec-bestanden JSON gevonden")
        csv_files = storage.list_files(f"{dec_dir}/*.csv")
        for filepath in csv_files:
            filename = os.path.basename(filepath)
            if not _is_main_csv_file(filename):
                continue

            main_df = storage.read_dataframe(filepath, format="csv", infer_schema_length=0)
            report.begin_file(filename, main_df.columns)
            report.set_file_status(
                decoded="written" if do_decode else "not_requested",
                enriched="not_requested",
            )

            if do_decode:
                dec_only_df = decoder.decode_fields_dec_only(
                    main_df, dec_metadata_json, dec_tables,
                    decode_columns=output_config.decode_columns,
                    vakken_metadata_json=vakken_metadata_json,
                    report=report,
                )
                dec_only_file = filepath.replace(".csv", DECODED_SUFFIX)
                storage.write_text(dec_only_df.write_csv(separator=";"), dec_only_file)
            else:
                dec_only_df = None

            if do_enrich:
                log = _process_enriched_file(
                    storage, main_df, filepath, filename, log,
                    dec_metadata_json, dec_tables, variable_metadata_json,
                    var_maps, output_config, dec_only_df,
                    vakken_metadata_json=vakken_metadata_json, report=report,
                )

            decoded_count += 1
    if do_decode or do_enrich:
        log += f"[pipeline] {decoded_count} bestand(en) gedecodeerd.\n"
    decode_log_file = report.write(storage, logs_dir)
    log += f"[pipeline] Decodeerlog opgeslagen: {decode_log_file}\n"
    if progress_callback:
        progress_callback(40)
    # Step 3: Validate conversion
    if status_callback:
        status_callback("🔍 Conversie controleren...")
    log += "[pipeline] Conversie controleren...\n"
    cv.converter_validation(
        conversion_log_path=os.path.join(logs_dir, "(5)_conversion_log_latest.json"),
        matching_log_path=os.path.join(logs_dir, "(4)_file_matching_log_latest.json"),
        output_log_path=os.path.join(logs_dir, "(6)_conversion_validation_log_latest.json"),
    )
    log += "[pipeline] Controle voltooid.\n"
    if progress_callback:
        progress_callback(50)
    # Step 3.5: BSN → lokaal ID koppelen (optioneel)
    if output_config.bsn_mapping_file:
        if status_callback:
            status_callback("🔗 Studentnummers koppelen...")
        log += "[pipeline] Studentnummers koppelen...\n"
        log += tr.translate_bsn_to_local_id(
            output_dir=output_dir,
            mapping_file=output_config.bsn_mapping_file,
            mapping_bsn_col=output_config.bsn_mapping_right_on,
            mapping_id_col=output_config.bsn_mapping_id_col,
        )
        log += "[pipeline] Koppeling voltooid.\n"
        if progress_callback:
            progress_callback(55)
    if progress_callback:
        progress_callback(75)
    # Step 5: Header normalization
    if output_config.column_casing == "snake_case":
        if status_callback:
            status_callback("🔨 Kolomnamen standaardiseren...")
        log += "[pipeline] Kolomnamen standaardiseren...\n"
        ch.convert_csv_headers_to_snake_case(output_dir)
        log += "[pipeline] Kolomnamen gestandaardiseerd.\n"
    if progress_callback:
        progress_callback(90)
    # Step 6: Compress to Parquet
    # After step 5, so the parquet carries the same column names as the CSV.
    if "parquet" in output_config.formats:
        if status_callback:
            status_callback("🗜️ Bestanden comprimeren...")
        log += "[pipeline] Bestanden comprimeren...\n"
        co.convert_csv_to_parquet(output_dir)
        log += "[pipeline] Compressie voltooid.\n"
    if progress_callback:
        progress_callback(100)

    output_files = _collect_output_files(storage, output_dir)
    return log, output_files

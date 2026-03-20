"""
Modular pipeline orchestrator for conversion, decoding, validation, compression, encryption, header normalization.
"""

import glob
import json
import datetime
import os
from eencijferho.core import converter, decoder
import eencijferho.utils.converter_validation as cv
import eencijferho.utils.compressor as co
import eencijferho.utils.encryptor as en
import eencijferho.utils.converter_headers as ch
from typing import Any, Callable, Dict, List, Tuple, Optional


def run_turbo_convert_pipeline(
    input_dir: str = "data/01-input",
    dec_metadata_json: str | None = None,
    output_dir: str = "data/02-output",
    metadata_dir: str | None = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    status_callback: Optional[Callable[[str], None]] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
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
    """
    # Resolve metadata_dir and dec_metadata_json from arguments
    if metadata_dir is None:
        from eencijferho.config import METADATA_DIR
        metadata_dir = METADATA_DIR
    if dec_metadata_json is None:
        json_dir = os.path.join(metadata_dir, "json")
        matches = glob.glob(os.path.join(json_dir, "Bestandsbeschrijving_Dec-bestanden*.json"))
        dec_metadata_json = matches[0] if matches else None
    logs_dir = os.path.join(metadata_dir, "logs")

    variable_metadata_json = os.path.join(metadata_dir, "json", "variable_metadata.json")
    log = ""
    # Step 1: Convert files
    if status_callback:
        status_callback("⚡ Bestanden omzetten...")
    log += "[pipeline] Bestanden omzetten...\n"
    match_log_file = os.path.join(logs_dir, "(4)_file_matching_log_latest.json")
    converter.run_conversions_from_matches(
        input_dir,
        metadata_folder=metadata_dir,
        match_log_file=match_log_file,
        output_folder=output_dir,
    )
    converter.convert_dec_files(input_dir, metadata_folder=metadata_dir, output_folder=output_dir)
    log += "[pipeline] Omzetting voltooid.\n"
    if progress_callback:
        progress_callback(30)
    # Step 2: Decode main files
    if status_callback:
        status_callback("🔤 Gedecodeerde bestanden aanmaken...")
    log += "[pipeline] Gedecodeerde bestanden aanmaken...\n"
    dec_dir = output_dir
    os.makedirs(dec_dir, exist_ok=True)
    decoded_count = 0

    # Load dec_tables and variable_mappings once — shared across all files
    dec_tables = decoder.load_dec_tables_from_metadata(dec_metadata_json, dec_dir)
    var_maps = decoder.load_variable_mappings(variable_metadata_json)

    if os.path.isdir(dec_dir):
        for file in os.listdir(dec_dir):
            if (
                (file.startswith("EV") or file.startswith("VAKHAVW"))
                and file.endswith(".csv")
                and not file.endswith("_decoded.csv")
            ):
                file_path = os.path.join(dec_dir, file)
                main_df = decoder.pl.read_csv(file_path, separator=";", encoding="utf-8")

                # DEC-only decode
                dec_only_df = decoder.decode_fields_dec_only(
                    main_df, dec_metadata_json, dec_tables
                )
                dec_only_file = file_path.replace(".csv", "_decoded.csv")
                with open(dec_only_file, "w", encoding="utf-8") as f:
                    f.write(dec_only_df.write_csv(separator=";"))

                # Enriched decode:
                # 1. Skip entirely when no column names overlap with var_maps (fast path).
                # 2. When overlap exists, compute decode_fields and only write when the
                #    result actually differs from _decoded (correct, no redundant files).
                normalized_cols = {
                    ch.normalize_name(ch.clean_header_name(c)) for c in main_df.columns
                }
                if not (var_maps and normalized_cols & set(var_maps.keys())):
                    log += f"[pipeline] {os.path.basename(file_path)}: geen variable_metadata mappings, _enriched overgeslagen.\n"
                else:
                    enriched_df = decoder.decode_fields(
                        main_df, dec_metadata_json, dec_tables,
                        variable_metadata_path=variable_metadata_json,
                    )
                    if not enriched_df.equals(dec_only_df):
                        enriched_file = file_path.replace(".csv", "_enriched.csv")
                        with open(enriched_file, "w", encoding="utf-8") as f:
                            f.write(enriched_df.write_csv(separator=";"))
                    else:
                        log += f"[pipeline] {os.path.basename(file_path)}: _enriched identiek aan _decoded, overgeslagen.\n"

                decoded_count += 1
    log += f"[pipeline] {decoded_count} bestand(en) gedecodeerd.\n"
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
    # Step 4: Compress to Parquet
    if status_callback:
        status_callback("🗜️ Bestanden comprimeren...")
    log += "[pipeline] Bestanden comprimeren...\n"
    co.convert_csv_to_parquet(output_dir)
    log += "[pipeline] Compressie voltooid.\n"
    if progress_callback:
        progress_callback(75)
    # Step 5: Encrypt final files
    if status_callback:
        status_callback("🔒 Gevoelige gegevens versleutelen...")
    log += "[pipeline] Gevoelige gegevens versleutelen...\n"
    en.encryptor(output_dir, output_dir)
    log += "[pipeline] Versleuteling voltooid.\n"
    if progress_callback:
        progress_callback(90)
    # Step 6: Header normalization
    if status_callback:
        status_callback("🔨 Kolomnamen standaardiseren...")
    log += "[pipeline] Kolomnamen standaardiseren...\n"
    ch.convert_csv_headers_to_snake_case(output_dir)
    log += "[pipeline] Kolomnamen gestandaardiseerd.\n"
    if progress_callback:
        progress_callback(100)
    # Collect output files
    output_files = []
    if os.path.isdir(output_dir):
        for file in os.listdir(output_dir):
            file_path = os.path.join(output_dir, file)
            if os.path.isfile(file_path):
                output_files.append(
                    {
                        "name": file,
                        "size": os.path.getsize(file_path),
                        "size_formatted": f"{os.path.getsize(file_path) / 1024:.1f} KB",
                    }
                )
    return log, output_files

"""
Modular pipeline orchestrator for conversion, decoding, validation, compression, encryption, header normalization.
"""

import os
from eencijferho.config import OutputConfig
import polars as pl
from eencijferho.core import converter, decoder
import eencijferho.utils.converter_validation as cv
import eencijferho.utils.compressor as co
import eencijferho.utils.encryptor as en
import eencijferho.utils.converter_headers as ch
from collections.abc import Callable
from typing import Any

from eencijferho.io.decorators import with_storage


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
            (decoded + enriched + parquet + encrypt + snake_case).
    """
    if output_config is None:
        output_config = OutputConfig()
    # Resolve metadata_dir and dec_metadata_json from arguments
    if metadata_dir is None:
        from eencijferho.config import METADATA_DIR
        metadata_dir = METADATA_DIR
    if dec_metadata_json is None:
        json_dir = os.path.join(metadata_dir, "json")
        matches = storage.list_files(f"{json_dir}/Bestandsbeschrijving_Dec-bestanden*.json")
        dec_metadata_json = matches[0] if matches else None
    logs_dir = os.path.join(metadata_dir, "logs")

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
    converter.run_conversions_from_matches(
        input_dir,
        metadata_folder=metadata_dir,
        match_log_file=match_log_file,
        output_folder=output_dir,
        skip_prefixes=skip_prefixes,
    )
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

    # Load dec_tables and variable_mappings once — shared across all files
    dec_tables = decoder.load_dec_tables_from_metadata(dec_metadata_json, dec_dir)
    var_maps = decoder.load_variable_mappings(variable_metadata_json)

    if do_decode or do_enrich:
        csv_files = storage.list_files(f"{dec_dir}/*.csv")
        for filepath in csv_files:
            filename = filepath.rsplit("/", 1)[-1] if "/" in filepath else filepath
            if not (
                (filename.startswith("EV") or filename.startswith("VAKHAVW"))
                and filename.endswith(".csv")
                and not filename.endswith("_decoded.csv")
            ):
                continue

            main_df = storage.read_dataframe(filepath, format="csv")

            if do_decode:
                # DEC-only decode
                dec_only_df = decoder.decode_fields_dec_only(
                    main_df, dec_metadata_json, dec_tables,
                    decode_columns=output_config.decode_columns,
                )
                dec_only_file = filepath.replace(".csv", "_decoded.csv")
                storage.write_text(dec_only_df.write_csv(separator=";"), dec_only_file)
            else:
                dec_only_df = None

            if do_enrich:
                normalized_cols = {
                    ch.normalize_name(ch.clean_header_name(c)) for c in main_df.columns
                }
                if not (var_maps and normalized_cols & set(var_maps.keys())):
                    log += f"[pipeline] {filename}: geen variable_metadata mappings, _enriched overgeslagen.\n"
                else:
                    enriched_df = decoder.decode_fields(
                        main_df, dec_metadata_json, dec_tables,
                        variable_metadata_path=variable_metadata_json,
                        decode_columns=output_config.decode_columns,
                        enrich_variables=output_config.enrich_variables,
                    )
                    if dec_only_df is None or not enriched_df.equals(dec_only_df):
                        enriched_file = filepath.replace(".csv", "_enriched.csv")
                        storage.write_text(enriched_df.write_csv(separator=";"), enriched_file)
                    else:
                        log += f"[pipeline] {filename}: _enriched identiek aan _decoded, overgeslagen.\n"

            decoded_count += 1
    if do_decode or do_enrich:
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
    if "parquet" in output_config.formats:
        if status_callback:
            status_callback("🗜️ Bestanden comprimeren...")
        log += "[pipeline] Bestanden comprimeren...\n"
        co.convert_csv_to_parquet(output_dir)
        log += "[pipeline] Compressie voltooid.\n"
    if progress_callback:
        progress_callback(75)
    # Step 5: Encrypt final files
    if output_config.encrypt:
        if status_callback:
            status_callback("🔒 Gevoelige gegevens versleutelen...")
        log += "[pipeline] Gevoelige gegevens versleutelen...\n"
        en.encryptor(output_dir, output_dir)
        log += "[pipeline] Versleuteling voltooid.\n"
    if progress_callback:
        progress_callback(90)
    # Step 6: Header normalization
    if output_config.column_casing == "snake_case":
        if status_callback:
            status_callback("🔨 Kolomnamen standaardiseren...")
        log += "[pipeline] Kolomnamen standaardiseren...\n"
        ch.convert_csv_headers_to_snake_case(output_dir)
        log += "[pipeline] Kolomnamen gestandaardiseerd.\n"
    if progress_callback:
        progress_callback(100)
    # Collect output files
    output_files = []
    all_output = storage.list_files(f"{output_dir}/*")
    for filepath in all_output:
        filename = filepath.rsplit("/", 1)[-1] if "/" in filepath else filepath
        try:
            size = len(storage.read_bytes(filepath))
        except Exception:
            size = 0
        output_files.append(
            {
                "name": filename,
                "size": size,
                "size_formatted": f"{size / 1024:.1f} KB",
            }
        )
    return log, output_files

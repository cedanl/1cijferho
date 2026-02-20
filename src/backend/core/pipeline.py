"""
Modular pipeline orchestrator for conversion, decoding, validation, compression, encryption, header normalization.
"""
import os
from backend.core import converter, decoder
import backend.utils.converter_validation as cv
import backend.utils.compressor as co
import backend.utils.encryptor as en
import backend.utils.converter_headers as ch

def run_turbo_convert_pipeline(input_dir="data/01-input", dec_metadata_json="data/00-metadata/json/Bestandsbeschrijving_Dec-bestanden_DEMO.json", output_dir="data/02-output", progress_callback=None, status_callback=None):
    log = ""
    # Step 1: Convert files
    if status_callback: status_callback("⚡ Stap 3: Converting fixed-width files...")
    log += "[pipeline] Converting files...\n"
    converter.run_conversions_from_matches(input_dir)
    converter.convert_dec_files(input_dir)
    log += "[pipeline] Conversion complete.\n"
    if progress_callback: progress_callback(30)
    # Step 2: Decode main files
    if status_callback: status_callback("🔤 Stap 3b: Decoding main files...")
    log += "[pipeline] Decoding main files...\n"
    dec_dir = output_dir
    decoded_count = 0
    for file in os.listdir(dec_dir):
        if (file.startswith("EV") or file.startswith("VAKHAVW")) and file.endswith(".csv") and not file.endswith("_decoded.csv"):
            file_path = os.path.join(dec_dir, file)
            main_df = decoder.pl.read_csv(file_path, separator=';', encoding='latin1')
            dec_tables = decoder.load_dec_tables_from_metadata(dec_metadata_json, dec_dir)
            decoded_df = decoder.decode_fields(main_df, dec_metadata_json, dec_tables)
            decoded_file = file_path.replace('.csv', '_decoded.csv')
            decoded_df.write_csv(decoded_file, separator=';')
            decoded_count += 1
    log += f"[pipeline] Decoding completed for {decoded_count} file(s).\n"
    if progress_callback: progress_callback(40)
    # Step 3: Validate conversion
    if status_callback: status_callback("🔍 Stap 4: Validating conversion results...")
    log += "[pipeline] Validating conversion...\n"
    cv.converter_validation()
    log += "[pipeline] Validation complete.\n"
    if progress_callback: progress_callback(50)
    # Step 4: Compress to Parquet
    if status_callback: status_callback("🗜️ Stap 5: Compressing to Parquet format...")
    log += "[pipeline] Compressing to Parquet...\n"
    co.convert_csv_to_parquet()
    log += "[pipeline] Compression complete.\n"
    if progress_callback: progress_callback(75)
    # Step 5: Encrypt final files
    if status_callback: status_callback("🔒 Stap 6: Encrypting final files...")
    log += "[pipeline] Encrypting files...\n"
    en.encryptor()
    log += "[pipeline] Encryption complete.\n"
    if progress_callback: progress_callback(90)
    # Step 6: Header normalization
    if status_callback: status_callback("🔨 Stap 7: Converteer headers naar snake_case...")
    log += "[pipeline] Normalizing headers...\n"
    ch.convert_csv_headers_to_snake_case()
    log += "[pipeline] Header normalization complete.\n"
    if progress_callback: progress_callback(100)
    # Collect output files
    output_files = []
    for file in os.listdir(output_dir):
        file_path = os.path.join(output_dir, file)
        if os.path.isfile(file_path):
            output_files.append({
                'name': file,
                'size': os.path.getsize(file_path),
                'size_formatted': f"{os.path.getsize(file_path)/1024:.1f} KB"
            })
    return log, output_files

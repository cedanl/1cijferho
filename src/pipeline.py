from backend.core import extractor as ex
from backend.utils import extractor_validation as ex_val
from backend.utils import converter_match as cm
from backend.utils import converter_validation as cv
from backend.utils import compressor as co
from backend.utils import encryptor as en
from backend.utils import converter_headers as ch
import subprocess
import argparse


def run_pipeline(input_folder):
    """Run the complete pipeline with the given input folder"""
    # Step 1: Extract Metadata from Bestandsbeschrijving files (txt -> JSON -> Excel)
    ex.process_txt_folder(input_folder)
    ex.process_json_folder()
    ex_val.validate_metadata_folder()
    
    # Step 2: Match Input Files to Metadata (Validation Logs)
    cm.match_files(input_folder)
    
    # Step 3: Convert Files
    subprocess.run(["uv", "run", "src/backend/core/converter.py", input_folder])
    
    # Step 4: Validate Conversion
    cv.converter_validation()
    
    # Step 5: Run Compressor
    co.convert_csv_to_parquet()

    # Step 6: Run Converter Headers (snake_case)
    ch.convert_csv_headers_to_snake_case()

    # Step 7: Run Encryptor
    en.encryptor()

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Process input files through the pipeline')
    parser.add_argument('input_folder', type=str, help='Path to the input folder')
    args = parser.parse_args()
    
    run_pipeline(args.input_folder)


if __name__ == "__main__":
    main()

# Gebruik: uv run src/pipeline.py "data/01-input/"
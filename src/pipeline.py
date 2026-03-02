from backend.core import extractor as ex
from backend.utils import extractor_validation as ex_val
from backend.utils import converter_match as cm
from backend.utils import converter_validation as cv
from backend.utils import compressor as co
from backend.utils import encryptor as en
from backend.utils import converter_headers as ch
import subprocess
import argparse
from config import INPUT_DIR, OUTPUT_DIR


def run_pipeline(input_folder: str | None = None, output_folder: str | None = None) -> None:
    """Run the complete pipeline with the given input folder"""
    if input_folder is None:
        input_folder = INPUT_DIR
    if output_folder is None:
        output_folder = OUTPUT_DIR
    # Step 1: Extract Metadata from Bestandsbeschrijving files (txt -> JSON -> Excel)
    ex.process_txt_folder(input_folder)
    ex.process_json_folder()
    ex_val.validate_metadata_folder()
    
    # Step 2: Match Input Files to Metadata (Validation Logs)
    cm.match_files(input_folder)
    
    # Step 3: Convert Files
    subprocess.run(["uv", "run", "src/backend/core/converter.py", input_folder, output_folder])
    
    # Step 4: Validate Conversion
    cv.converter_validation()
    
    # Step 5: Run Compressor
    co.convert_csv_to_parquet(output_folder)

    # Step 6: Run Encryptor
    en.encryptor(output_folder, output_folder)

    # Step 7: Run Converter Headers (snake_case)
    ch.convert_csv_headers_to_snake_case(output_folder)



def main() -> None:
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Process input files through the pipeline')
    parser.add_argument('input_folder', type=str, nargs='?', default=None, help='Path to the input folder (default: from config)')
    parser.add_argument('--output', type=str, default=None, dest='output_folder', help='Path to the output folder (default: from config)')
    args = parser.parse_args()
    
    run_pipeline(args.input_folder, args.output_folder)


if __name__ == "__main__":
    main()

# Gebruik: uv run src/pipeline.py  (uses config defaults)
# Gebruik: uv run src/pipeline.py "data/01-input/" --output "data/02-output/"

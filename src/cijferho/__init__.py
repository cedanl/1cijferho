"""
CijferHO Package - Professional tools for processing Dutch educational data (DUO files)
"""
import sys
sys.path.append('src/backend')

__version__ = "0.1.0"

# ============================================================================
# EXTRACTION - Metadata extraction from Bestandsbeschrijving files
# ============================================================================
from core.extractor import (
    extract_tables_from_txt,      # Extract tables from single .txt file
    process_txt_folder,            # Process all .txt files in folder
    extract_excel_from_json,       # Convert JSON to Excel
    process_json_folder            # Process all JSON files
)

# ============================================================================
# CONVERSION - Fixed-width file conversion
# ============================================================================
from core.converter import (
    process_chunk,                 # Process single chunk (internal)
    converter,                     # Convert single file
    run_conversions_from_matches   # Convert all matched files
)

# ============================================================================
# VALIDATION - File validation and matching
# ============================================================================
from utils.extractor_validation import (
    validate_metadata,             # Validate single Excel file
    validate_metadata_folder       # Validate entire folder
)

from utils.converter_validation import (
    converter_validation           # Validate conversion results
)

from utils.converter_match import (
    load_input_files,              # Load input file list
    load_validation_log,           # Load validation log
    match_files                    # Match input with validation files
)

# ============================================================================
# PROCESSING - Data compression and encryption
# ============================================================================
from utils.compressor import (
    convert_csv_to_parquet         # Compress CSV to Parquet
)

from utils.encryptor import (
    encryptor                      # Encrypt sensitive columns
)

# ============================================================================
# CASE HANDLING - Column name normalization
# ============================================================================
from core.case_utils import (
    to_snake_case,                 # Convert to snake_case
    normalize_column_names,        # Normalize all column names
    denormalize_column_names,      # Restore original case
    get_column_case_style,         # Detect case style
    create_column_mapping_report   # Create mapping report
)

# ============================================================================
# COMBINING - Join main data with decoder files
# ============================================================================
from core.combiner import (
    combine_all_data,              # Complete data combination
    load_yaml_config,              # Load YAML configuration
    load_main_data,                # Load processed main data
    load_decoder_data              # Load decoder files
)

# ============================================================================
# ENRICHMENT - Add calculated fields and variables
# ============================================================================
from core.enricher import (
    enrich_dataframe,              # Apply all enrichments
    enrich_all_data                # Process all files
)

from core.enricher_switch import (
    enrich_switch_data,            # Apply switch analysis
    enrich_all_switch_data         # Process all files with switches
)

# ============================================================================
# CONVENIENCE - Quick processing function
# ============================================================================
def quick_process(input_folder):
    """
    Complete processing pipeline for DUO data

    Steps:
    1. Extract metadata from .txt files
    2. Validate extracted metadata
    3. Match input files with metadata
    4. Convert to CSV format
    5. Validate conversion
    6. Compress to Parquet
    7. Encrypt sensitive data

    Args:
        input_folder (str): Path to folder containing DUO files

    Example:
        >>> import cijferho_core
        >>> cijferho_core.quick_process("data/01-input")
    """
    print("🔄 Starting complete DUO data processing...")

    print("📄 [1/7] Extracting metadata...")
    process_txt_folder(input_folder)
    process_json_folder()

    print("🛡️ [2/7] Validating metadata...")
    validate_metadata_folder()

    print("🔗 [3/7] Matching files...")
    match_files(input_folder)

    print("⚡ [4/7] Converting files...")
    run_conversions_from_matches(input_folder)

    print("✅ [5/7] Validating conversion...")
    converter_validation()

    print("🗜️ [6/7] Compressing to Parquet...")
    convert_csv_to_parquet()

    print("🔒 [7/7] Encrypting sensitive data...")
    encryptor()

    print("✅ Complete processing finished!")

# ============================================================================
# PUBLIC API - All available functions
# ============================================================================
__all__ = [
    # Extraction
    'extract_tables_from_txt',
    'process_txt_folder',
    'extract_excel_from_json',
    'process_json_folder',

    # Conversion
    'process_chunk',
    'converter',
    'run_conversions_from_matches',

    # Validation
    'validate_metadata',
    'validate_metadata_folder',
    'converter_validation',
    'load_input_files',
    'load_validation_log',
    'match_files',

    # Processing
    'convert_csv_to_parquet',
    'encryptor',

    # Case handling
    'to_snake_case',
    'normalize_column_names',
    'denormalize_column_names',
    'get_column_case_style',
    'create_column_mapping_report',

    # Combining
    'combine_all_data',
    'load_yaml_config',
    'load_main_data',
    'load_decoder_data',

    # Enrichment
    'enrich_dataframe',
    'enrich_all_data',
    'enrich_switch_data',
    'enrich_all_switch_data',

    # Convenience
    'quick_process'
]

# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
eencijferho - Standalone PyPI package for 1CijferHO backend processing.

Public API:
    from eencijferho import run_turbo_convert_pipeline
    from eencijferho import process_txt_folder, write_variable_metadata
"""

__version__ = "0.1.0"

from eencijferho.core.pipeline import run_turbo_convert_pipeline
from eencijferho.core.extractor import process_txt_folder, write_variable_metadata, process_json_folder
from eencijferho.utils.converter_validation import converter_validation
from eencijferho.utils.compressor import convert_csv_to_parquet
from eencijferho.utils.encryptor import encryptor
from eencijferho.utils.converter_headers import convert_csv_headers_to_snake_case
from eencijferho.utils.extractor_validation import validate_metadata_folder
from eencijferho.utils.converter_match import match_files

__all__ = [
    "__version__",
    "run_turbo_convert_pipeline",
    "process_txt_folder",
    "write_variable_metadata",
    "process_json_folder",
    "converter_validation",
    "convert_csv_to_parquet",
    "encryptor",
    "convert_csv_headers_to_snake_case",
    "validate_metadata_folder",
    "match_files",
]

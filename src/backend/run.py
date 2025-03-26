# Import Folder
import os
import sys
from pathlib import Path

from core import extractor as ex
from validation import extractor_validation as ex_val
from core import converter_match as cm

# Define default paths as constants
INPUT_FOLDER = "data/01-input"

# Step 1: Extract Metadata from Bestandsbeschrijving files (txt -> JSON -> Excel)
ex.process_txt_folder(INPUT_FOLDER)
ex.process_json_folder()
ex_val.validate_metadata_folder()

# Step 2: Match Metadata to Input Files & Convert (Delimit) to CSV
cm.match_metadata_inputs()

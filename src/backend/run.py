# Import Folder
import os
import sys
from pathlib import Path

from utils import extractor as ex
from validation import extractor_validation as ex_val
from utils import converter_match as cm

# Define default paths as constants
INPUT_FOLDER = "data/01-input"

# Step 1: Extract Metadata from Bestandsbeschrijving files (txt -> JSON -> Excel)
ex.process_txt_folder(INPUT_FOLDER)
ex.process_json_folder()
ex_val.validate_metadata_folder()

# Step 2: Match Metadata to Input Files & Convert (Delimit) to CSV
cm.match_metadata_inputs()


# Step 3: Decode Main Files with Dec Files
# Import File Overview

# Add the frontend directory to the Python path
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
sys.path.append(frontend_path)

# Import dashboard from frontend modules
from Modules.dashboard import render_dashboard


def main():
    """Main entry point for the application"""
    # Initialize and run the dashboard
    render_dashboard()


if __name__ == "__main__":
    main()


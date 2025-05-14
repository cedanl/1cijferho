from core import extractor as ex
from utils import extractor_validation as ex_val
from utils import match_validation as cm

# Step 0: Set Input Folder
input_folder = "data/01-input"

# Step 1: Extract Metadata from Bestandsbeschrijving files (txt -> JSON -> Excel)
ex.process_txt_folder(input_folder)
ex.process_json_folder()
ex_val.validate_metadata_folder()

# Step 2: Match Input Files to Metadata (Validation Logs)
cm.match_files(input_folder)

# Step 3:
# uv run src/backend/convert.py
# uv run src/backend/compress.py
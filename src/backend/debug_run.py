from core import extractor as ex
from validation import extractor_validation as ex_val
from core import converter_match as cm

# Step 1: Extract Metadata from Bestandsbeschrijving files (txt -> JSON -> Excel)
ex.process_txt_folder()
ex.process_json_folder()
ex_val.validate_metadata_folder()

# Step 2: Match Metadata to Input Files & Convert (Delimit) to CSV
cm.match_metadata_inputs()


# Step 3:
# uv run src/backend/convert.py
# uv run src/backend/compress.py
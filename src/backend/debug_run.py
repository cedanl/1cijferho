from core import extractor as ex
from utils import extractor_validation as ex_val
from utils import converter_match as cm
from utils import converter_validation as cv
from utils import compressor as co
from utils import encryptor as en
from utils import converter_headers as ch
import subprocess


# Step 0: Set Input Folder
input_folder = "data/01-input/"

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

# Step 8: Run Decoder
# Nog niet ontwikkeld

# NOTE TO DEVS: Refactor en overweeg om een overstap te maken naar dash.plotly.com als de UI verbeterd moet worden. 
# Streamlit is een geweldige tool, maar is niet geoptimaliseerd voor multi-page apps.

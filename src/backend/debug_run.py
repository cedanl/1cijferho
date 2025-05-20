from core import extractor as ex
from utils import extractor_validation as ex_val
from utils import converter_match as cm
from utils import converter_validation as cv
from utils import check_dec_csv as cd
from utils import compressor as co
from utils import encryptor as en
import subprocess

# Step 0: Set Input Folder
input_folder = "data/01-input"

# Step 1: Extract Metadata from Bestandsbeschrijving files (txt -> JSON -> Excel)
ex.process_txt_folder(input_folder)
ex.process_json_folder()
ex_val.validate_metadata_folder() # Check for DECodeer .csv -> convert to .xlsx then it can run!

# Step 2: Match Input Files to Metadata (Validation Logs)
cm.match_files(input_folder)

# Step 2.1: Some users already have Dec*.csv files in data/01-input. Move them to data/02-output
cd.move_dec_csv_files()

# Step 3: Convert Files
subprocess.run(["uv", "run", "src/backend/core/converter.py"])

# Step 4: Validate Conversion
cv.converter_validation()

# Step 5: Run Compressor
co.convert_csv_to_parquet()

# Step 6: Run Encryptor
en.encryptor()

# Step 7: Run Decoder


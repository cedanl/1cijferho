# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Extractor module for 1cijferho data. Contains functions for extracting tables from text files and converting them
into a JSON then XLSX format.

Functions:
    [x] extract_tables_from_txt(txt_file_path, json_folder)
        - Extracts tables from a .txt file and saves them as JSON.
    [M] process_txt_folder(txt_folder, json_output_folder) -> Main function
        - Finds all .txt files containing 'Bestandsbeschrijving' and extracts tables from them.
    [x] extract_excel_from_json(json_file_path, excel_output_folder)
        - Extracts tables from a JSON file and saves them as Excel files.
    [M] process_excel_folder(excel_folder, json_output_folder) -> Main function
        - Processes all JSON files in the metadata/json folder, converting tables to Excel files.
"""
import os
import json
import re
import polars as pl
import datetime
from rich.console import Console

# Constants for table extraction
MAX_LOOKAHEAD_LINES_FOR_DECODING_SECTION = 20  # Lines to search for "Ten behoeve van de decodering" section
MAX_LOOKAHEAD_LINES_FOR_VARIABLE_LIST = 50     # Lines to search for bullet-pointed variable list (longer because list can be extended)

def extract_decoding_variables(lines, start_index):
    """
    Extract decoding variables from lines starting at start_index.
    
    Looks for "Ten behoeve van de decodering" section and extracts bullet-pointed variables.
    
    Args:
        lines: List of text lines to search
        start_index: Index to start searching from
        
    Returns:
        List of decoding variable names
    """
    decoding_variables = []
    j = start_index
    
    # Look for "Ten behoeve van de decodering" or "Ten behoeve van de vertaling" section
    while j < len(lines) and j < start_index + MAX_LOOKAHEAD_LINES_FOR_DECODING_SECTION:
        current_line = lines[j].strip().lower()

        # Check if we've found the decoding/vertaling section
        if ("ten behoeve van de decodering" in current_line) or ("ten behoeve van de vertaling" in current_line):
            # Now collect all the bullet points (lines starting with *)
            j += 1
            while j < len(lines) and j < start_index + MAX_LOOKAHEAD_LINES_FOR_VARIABLE_LIST:
                var_line = lines[j].strip()
                if var_line.startswith('*'):
                    # Extract the variable name after the asterisk
                    var_name = var_line[1:].strip()
                    if var_name:
                        decoding_variables.append(var_name)
                    j += 1
                elif not var_line:
                    # Empty line, continue to check for more
                    j += 1
                elif var_line.startswith('NB:') or var_line.startswith('Opmerking:') or var_line.startswith('Mogelijke'):
                    # Stop when we hit notes or remarks
                    break
                else:
                    # Stop if we hit non-empty line that's not a bullet point or empty line
                    break
            break

        # Stop if we hit another table or section divider
        if current_line.startswith('==') or "startpositie" in current_line:
            break

        j += 1
    
    return decoding_variables

def extract_tables_from_txt(txt_file, json_output_folder):
    """Extracts tables from a .txt file and saves them as JSON."""
    os.makedirs(json_output_folder, exist_ok=True)
    
    try:
        with open(txt_file, 'r', encoding='latin-1') as file:
            text = file.read()
    except Exception as e:
        print(f"Error reading {txt_file}: {e}")
        return None
    
    # Process the text to find tables
    lines = text.split('\n')
    found = False
    table_title = ""
    table_content = []
    tables_found = 0
    all_tables = []
    
    for i, line in enumerate(lines):
        # Check for table header
        if "startpositie" in line.lower() and not found:
            found = True
            tables_found += 1
            table_content = [line]  # Start collecting table content
            
            # Look backwards to find the title
            table_title = f"untitled_table_{tables_found}"  # Default title
            search_range = 10
            for j in range(i-1, max(0, i-search_range), -1):
                if lines[j].strip().startswith('=='):
                    # Title is the line above the === line
                    if j > 0 and lines[j-1].strip():
                        table_title = lines[j-1].strip()
                        break
        
        # Collect table content
        elif found:
            if not line.strip():
                found = False
                
                # Extract decoding variables using helper function
                decoding_variables = extract_decoding_variables(lines, i + 1)
                
                all_tables.append({
                    "table_number": tables_found,
                    "table_title": table_title,
                    "content": table_content,
                    "decoding_variables": decoding_variables
                })
                table_content = []
                continue
            
            table_content.append(line)
    
    # Check if the last table extends to the end of the file
    if found and table_content:
        # Extract decoding variables for the last table using helper function
        start_idx = len(lines) - len(table_content)
        decoding_variables = extract_decoding_variables(lines, start_idx + len(table_content))
        
        all_tables.append({
            "table_number": tables_found,
            "table_title": table_title,
            "content": table_content,
            "decoding_variables": decoding_variables
        })
    
    # Save all tables to a single JSON file
    if all_tables:
        base_filename = os.path.splitext(os.path.basename(txt_file))[0]
        json_path = os.path.join(json_output_folder, f"{base_filename}.json")
        
        with open(json_path, 'w', encoding='latin-1') as json_file:
            json.dump({"filename": base_filename, "tables": all_tables}, json_file, indent=2, ensure_ascii=False)
        
        return json_path
    
    return None
    


def process_txt_folder(input_folder, json_output_folder="data/00-metadata/json"):
    """Finds all .txt files containing 'Bestandsbeschrijving' in the root directory only and extracts tables from them."""
    os.makedirs(json_output_folder, exist_ok=True)
    
    # Remove any existing json files
    for file in os.listdir(json_output_folder):
        if file.endswith(".json"):
            os.remove(os.path.join(json_output_folder, file))

    # Setup logging
    log_folder = "data/00-metadata/logs"
    os.makedirs(log_folder, exist_ok=True)
    
    # Create both timestamped and latest logs
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(log_folder, f"json_processing_log_{timestamp}.json")
    latest_log_file = os.path.join(log_folder, "(1)_json_processing_log_latest.json")
    
    log_data = {
        "timestamp": timestamp,
        "input_folder": input_folder,
        "output_folder": json_output_folder,
        "status": "started",
        "processed_files": [],
        "total_files_processed": 0,
        "total_files_extracted": 0
    }
    
    filter_keyword = "Bestandsbeschrijving"
    extracted_files = []

    # Only process files in the root directory, not subdirectories
    if os.path.exists(input_folder):
        for file in os.listdir(input_folder):
            file_path = os.path.join(input_folder, file)
            # Process Bestandsbeschrijving .txt files
            if os.path.isfile(file_path) and file.endswith(".txt") and filter_keyword in file:
                file_log = {
                    "file": file,
                    "status": "processing",
                    "output": None
                }
                json_path = extract_tables_from_txt(file_path, json_output_folder)
                file_log["status"] = "success" if json_path else "no_tables_found"
                if json_path:
                    extracted_files.append(json_path)
                    file_log["output"] = os.path.basename(json_path)
                log_data["processed_files"].append(file_log)
            # Also process all .asc files (DEC files)
            elif os.path.isfile(file_path) and file.endswith(".asc"):
                file_log = {
                    "file": file,
                    "status": "processing",
                    "output": None
                }
                json_path = extract_tables_from_txt(file_path, json_output_folder)
                file_log["status"] = "success" if json_path else "no_tables_found"
                if json_path:
                    extracted_files.append(json_path)
                    file_log["output"] = os.path.basename(json_path)
                log_data["processed_files"].append(file_log)
    
    # Update final log status
    log_data["status"] = "completed"
    log_data["total_files_processed"] = len(log_data["processed_files"])
    log_data["total_files_extracted"] = len(extracted_files)
    
    # Save log file to both locations
    with open(timestamped_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    with open(latest_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    
    # Print summary to console
    console = Console()
    console.print(f"[green]Processed {log_data['total_files_processed']} text files")
    console.print(f"[green]Extracted tables to {log_data['total_files_extracted']} JSON files")
    console.print(f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}")

    # --- PATCH: Merge Dec_vakcode table from Vakkenbestanden JSON into Dec-bestanden JSON ---
    vakken_json = os.path.join(json_output_folder, "Bestandsbeschrijving_Vakkenbestanden_DEMO.json")
    dec_json = os.path.join(json_output_folder, "Bestandsbeschrijving_Dec-bestanden_DEMO.json")
    if os.path.exists(vakken_json) and os.path.exists(dec_json):
        try:
            with open(vakken_json, "r", encoding="latin-1") as f_vak:
                vak_data = json.load(f_vak)
            with open(dec_json, "r", encoding="latin-1") as f_dec:
                dec_data = json.load(f_dec)
            # Find Dec_vakcode table in vak_data
            vakcode_tables = [t for t in vak_data.get("tables", []) if "vakcode" in t.get("table_title", "").lower()]
            if vakcode_tables:
                # Only add if not already present in dec_data
                existing_titles = [t.get("table_title", "").lower() for t in dec_data.get("tables", [])]
                max_table_number = max([t.get("table_number", 0) for t in dec_data.get("tables", [])] or [0])
                for t in vakcode_tables:
                    # Set correct table_title and unique table_number
                    t["table_title"] = "Dec_vakcode.asc"
                    max_table_number += 1
                    t["table_number"] = max_table_number
                    # If decoding_variables is empty, set to ["Vakcode"]
                    if not t.get("decoding_variables"):
                        t["decoding_variables"] = ["Vakcode"]
                    if t["table_title"].lower() not in existing_titles:
                        dec_data["tables"].append(t)
                        existing_titles.append(t["table_title"].lower())
                # Save back
                with open(dec_json, "w", encoding="latin-1") as f_dec:
                    json.dump(dec_data, f_dec, indent=2, ensure_ascii=False)
                console.print(f"[cyan]Patched: Added Dec_vakcode table(s) from Vakkenbestanden JSON to Dec-bestanden JSON with correct title and table_number.")
        except Exception as e:
            console.print(f"[red]Error patching Dec_vakcode into Dec-bestanden JSON: {e}")

    # After processing and optional patching, write consolidated variable metadata
    try:
        write_variable_metadata(json_output_folder)
        console.print(f"[blue]Wrote variable metadata to {os.path.join(json_output_folder, 'variable_metadata.json')}")
    except Exception:
        console.print(f"[yellow]Could not write variable metadata file.")

    return None


def write_variable_metadata(json_folder="data/00-metadata/json", output_filename="variable_metadata.json"):
    """Scan JSON metadata files and write a consolidated variable metadata JSON.

    The function collects variable names from all table `content` lines in the
    JSON files found in `json_folder`. It records where each variable was found
    and any decoding_variables listed for the table.
    """
    os.makedirs(json_folder, exist_ok=True)
    output_path = os.path.join(json_folder, output_filename)

    # Prefer using the dedicated parser in dev/parse_metadata_to_json.py so output
    # matches the original script format. If that parser isn't available, fall
    # back to a best-effort scan of existing JSON metadata files.
    # Use the original parser logic from dev/parse_metadata_to_json.py to
    # produce the canonical variables-with-values structure.
    try:
        import importlib.util
        script_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../..", "dev", "parse_metadata_to_json.py"))
        if not os.path.exists(script_path):
            console = Console()
            console.print(f"[red]Dev parser not found at {script_path}; cannot write variable metadata using dev logic.")
            return
        spec = importlib.util.spec_from_file_location("dev_parse_metadata", script_path)
        devmod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(devmod)
        if not hasattr(devmod, "parse_metadata_file"):
            console = Console()
            console.print(f"[red]Dev parser does not expose parse_metadata_file(); cannot write variable metadata using dev logic.")
            return

        default_input = os.path.join("data", "01-input", "Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt")
        if not os.path.exists(default_input):
            console = Console()
            console.print(f"[red]Default input file not found: {default_input}")
            return

        parsed = devmod.parse_metadata_file(default_input)
        with open(output_path, 'w', encoding='utf-8') as out_f:
            json.dump(parsed, out_f, ensure_ascii=False, indent=2)
        return
    except Exception as e:
        console = Console()
        console.print(f"[red]Error running dev parser: {e}")
        return


def extract_excel_from_json(json_file, excel_output_folder):
    """
    Extracts tables from a JSON file and saves them as Excel files.
    Includes ID column and a column for comments (Opmerkingen) after Aantal posities.
    Also extracts and stores decoding variables information in a separate sheet.
    Returns detailed processing results for table reporting.
    Sets specific data types for Excel columns: ID (int), Naam (str), Startpositie (int), 
    Aantal posities (int), Opmerking (str).
    """
    # Initialize Rich console for better output
    console = Console()
    
    # Create output directory if it doesn't exist
    os.makedirs(excel_output_folder, exist_ok=True)
    
    # Initialize results tracking
    results = []
    
    # Load the JSON file with appropriate encoding
    try:
        with open(json_file, 'r', encoding='latin1') as file:
            data = json.load(file)
    except json.JSONDecodeError as e:
        # Handle JSON parsing errors
        console.print(f"[red]Error decoding JSON: {e}")
        return [], 0, 0
    except Exception as e:
        # Handle other exceptions
        console.print(f"[red]Error opening file: {e}")
        return [], 0, 0
    
    # Get the base filename without extension
    base_filename = os.path.basename(json_file)
    base_filename = os.path.splitext(base_filename)[0]
    
    # Extract the filename from the JSON if available
    if "filename" in data:
        base_filename = data["filename"]
    
    # Function to sanitize filenames
    def sanitize_filename(filename):
        """Sanitize the filename by removing or replacing invalid characters."""
        return re.sub(r'[\\/*?:"<>|]', "_", filename)
    
    # Get the list of tables from the JSON
    tables = data.get("tables", [])
    total_tables = len(tables)
    
    if total_tables == 0:
        console.print("[yellow]Warning: No tables found in the JSON file.")
        return [], 0, 0
    
    # Process tables
    files_created = 0
    
    try:
        for i, table in enumerate(tables):
            table_number = table.get("table_number", i+1)
            table_title = table.get("table_title", f"Table_{table_number}")
            content_array = table.get("content", [])
            
            # Initialize result for this table
            table_result = {
                "table_number": table_number,
                "table_title": table_title,
                "status": "Processed",
                "rows": 0,
                "output_file": "",
                "notes": ""
            }
            
            # Skip empty tables
            if not content_array:
                table_result["status"] = "Skipped"
                table_result["notes"] = "Empty content"
                results.append(table_result)
                continue
            
            # Sanitize the table title for use in filename
            safe_table_title = sanitize_filename(table_title)
            
            # Create output filename
            output_filename = f"{base_filename}_{table_number}_{safe_table_title}.xlsx"
            output_path = os.path.join(excel_output_folder, output_filename)
            table_result["output_file"] = output_filename
            
            # Get the header line
            header = content_array[0]
            
            # Check if header contains the expected keywords
            if "Startpositie" not in header or "Aantal posities" not in header:
                table_result["status"] = "Skipped"
                table_result["notes"] = "Missing required headers"
                results.append(table_result)
                continue
            
            # Find the positions of the key headers
            start_pos_index = header.find("Startpositie")
            aantal_pos_index = header.find("Aantal posities")
            
            if start_pos_index == -1 or aantal_pos_index == -1:
                table_result["status"] = "Skipped"
                table_result["notes"] = "Could not locate positions for header columns"
                results.append(table_result)
                continue
            
            # Check if header contains "Opmerking"
            has_opmerking = "Opmerking" in header
            
            # Create rows for Excel
            rows = []
            
            # Add header row with ID column and the fourth column
            if has_opmerking:
                rows.append(["ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"])
            else:
                rows.append(["ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"])
            
            # Count the number of valid content lines for later verification
            valid_content_lines = 0
            
            # Process each data line
            row_id = 1  # Start ID counter
            for line in content_array[1:]:
                # Skip empty lines
                if not line.strip():
                    continue

                # Skip if the line is shorter than our reference indices
                if len(line) <= start_pos_index:
                    continue

                # Handle lines that might contain both header keywords
                if "Startpositie" in line and "Aantal posities" in line:
                    modified_line = line.replace("Startpositie", "|Startpositie")
                    modified_line = modified_line.replace("Aantal posities", "|Aantal posities|")
                    parts = modified_line.split("|")
                    if len(parts) >= 3:
                        field_name = parts[0].strip()
                        start_pos = parts[1].replace("Startpositie", "").strip()
                        aantal_pos = parts[2].replace("Aantal posities", "").strip()

                        # Extract comment if any (content after Aantal posities)
                        comment = ""
                        if len(parts) > 3:
                            comment = parts[3].strip()

                        # Only add row if both start_pos and aantal_pos are valid digits
                        if field_name and start_pos.isdigit() and aantal_pos.isdigit():
                            try:
                                rows.append([row_id, field_name, int(start_pos), int(aantal_pos), comment])
                                row_id += 1
                                valid_content_lines += 1
                            except Exception as e:
                                console.print(f"[red]Row creation error: {e} | field_name={field_name}, start_pos={start_pos}, aantal_pos={aantal_pos}, comment={comment}")
                        else:
                            # Debug log for invalid row
                            if not (field_name and start_pos.isdigit() and aantal_pos.isdigit()):
                                console.print(f"[yellow]Skipping row: field_name={field_name}, start_pos={start_pos}, aantal_pos={aantal_pos}, comment={comment}")
                    continue

                # Extract field name - use a more precise approach that preserves all characters
                field_name = ""
                pos_start = None

                # Find where the actual digits of start position begin
                for j in range(start_pos_index, len(line)):
                    if line[j].isdigit():
                        pos_start = j
                        break

                # If we found the start of the position digits, extract the field name before it
                if pos_start is not None:
                    field_name = line[:pos_start].rstrip()
                else:
                    field_name = line[:start_pos_index].rstrip()

                # Extract start position
                start_pos = None
                i = start_pos_index
                start_digits = ""
                while i < len(line) and not line[i].isdigit():
                    i += 1
                while i < len(line) and line[i].isdigit():
                    start_digits += line[i]
                    i += 1
                if start_digits:
                    start_pos = int(start_digits)

                # Extract aantal posities
                aantal_pos = None
                comment = ""
                if len(line) > aantal_pos_index:
                    i = aantal_pos_index
                    aantal_digits = ""
                    while i < len(line) and not line[i].isdigit():
                        i += 1
                    while i < len(line) and line[i].isdigit():
                        aantal_digits += line[i]
                        i += 1
                    if aantal_digits:
                        aantal_pos = int(aantal_digits)
                    # Improved comment extraction to preserve all characters
                    if i < len(line):
                        while i < len(line) and line[i].isspace():
                            i += 1
                        if i < len(line):
                            comment = line[i:].strip()

                # Only add row if both start_pos and aantal_pos are not None
                if field_name and start_pos is not None and aantal_pos is not None:
                    try:
                        rows.append([row_id, field_name, start_pos, aantal_pos, comment])
                        row_id += 1
                        valid_content_lines += 1
                    except Exception as e:
                        console.print(f"[red]Row creation error: {e} | field_name={field_name}, start_pos={start_pos}, aantal_pos={aantal_pos}, comment={comment}")
                else:
                    # Debug log for invalid row
                    if not (field_name and start_pos is not None and aantal_pos is not None):
                        console.print(f"[yellow]Skipping row: field_name={field_name}, start_pos={start_pos}, aantal_pos={aantal_pos}, comment={comment}")
            
            # Skip if no data rows were found
            if len(rows) <= 1:
                table_result["status"] = "Skipped"
                table_result["notes"] = "No data rows found"
                results.append(table_result)
                continue
            
            # Record the number of data rows
            table_result["rows"] = len(rows) - 1  # Subtract header row
            
            # Add decoding variables information if available
            decoding_variables = table.get("decoding_variables", [])
            if decoding_variables:
                # Store info about decoding variables in the table result
                table_result["decoding_variables"] = decoding_variables
                table_result["notes"] += f" Includes {len(decoding_variables)} decoding variable(s)."

            # Write to Excel file (main sheet: only valid column rows)
            try:
                import pandas as pd
                # Only keep rows with valid ID (int) for the main table
                main_rows = [row for row in rows if isinstance(row[0], int)]
                df_main = pd.DataFrame(main_rows, columns=rows[0])

                # Prepare decoding variables DataFrame if present
                if decoding_variables:
                    df_decoding = pd.DataFrame({"DecodingVariables": decoding_variables})
                    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                        df_main.to_excel(writer, index=False, sheet_name="Table")
                        df_decoding.to_excel(writer, index=False, sheet_name="DecodingVariables")
                else:
                    df_main.to_excel(output_path, index=False)

                # Check if the number of rows in the DataFrame matches the expected count
                df_row_count = df_main.shape[0]
                if df_row_count != valid_content_lines:
                    console.print(f"[yellow]Warning: Row count mismatch for table {table_title}.")
                    console.print(f"[yellow]Expected {valid_content_lines} rows, got {df_row_count} rows in DataFrame.")
                    table_result["notes"] += f" Row count mismatch: {valid_content_lines} valid content lines vs {df_row_count} DataFrame rows."
                files_created += 1
                results.append(table_result)
            except PermissionError:
                table_result["status"] = "Error"
                table_result["notes"] = "File may be open in another program (e.g. Excel)"
                results.append(table_result)
            except Exception as e:
                table_result["status"] = "Error"
                table_result["notes"] = f"Error: {str(e)}"
                results.append(table_result)
            
    except Exception as e:
        console.print(f"[red]Error during processing: {str(e)}")
        return results, files_created, total_tables
    
    return results, files_created, total_tables


def process_json_folder(json_input_folder="data/00-metadata/json", excel_output_folder="data/00-metadata"):
    """Processes all JSON files in a folder, converting tables to Excel files."""
    os.makedirs(excel_output_folder, exist_ok=True)
    
    # Remove any existing Excel files
    for file in os.listdir(excel_output_folder):
        if file.endswith(".xlsx"):
            os.remove(os.path.join(excel_output_folder, file))

    # Setup logging
    log_folder = "data/00-metadata/logs"
    os.makedirs(log_folder, exist_ok=True)
    
    # Create both a timestamped log and a latest log
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(log_folder, f"xlsx_processing_log_{timestamp}.json")
    latest_log_file = os.path.join(log_folder, "(2)_xlsx_processing_log_latest.json")
    
    log_data = {
        "timestamp": timestamp,
        "input_folder": json_input_folder,
        "output_folder": excel_output_folder,
        "status": "started",
        "processed_files": [],
        "total_files_processed": 0,
        "total_files_extracted": 0,
        "row_count_mismatches": 0  # Track files with row count mismatches
    }
    
    # Find all JSON files in the folder, but ignore the generated variable metadata
    json_files = [os.path.join(root, file)
                  for root, _, files in os.walk(json_input_folder)
                  for file in files if file.endswith(".json") and file != "variable_metadata.json"]
    
    total_json_files = len(json_files)
    if total_json_files == 0:
        log_data["status"] = "completed"
        log_data["message"] = "No JSON files found"
        # Save to both log files
        with open(timestamped_log_file, "w", encoding="latin1") as f:
            json.dump(log_data, f, indent=2)
        with open(latest_log_file, "w", encoding="latin1") as f:
            json.dump(log_data, f, indent=2)
        return None
    
    # Process each JSON file
    total_excel_files = 0
    processed_json_files = 0
    total_row_mismatches = 0
    
    for json_file in json_files:
        file_name = os.path.basename(json_file)
        
        # Log file processing
        file_log = {
            "file": file_name,
            "status": "processing",
            "tables": []
        }
        
        # Extract tables from JSON file - now also gets detailed results
        table_results, files_created, tables_found = extract_excel_from_json(json_file, excel_output_folder)
        
        # Check for row count mismatches in any tables
        file_has_mismatch = False
        for table_result in table_results:
            if "Row count mismatch" in table_result.get("notes", ""):
                file_has_mismatch = True
                total_row_mismatches += 1
                
            # Add table results to file log
            file_log["tables"].append(table_result)
        
        # Update file status in log
        file_log["status"] = "success" if files_created > 0 else "no_tables_extracted"
        file_log["tables_found"] = tables_found
        file_log["files_created"] = files_created
        file_log["has_row_mismatch"] = file_has_mismatch
        
        log_data["processed_files"].append(file_log)
        
        # Update counters
        total_excel_files += files_created
        if files_created > 0:
            processed_json_files += 1
    
    # Update final log status
    log_data["status"] = "completed"
    log_data["total_files_processed"] = total_json_files
    log_data["total_files_extracted"] = processed_json_files
    log_data["row_count_mismatches"] = total_row_mismatches
    
    # Save log file to both locations
    with open(timestamped_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    with open(latest_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    
    # Print summary to console
    console = Console()
    console.print(f"[green]Processed {total_json_files} JSON files")
    console.print(f"[green]Created {total_excel_files} Excel files from {processed_json_files} JSON files")
    
    if total_row_mismatches > 0:
        console.print(f"[yellow]Warning: {total_row_mismatches} tables had row count mismatches. Check logs for details.")
    else:
        console.print(f"[green]All tables passed row count verification")
        
    console.print(f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}")

    return None
# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Extractor module for 1cijferho data. Contains functions for extracting tables from text files and converting them
into a JSON then XLSX format.

Uses the storage abstraction layer to support disk, MinIO, and PostgreSQL backends.
Set STORAGE_BACKEND environment variable to switch backends.

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
import io
import polars as pl
import datetime
from rich.console import Console

from backend.io import storage_context, get_config

def extract_tables_from_txt(txt_file: str, json_output_folder: str | None = None):
    """
    Extracts tables from a .txt file and saves them as JSON.

    Uses storage abstraction layer for file operations.
    """
    config = get_config()
    json_output_folder = json_output_folder or f"{config.paths.metadata_dir}/json"

    with storage_context() as storage:
        storage.makedirs(json_output_folder)

        try:
            # Read text file as bytes and decode
            text_bytes = storage.read_bytes(txt_file)
            text = text_bytes.decode('latin-1')
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
                    all_tables.append({
                        "table_number": tables_found,
                        "table_title": table_title,
                        "content": table_content
                    })
                    table_content = []
                    continue

                table_content.append(line)

        # Check if the last table extends to the end of the file
        if found and table_content:
            all_tables.append({
                "table_number": tables_found,
                "table_title": table_title,
                "content": table_content
            })

        # Save all tables to a single JSON file
        if all_tables:
            base_filename = os.path.splitext(os.path.basename(txt_file))[0]
            json_path = f"{json_output_folder}/{base_filename}.json"

            # Write JSON using storage abstraction
            storage.write_json({"filename": base_filename, "tables": all_tables}, json_path)

            return json_path

        return None


def process_txt_folder(input_folder: str | None = None, json_output_folder: str | None = None):
    """
    Finds all .txt files containing 'Bestandsbeschrijving' in the root directory only
    and extracts tables from them.

    Uses storage abstraction layer for file operations.
    """
    config = get_config()
    input_folder = input_folder or config.paths.input_dir
    json_output_folder = json_output_folder or f"{config.paths.metadata_dir}/json"
    log_dir = f"{config.paths.metadata_dir}/logs"

    with storage_context() as storage:
        storage.makedirs(json_output_folder)
        storage.makedirs(log_dir)

        # Remove any existing json files
        existing_files = storage.list_files(json_output_folder, "*.json")
        for file in existing_files:
            storage.delete(file)

        # Create both timestamped and latest logs
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_log_file = f"{log_dir}/json_processing_log_{timestamp}.json"
        latest_log_file = f"{log_dir}/(1)_json_processing_log_latest.json"

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
        txt_files = storage.list_files(input_folder, "*.txt")
        for file_path in txt_files:
            file_name = os.path.basename(file_path)
            # Check if it meets our criteria
            if filter_keyword in file_name:
                # Log file processing
                file_log = {
                    "file": file_name,
                    "status": "processing",
                    "output": None
                }

                json_path = extract_tables_from_txt(file_path, json_output_folder)

                # Update file status in log
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
        storage.write_json(log_data, timestamped_log_file)
        storage.write_json(log_data, latest_log_file)

        # Print summary to console
        console = Console()
        console.print(f"[green]Processed {log_data['total_files_processed']} text files")
        console.print(f"[green]Extracted tables to {log_data['total_files_extracted']} JSON files")
        console.print(f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_dir}")

    return None

def extract_excel_from_json(json_file: str, excel_output_folder: str | None = None):
    """
    Extracts tables from a JSON file and saves them as Excel files.
    Includes ID column and a column for comments (Opmerkingen) after Aantal posities.
    Returns detailed processing results for table reporting.
    Sets specific data types for Excel columns: ID (int), Naam (str), Startpositie (int),
    Aantal posities (int), Opmerking (str).

    Uses storage abstraction layer for file operations.
    """
    # Initialize Rich console for better output
    console = Console()

    config = get_config()
    excel_output_folder = excel_output_folder or config.paths.metadata_dir

    with storage_context() as storage:
        # Create output directory if it doesn't exist
        storage.makedirs(excel_output_folder)

        # Initialize results tracking
        results = []

        # Load the JSON file using storage abstraction
        try:
            data = storage.read_json(json_file)
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
                output_path = f"{excel_output_folder}/{output_filename}"
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

                            if field_name and start_pos.isdigit() and aantal_pos.isdigit():
                                # Convert numeric fields to integers
                                rows.append([int(row_id), field_name, int(start_pos), int(aantal_pos), comment])
                                row_id += 1  # Increment ID
                                valid_content_lines += 1
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
                        # Fallback to the original approach if we can't find digits
                        field_name = line[:start_pos_index].rstrip()

                    # Extract start position
                    start_pos = ""
                    idx = start_pos_index
                    # Skip to the first digit
                    while idx < len(line) and not line[idx].isdigit():
                        idx += 1
                    # Collect all consecutive digits
                    while idx < len(line) and line[idx].isdigit():
                        start_pos += line[idx]
                        idx += 1

                    # Extract aantal posities
                    aantal_pos = ""
                    comment = ""
                    if len(line) > aantal_pos_index:
                        idx = aantal_pos_index
                        # Skip to the first digit
                        while idx < len(line) and not line[idx].isdigit():
                            idx += 1
                        # Collect all consecutive digits
                        while idx < len(line) and line[idx].isdigit():
                            aantal_pos += line[idx]
                            idx += 1

                        # Improved comment extraction to preserve all characters
                        if idx < len(line):
                            # Skip any whitespace after the digits
                            while idx < len(line) and line[idx].isspace():
                                idx += 1
                            # The rest is the comment, preserve all characters including parentheses
                            if idx < len(line):
                                comment = line[idx:].strip()

                    # Add to rows if both numbers were found
                    if field_name and start_pos and aantal_pos:
                        # Convert numeric fields to integers
                        rows.append([int(row_id), field_name, int(start_pos), int(aantal_pos), comment])
                        row_id += 1  # Increment ID
                        valid_content_lines += 1

                # Skip if no data rows were found
                if len(rows) <= 1:
                    table_result["status"] = "Skipped"
                    table_result["notes"] = "No data rows found"
                    results.append(table_result)
                    continue

                # Record the number of data rows
                table_result["rows"] = len(rows) - 1  # Subtract header row

                # Write to Excel file using storage abstraction
                try:
                    df = pl.DataFrame(rows[1:], schema=rows[0], orient="row")

                    # Check if the number of rows in the DataFrame matches the expected count
                    df_row_count = df.shape[0]
                    if df_row_count != valid_content_lines:
                        console.print(f"[yellow]Warning: Row count mismatch for table {table_title}.")
                        console.print(f"[yellow]Expected {valid_content_lines} rows, got {df_row_count} rows in DataFrame.")
                        table_result["notes"] += f" Row count mismatch: {valid_content_lines} valid content lines vs {df_row_count} DataFrame rows."

                    # Write Excel to bytes buffer, then to storage
                    excel_buffer = io.BytesIO()
                    df.write_excel(excel_buffer, autofit=True)
                    excel_buffer.seek(0)
                    storage.write_bytes(excel_buffer.read(), output_path)

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


def process_json_folder(json_input_folder: str | None = None, excel_output_folder: str | None = None):
    """
    Processes all JSON files in a folder, converting tables to Excel files.

    Uses storage abstraction layer for file operations.
    """
    config = get_config()
    json_input_folder = json_input_folder or f"{config.paths.metadata_dir}/json"
    excel_output_folder = excel_output_folder or config.paths.metadata_dir
    log_dir = f"{config.paths.metadata_dir}/logs"

    with storage_context() as storage:
        storage.makedirs(excel_output_folder)
        storage.makedirs(log_dir)

        # Remove any existing Excel files
        existing_files = storage.list_files(excel_output_folder, "*.xlsx")
        for file in existing_files:
            storage.delete(file)

        # Create both a timestamped log and a latest log
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_log_file = f"{log_dir}/xlsx_processing_log_{timestamp}.json"
        latest_log_file = f"{log_dir}/(2)_xlsx_processing_log_latest.json"

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

        # Find all JSON files in the folder
        json_files = storage.list_files(json_input_folder, "*.json")

        total_json_files = len(json_files)
        if total_json_files == 0:
            log_data["status"] = "completed"
            log_data["message"] = "No JSON files found"
            # Save to both log files
            storage.write_json(log_data, timestamped_log_file)
            storage.write_json(log_data, latest_log_file)
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
        storage.write_json(log_data, timestamped_log_file)
        storage.write_json(log_data, latest_log_file)

        # Print summary to console
        console = Console()
        console.print(f"[green]Processed {total_json_files} JSON files")
        console.print(f"[green]Created {total_excel_files} Excel files from {processed_json_files} JSON files")

        if total_row_mismatches > 0:
            console.print(f"[yellow]Warning: {total_row_mismatches} tables had row count mismatches. Check logs for details.")
        else:
            console.print(f"[green]All tables passed row count verification")

        console.print(f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_dir}")

    return None

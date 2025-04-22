# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Extractor module for 1cijferho data. Contains functions for extracting tables from text files and converting them
into a JSON format.

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
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn


def extract_tables_from_txt(txt_file_path, json_output_folder):
    """Extracts tables from a .txt file and saves them as JSON."""
    os.makedirs(json_output_folder, exist_ok=True)
    
    try:
        with open(txt_file_path, 'r', encoding='latin-1') as file:
            text = file.read()
    except Exception as e:
        print(f"Error reading {txt_file_path}: {e}")
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
        base_filename = os.path.splitext(os.path.basename(txt_file_path))[0]
        json_path = os.path.join(json_output_folder, f"{base_filename}.json")
        
        with open(json_path, 'w', encoding='latin-1') as json_file:
            json.dump({"filename": base_filename, "tables": all_tables}, json_file, indent=2, ensure_ascii=False)
        
        return json_path
    
    return None

def process_txt_folder(input_folder, json_output_folder="data/00-metadata/json"):
    """Finds all .txt files containing 'Bestandsbeschrijving' and extracts tables from them."""
    os.makedirs(json_output_folder, exist_ok=True)
    
    # Setup logging
    log_folder = "data/00-metadata/logs"
    os.makedirs(log_folder, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_folder, f"extraction_log_{timestamp}.json")
    
    log_data = {
        "timestamp": timestamp,
        "input_folder": input_folder,
        "output_folder": json_output_folder,
        "status": "started",
        "files_processed": [],
        "extracted_files": []
    }
    
    filter_keyword = "Bestandsbeschrijving"
    extracted_files = []
    
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith(".txt") and filter_keyword in file:
                txt_path = os.path.join(root, file)
                
                # Log file processing
                file_log = {"file": os.path.basename(txt_path), "status": "processing"}
                log_data["files_processed"].append(file_log)
                
                json_path = extract_tables_from_txt(txt_path, json_output_folder)
                
                # Update file status in log
                file_log["status"] = "success" if json_path else "no_tables_found"
                if json_path:
                    extracted_files.append(json_path)
                    file_log["output"] = os.path.basename(json_path)
                    log_data["extracted_files"].append(os.path.basename(json_path))
    
    # Update final log status
    log_data["status"] = "completed"
    log_data["total_processed"] = len(log_data["files_processed"])
    log_data["total_extracted"] = len(extracted_files)
    
    # Save log file
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)
    
    return extracted_files

def extract_excel_from_json(json_file_path, excel_output_folder):
    """
    Extracts tables from a JSON file and saves them as Excel files.
    Includes ID column and a column for comments (Opmerkingen) after Aantal posities.
    Returns detailed processing results for table reporting.
    """
    # Initialize Rich console for better output
    console = Console()
    
    # Create output directory if it doesn't exist
    os.makedirs(excel_output_folder, exist_ok=True)
    
    # Initialize results tracking
    results = []
    
    # Load the JSON file with appropriate encoding
    try:
        with open(json_file_path, 'r', encoding='latin1') as file:
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
    base_filename = os.path.basename(json_file_path)
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
                            rows.append([row_id, field_name, start_pos, aantal_pos, comment])
                            row_id += 1  # Increment ID
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
                i = start_pos_index
                # Skip to the first digit
                while i < len(line) and not line[i].isdigit():
                    i += 1
                # Collect all consecutive digits
                while i < len(line) and line[i].isdigit():
                    start_pos += line[i]
                    i += 1
                
                # Extract aantal posities
                aantal_pos = ""
                comment = ""
                if len(line) > aantal_pos_index:
                    i = aantal_pos_index
                    # Skip to the first digit
                    while i < len(line) and not line[i].isdigit():
                        i += 1
                    # Collect all consecutive digits
                    while i < len(line) and line[i].isdigit():
                        aantal_pos += line[i]
                        i += 1
                    
                    # Improved comment extraction to preserve all characters
                    if i < len(line):
                        # Skip any whitespace after the digits
                        while i < len(line) and line[i].isspace():
                            i += 1
                        # The rest is the comment, preserve all characters including parentheses
                        if i < len(line):
                            comment = line[i:].strip()
                
                # Add to rows if both numbers were found
                if field_name and start_pos and aantal_pos:
                    rows.append([row_id, field_name, start_pos, aantal_pos, comment])
                    row_id += 1  # Increment ID
            
            # Skip if no data rows were found
            if len(rows) <= 1:
                table_result["status"] = "Skipped"
                table_result["notes"] = "No data rows found"
                results.append(table_result)
                continue
            
            # Record the number of data rows
            table_result["rows"] = len(rows) - 1  # Subtract header row
                
            # Write to Excel file
            try:
                df = pl.DataFrame(rows[1:], schema=rows[0], orient="row")
                df.write_excel(output_path, autofit=True)
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
    """
    Processes all JSON files in a folder, converting tables to Excel files.
    Uses a table to display results instead of spinners/progress bars.
    """
    # Initialize Rich console for better output
    console = Console()
    from rich.table import Table
    
    # Create output directory if it doesn't exist
    os.makedirs(excel_output_folder, exist_ok=True)
    
    console.print(f"[bold cyan]Processing JSON files from: {json_input_folder}")
    
    total_excel_files = 0
    total_json_files = 0
    
    # Find all JSON files in the folder
    json_files = []
    for root, _, files in os.walk(json_input_folder):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(root, file))
    
    total_json_files = len(json_files)
    if total_json_files == 0:
        console.print("[yellow]Warning: No JSON files found in the specified folder.")
        return 0, 0
    
    console.print(f"Found {total_json_files} JSON files")
    
    # Create a master results table
    master_table = Table(title="JSON Processing Results")
    master_table.add_column("JSON File", style="cyan")
    master_table.add_column("Tables", style="blue")
    master_table.add_column("Processed", style="green")
    master_table.add_column("Skipped", style="yellow")
    master_table.add_column("Errors", style="red")
    master_table.add_column("Status", style="magenta")
    
    # Process each JSON file and collect results
    all_detailed_results = []
    processed_json_files = 0
    
    for json_file in json_files:
        file_name = os.path.basename(json_file)
        console.print(f"\n[cyan]Processing file: {file_name}")
        
        # Process the JSON file
        table_results, files_created, tables_found = extract_excel_from_json(json_file, excel_output_folder)
        
        # Count different statuses
        processed = sum(1 for r in table_results if r["status"] == "Processed")
        skipped = sum(1 for r in table_results if r["status"] == "Skipped")
        errors = sum(1 for r in table_results if r["status"] == "Error")
        
        # Determine overall status
        if errors > 0:
            status = "Issues Encountered"
        elif skipped > 0:
            status = "Partially Processed"
        elif processed > 0:
            status = "Success"
        else:
            status = "No Tables Processed"
        
        # Add to the master table
        master_table.add_row(
            file_name, 
            str(tables_found), 
            str(processed), 
            str(skipped),
            str(errors),
            status
        )
        
        # Store detailed results for this file
        file_detail = {
            "file_name": file_name,
            "tables_found": tables_found,
            "processed": processed,
            "skipped": skipped,
            "errors": errors,
            "table_results": table_results
        }
        all_detailed_results.append(file_detail)
        
        # Create a detailed table for this file
        if table_results:
            detail_table = Table(title=f"Detailed Results for {file_name}", show_header=True)
            detail_table.add_column("Table #", style="cyan", justify="center")
            detail_table.add_column("Title", style="blue")
            detail_table.add_column("Status", style="green")
            detail_table.add_column("Rows", style="magenta", justify="right")
            detail_table.add_column("Output File", style="cyan")
            detail_table.add_column("Notes", style="yellow")
            
            for result in table_results:
                status_style = "green" if result["status"] == "Processed" else "red"
                
                detail_table.add_row(
                    str(result["table_number"]),
                    result["table_title"],
                    f"[{status_style}]{result['status']}[/{status_style}]",
                    str(result["rows"]),
                    result["output_file"],
                    result["notes"]
                )
            
            console.print(detail_table)
        
        # Update counters
        total_excel_files += files_created
        if files_created > 0:
            processed_json_files += 1
    
    # Print the master summary table
    console.print("\n")
    console.print(master_table)
    
    # Print summary
    console.print("\n[bold]Summary:[/bold]")
    console.print(f"Processed {total_json_files} JSON files: [green]{processed_json_files} with tables extracted[/green], [yellow]{total_json_files - processed_json_files} with no tables extracted[/yellow]")
    console.print(f"[green]Created {total_excel_files} Excel files from tables.")
    
    return total_excel_files, processed_json_files
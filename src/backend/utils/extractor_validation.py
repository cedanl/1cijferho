# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Script that runs tests on the Bestandsbeschrijving files (.xlsx) in the data folder.

Functions:
    [x] validate_metadata(file_path, verbose=True)
        - Validates a single Excel file and returns validation results
    [M] validate_metadata_folder(metadata_folder="data/00-metadata", return_dict=False) -> Main function
        - Validates all Excel files in a metadata_folder and returns a summary
"""
import polars as pl
from rich.console import Console
import os
import glob
import json
import datetime
from pathlib import Path


# Check column lenght in extractor with json  

# Move .csv to 02-output

def validate_metadata(file_path):
    """Validates a single layout specification file and returns validation results."""
    
    issues_dict = {
        "duplicates": [],
        "position_errors": [],
        "length_mismatch": False,
        "total_issues": 0
    }
    
    # Load file
    try:
        # Read the first four columns
        # Load Excel file into pandas
        df = pl.read_excel(file_path, columns=[0, 1, 2, 3])
    
    except Exception as e:
        issues_dict["load_error"] = str(e)
        issues_dict["total_issues"] += 1
        return False, issues_dict

    # Always rename the columns
    df = df.rename({
        df.columns[2]: "Start_Positie",
        df.columns[3]: "Aantal_Posities"
    })

    try:
        fname = Path(file_path).name
        if any(x in fname for x in ("Dec_landcode", "Dec_nationaliteitscode")):
            # add a temporary row counter so we can target the 2nd row (index 1)
            df = df.with_row_count("__rownum")
            if df.height > 1:
                df = df.with_column(
                    pl.when(pl.col("__rownum") == 1)
                    .then(pl.col("Start_Positie") + 1)
                    .otherwise(pl.col("Start_Positie"))
                    .alias("Start_Positie")
                )
            df = df.drop("__rownum")
    except Exception:
        pass
    
    # 1. Duplicate check
    duplicate_names = df.filter(df["Naam"].is_duplicated())["Naam"].to_list()
    
    if duplicate_names:
        # Store detailed information about duplicates with row numbers
        duplicate_details = []
        for dup_name in set(duplicate_names):
            # Get row indices for each duplicate
            dup_rows = df.select(pl.col("Naam")).with_row_index().filter(pl.col("Naam") == dup_name)
            row_indices = dup_rows["index"].to_list()
            
            # Add row numbers (1-based)
            row_numbers = [idx + 1 for idx in row_indices]  # Convert to 1-based indexing
            
            duplicate_details.append({
                "name": dup_name,
                "row_numbers": row_numbers
            })
        
        issues_dict["duplicates"] = duplicate_details
        issues_dict["total_issues"] += len(duplicate_names)
    
    # 2. Position check (each field should start right after the previous one ends)
    df = df.sort("Start_Positie")
    position_errors = []
    
    for i in range(1, len(df)):
        prev_end = df["Start_Positie"][i-1] + df["Aantal_Posities"][i-1]
        curr_start = df["Start_Positie"][i]
        prev_field = df["Naam"][i-1]
        curr_field = df["Naam"][i]
        
        if prev_end != curr_start:
            # Store details about the position error
            error_detail = {
                "row": i + 1,  # 1-based row number
                "expected_start": prev_end,
                "actual_start": curr_start,
                "previous_field": prev_field,
                "current_field": curr_field,
                "gap_size": curr_start - prev_end
            }
            position_errors.append(error_detail)
    
    if position_errors:
        issues_dict["position_errors"] = position_errors
        issues_dict["total_issues"] += len(position_errors)
    
    # 3. Sum check
    sum_positions = df["Aantal_Posities"].sum()
    last_pos = df["Start_Positie"].max() + df.filter(pl.col("Start_Positie") == df["Start_Positie"].max())["Aantal_Posities"][0] - 1
    
    if last_pos != sum_positions:
        issues_dict["length_mismatch"] = True
        issues_dict["length_sum"] = sum_positions
        issues_dict["length_last"] = last_pos
        issues_dict["total_issues"] += 1
    
    # Final result
    issues_count = issues_dict["total_issues"]
    if issues_count == 0:
        return True, issues_dict
    else:
        return False, issues_dict

    
def validate_metadata_folder(metadata_folder="data/00-metadata", return_dict=False):
    """Validates all Excel files in a metadata_folder and returns a summary."""
  
    console = Console()
    
    # Find all Excel files
    excel_files = glob.glob(os.path.join(metadata_folder, "*.xlsx"))
    
    if not excel_files:
        console.print(f"[yellow]No Excel files found in {metadata_folder}[/yellow]")
        return {} if return_dict else None
    
    console.print(f"[green]Validating {len(excel_files)} Excel files")
    
    # Setup logging
    log_folder = "data/00-metadata/logs"
    os.makedirs(log_folder, exist_ok=True)
    
    # Create both timestamped and latest logs (similar to the extractor code)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(log_folder, f"xlsx_validation_log_{timestamp}.json")
    latest_log_file = os.path.join(log_folder, "(3)_xlsx_validation_log_latest.json")
    
    log_data = {
        "timestamp": timestamp,
        "metadata_folder": metadata_folder,
        "status": "started",
        "processed_files": [],
        "total_files_processed": 0,
        "passed_files": 0,
        "failed_files": 0
    }
    
    # Validate each file
    results = {}
    for file_path in excel_files:
        file_name = os.path.basename(file_path)
        
        # Log file processing
        file_log = {
            "file": file_name,
            "status": "processing",
            "issues": None
        }
        
        # Call the validation function
        success, issues = validate_metadata(file_path)
        results[file_name] = {"success": success, "issues": issues}
        
        # Update log with results
        file_log["status"] = "success" if success else "failed"
        file_log["issues"] = issues
        log_data["processed_files"].append(file_log)
    
    # Update log summary
    passed = sum(1 for res in results.values() if res["success"])
    log_data["status"] = "completed"
    log_data["total_files_processed"] = len(results)
    log_data["passed_files"] = passed
    log_data["failed_files"] = len(results) - passed
    
    # Print summary
    console.print(f"[green]Validated {len(results)} files: {passed} passed[/green], [red]{len(results) - passed} failed[/red]")

    # Show failed files with issues in console
    if len(results) - passed > 0:
        console.print("\n[yellow]Failed files with issues, manually adjust these files and re-run extractor validation:[/yellow]")
        for file_name, res in results.items():
            if not res["success"]:
                issues = res["issues"]
                console.print(f"[bold red]{file_name}[/bold red]")
                
                # Display key issues
                if issues.get("duplicates"):
                    console.print(f"  - Duplicate fields: {len(issues['duplicates'])}")
                if issues.get("position_errors"):
                    console.print(f"  - Position errors: {len(issues['position_errors'])}")
                    # Show details for each position error for easier manual fixes
                    for err in issues["position_errors"]:
                        console.print(
                            f"    • Row {err['row']}: field '{err['current_field']}' expected start={err['expected_start']} "
                            f"but found start={err['actual_start']} (prev='{err['previous_field']}', current_start={err['actual_start']})"
                        )
                if issues.get("length_mismatch"):
                    console.print(
                        f"  - Length mismatch: Sum of Aantal_Posities={issues['length_sum']} vs last field end={issues['length_last']}.")
                if issues.get("load_error"):
                    console.print(f"  - Load error: {issues['load_error']}")
                if issues.get("column_error"):
                    console.print(f"  - Column error: {issues['column_error']}")
    
    # Save log files
    with open(timestamped_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False)
    with open(latest_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False)
    
    console.print(f"\n[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}[/blue]")
    
    if return_dict:
        return results
    return None
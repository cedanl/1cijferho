# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Script that matches the input files with validation records from previously processed metadata files.

Functions:
    [x] load_input_files(input_folder)
        - Get all files from the input folder, excluding certain extensions
    [x] load_validation_log(log_path)
        - Load processed bestandbeschrijvingen validation log into a Polars dataframe
    [M] match_files(input_folder, log_path="data/00-metadata/logs/(3)_xlsx_validation_log_latest.json") -> Main function
        - Matches input files with validation records and logs the results
"""
import os
import polars as pl
from rich.console import Console
import json
import datetime

def load_input_files(input_folder):
    """Get all files from the user input_folder (root level only), excluding .txt, .zip, and .xlsx extensions,
    and count the number of rows in each file."""
    files = []
    row_counts = []
    console = Console()
    
    # Only process files in the root directory, not subdirectories
    if os.path.exists(input_folder):
        for filename in os.listdir(input_folder):
            full_path = os.path.join(input_folder, filename)
            # Check if it's a file (not a directory) and doesn't have excluded extensions
            if os.path.isfile(full_path) and not filename.lower().endswith(('.txt', '.zip', '.xlsx', '.docx', '.csv')):
                files.append(filename)
                
                # Count number of rows in the file
                try:
                    with open(full_path, 'rb') as f:
                        # Fast way to count lines using binary mode
                        row_count = sum(1 for _ in f)
                    row_counts.append(row_count)
                except Exception as e:
                    # If we can't read the file for any reason, set count to -1
                    console.print(f"[red]Warning: Could not read file {filename}: {str(e)}")
                    row_counts.append(-1)
    
    # Create dataframe with file names and row counts
    df = pl.DataFrame({
        "input_file": files,
        "row_count": row_counts
    })
    
    return df

def load_validation_log(log_path):
    """Load processed bestandbeschrijvingen validation log and return a Polars dataframe with file and status columns"""
    with open(log_path, 'r') as f:
        data = json.load(f)
    
    df = pl.DataFrame([
        {'file': item['file'], 'status': item['status']} 
        for item in data.get('processed_files', [])
    ])
    
    return df

def match_files(input_folder, log_path="data/00-metadata/logs/(3)_xlsx_validation_log_latest.json"):
    """Match input files with metadata files and log the results.
    
    Special matching rules:
    - Files starting with "EV" match with files containing "1cyferho"
    - Files containing "VAKHAVW" match with files containing "Vakgegevens"
    """
    
    # Setup logging
    log_folder = "data/00-metadata/logs"
    os.makedirs(log_folder, exist_ok=True)
    
    # Create both timestamped and latest logs
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(log_folder, f"file_matching_log_{timestamp}.json")
    latest_log_file = os.path.join(log_folder, "(4)_file_matching_log_latest.json")
    
    # Load both dataframes
    input_df = load_input_files(input_folder)  # This now includes row_count
    validation_df = load_validation_log(log_path)

    # Print initial status message
    console = Console()
    console.print("[green]Finding matches between input files and validation records")
    
    # Initialize logging data structure
    log_data = {
        "timestamp": timestamp,
        "input_folder": input_folder,
        "validation_log": log_path,
        "status": "started",
        "processed_files": [],
        "total_input_files": len(input_df),
        "matched_files": 0,
        "unmatched_files": 0,
        "total_validation_files": len(validation_df),
        "matched_validation_files": 0,
        "unmatched_validation_files": 0,
    }
    
    # Create a new column with matches
    results = []
    
    # Keep track of which validation files have been matched
    matched_validation_files = set()
    
    for idx, row in enumerate(input_df.rows()):
        input_file = row[0]  # Filename
        row_count = row[1]   # Row count
        
        matches = None
        
        # Apply special matching rules based on input filename
        if input_file.startswith("EV"):
            # For files starting with "EV", match with files containing "1cyferho"
            matches = validation_df.filter(pl.col("file").str.contains("1cyferho"))
        elif "VAKHAVW" in input_file:
            # For files containing "VAKHAVW", match with files containing "Vakgegevens"
            matches = validation_df.filter(pl.col("file").str.contains("Vakgegevens"))
        else:
            # Default matching: find where input_file is contained in the 'file' column
            matches = validation_df.filter(pl.col("file").str.contains(input_file))
        
        file_log = {
            "input_file": input_file,
            "row_count": row_count,  # Add row count to log
            "status": "unmatched",
            "matches": []
        }
        
        if len(matches) > 0:
            # Get the status for each match
            file_log["status"] = "matched"
            
            for match_row in matches.rows():
                validation_file = match_row[0]
                # Add to set of matched validation files
                matched_validation_files.add(validation_file)
                
                match_detail = {
                    "validation_file": validation_file,
                    "validation_status": match_row[1]
                }
                file_log["matches"].append(match_detail)
                
                results.append({
                    "input_file": input_file,
                    "row_count": row_count,  # Add row count to results
                    "validation_file": validation_file,
                    "status": match_row[1],
                    "matched": True
                })
        else:
            # No match found
            results.append({
                "input_file": input_file,
                "row_count": row_count,  # Add row count to results
                "validation_file": None,
                "status": None,
                "matched": False
            })
        
        log_data["processed_files"].append(file_log)
    
    # Create result dataframe for input files
    result_df = pl.DataFrame(results)
    
    # Find unmatched validation files
    unmatched_validation = []
    for validation_row in validation_df.rows():
        validation_file = validation_row[0]
        if validation_file not in matched_validation_files:
            unmatched_validation.append({
                "validation_file": validation_file,
                "validation_status": validation_row[1],
                "matched": False
            })
    
    # Create unmatched validation dataframe
    unmatched_validation_df = pl.DataFrame(unmatched_validation)
    
     
    # Update log data
    log_data["status"] = "completed"
    log_data["matched_files"] = result_df.filter(pl.col('matched')).height
    log_data["unmatched_files"] = result_df.filter(~pl.col('matched')).height
    log_data["matched_validation_files"] = len(matched_validation_files)
    log_data["unmatched_validation_files"] = len(validation_df) - len(matched_validation_files)
    log_data["unmatched_validation"] = [
        {"validation_file": row["validation_file"], "validation_status": row["validation_status"]}
        for row in unmatched_validation
    ]
    
    # Save log file to both locations
    with open(timestamped_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    with open(latest_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    
    # Print summary to console
    console.print(f"[green]Total input files: {log_data['total_input_files']}  | Matched files: {log_data['matched_files']} [/green] | [red]Unmatched files: {log_data['unmatched_files']}[/red]")
    console.print(f"[green]Total validation files: {log_data['total_validation_files']} [/green] | [yellow]Unmatched validation files: {log_data['unmatched_validation_files']}[/yellow]")
    
    # Print unmatched files with helpful header if there are any
    if log_data["unmatched_files"] > 0 or log_data["unmatched_validation_files"] > 0:
        console.print("\n[yellow]Perhaps a naming error? Manually fix in data/01-input for input files or data/00-metadata for validation files[/yellow]")
    
    # Print unmatched input files details
    if log_data["unmatched_files"] > 0:
        console.print("\n[red]Unmatched input files:[/red]")
        unmatched_input = result_df.filter(~pl.col('matched'))
        for row in unmatched_input.rows():
            input_file = row[0]
            console.print(f"[red]{input_file}[/red]")
    
    # Print unmatched validation files details
    if log_data["unmatched_validation_files"] > 0:
        console.print("\n[yellow]Unmatched validation files:[/yellow]")
        for item in log_data["unmatched_validation"]:
            validation_file = item["validation_file"]
            console.print(f"[yellow]{validation_file}[/yellow]")
    
    console.print(f"\n[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}[/blue]")

    # Return both result dataframes
    return {
        "input_matches": result_df,
        "unmatched_validation": unmatched_validation_df
    }
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
from rich.table import Table
from difflib import SequenceMatcher
import json
import datetime

def load_input_files(input_folder):
    """Get all files from the user input_folder, excluding .txt, .zip, and .xlsx extensions"""
    files = []
    
    # Walk through the directory
    for root, _, filenames in os.walk(input_folder):
        for filename in filenames:
            # Check if the file doesn't have excluded extensions
            if not filename.lower().endswith(('.txt', '.zip', '.xlsx')):
                files.append(filename)
    
    # Create dataframe with file names
    df = pl.DataFrame({"input_file": files})
    
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
    input_df = load_input_files(input_folder)
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
        "unmatched_validation_files": 0
    }
    
    # Create a new column with matches
    results = []
    
    # Keep track of which validation files have been matched
    matched_validation_files = set()
    
    for input_file in input_df["input_file"]:
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
                    "validation_file": validation_file,
                    "status": match_row[1],
                    "matched": True
                })
        else:
            # No match found
            results.append({
                "input_file": input_file,
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
    
    # Print summary to console with unmatched validation files in yellow
    console.print(f"[green]Total input files: {log_data['total_input_files']} | Matched files: {log_data['matched_files']}[/green] | [red]Unmatched files: {log_data['unmatched_files']}[/red]")
    console.print(f"[yellow]Total validation files: {log_data['total_validation_files']} | Unmatched validation files: {log_data['unmatched_validation_files']}[/yellow]")
    console.print(f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}")
    
    # Return both result dataframes
    return {
        "input_matches": result_df,
        "unmatched_validation": unmatched_validation_df
    }

# Run the matching function if executed as a script
if __name__ == "__main__":
    match_files("data/01-input")
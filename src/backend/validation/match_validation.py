# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Script that matches the Bestandsbeschrijving files (.xlsx) in the data folder with the input files.

Functions:
    [M] match_metadata_inputs(input_folder="data/01-input", metadata_folder="data/00-metadata") -> Main function
        - Matches metadata files with input files and displays validation status from extractor_validation.py
    [x] find_matches(metadata_file, input_files)
        - Special case matching for 1cyferho → EV files and Vakgegevens → VAKHAVW files
    [x] extract_key_pattern(filename)
        - Extract key patterns from a metadata filename
    [x] print_summary(console, metadata_files, input_files, ignored_files, matched_metadata, validation_results)
        - Print summary statistics about matching results
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
    
    # Print dataframe
    print(df)
    
    return df

# Load XLSX Validation Log
def load_validation_log(log_path):
    """Load processed bestandbeschrijvingen validation log and return a Polars dataframe with file and status columns"""
    with open(log_path, 'r') as f:
        data = json.load(f)
    
    df = pl.DataFrame([
        {'file': item['file'], 'status': item['status']} 
        for item in data.get('processed_files', [])
    ])
    
    print(df)
    return df


# Match & Log
def match_files(input_folder, log_path="data/00-metadata/logs/(3)_xlsx_validation_log_latest.json"):
    """Match input files with metadata files and log the results."""
    
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
        "unmatched_files": 0
    }
    
    # Create a new column with matches
    # For each input file, check if it appears within any validation file string
    results = []
    
    for input_file in input_df["input_file"]:
        # Find matches in validation_df where input_file is contained in the 'file' column
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
                match_detail = {
                    "validation_file": match_row[0],
                    "validation_status": match_row[1]
                }
                file_log["matches"].append(match_detail)
                
                results.append({
                    "input_file": input_file,
                    "validation_file": match_row[0],
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
    
    # Create result dataframe
    result_df = pl.DataFrame(results)
    
    # Update log data
    log_data["status"] = "completed"
    log_data["matched_files"] = result_df.filter(pl.col('matched')).height
    log_data["unmatched_files"] = result_df.filter(~pl.col('matched')).height
    
    # Save log file to both locations
    with open(timestamped_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    with open(latest_log_file, "w", encoding="latin1") as f:
        json.dump(log_data, f, indent=2)
    
    # Print simple summary to console
    console.print(f"[green]Total input files: {log_data['total_input_files']} | Matched files: {log_data['matched_files']} | [/green][red]Unmatched files: {log_data['unmatched_files']}[/red]")
    console.print(f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}")
    
    # Return the result dataframe
    return result_df

# Run the matching function if executed as a script
if __name__ == "__main__":
    match_files("data/01-input")
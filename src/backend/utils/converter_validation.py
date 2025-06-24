# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Script that validates if row counts in matching log match with total lines in conversion log.
"""

import json
import os
import datetime
from rich import print as rprint

def converter_validation(conversion_log_path="data/00-metadata/logs/(5)_conversion_log_latest.json", 
                         matching_log_path="data/00-metadata/logs/(4)_file_matching_log_latest.json", 
                         output_log_path="data/00-metadata/logs/(6)_conversion_validation_log_latest.json"):
    # Prepare results structure
    results = {
        "timestamp": datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
        "total_files": 0,
        "successful_conversions": 0,
        "failed_conversions": 0,
        "status": "completed",
        "file_details": []
    }
    
    # Load logs
    with open(conversion_log_path, 'r') as f:
        conversion_data = json.load(f)
    
    with open(matching_log_path, 'r') as f:
        matching_data = json.load(f)
    
    # Create lookup dictionaries
    conversion_files = {item["input_file"]: item for item in conversion_data.get("details", [])}
    
    # Get file details from matching log
    matching_files = {item["input_file"]: item for item in matching_data.get("processed_files", [])}
    
    # Compare files from both logs
    for filename, match_data in matching_files.items():
        if filename in conversion_files and conversion_files[filename]["status"] == "success":
            expected_rows = match_data.get("row_count", 0)
            actual_rows = conversion_files[filename].get("total_lines", 0)
            
            file_result = {
                "input_file": filename,
                "expected_rows": expected_rows,
                "actual_rows": actual_rows
            }
            
            if expected_rows == actual_rows:
                file_result["status"] = "success"
                results["successful_conversions"] += 1
            else:
                file_result["status"] = "failed"
                file_result["error"] = f"Row count mismatch: expected {expected_rows}, found {actual_rows}"
                results["failed_conversions"] += 1
                
            results["file_details"].append(file_result)
            results["total_files"] += 1
    
    # Save results to output log
    os.makedirs(os.path.dirname(output_log_path), exist_ok=True)
    with open(output_log_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary using rich
    if results["failed_conversions"] == 0:
        rprint("[green]Total files:", results["total_files"])
        rprint("[green]Successfully validated:", results["successful_conversions"])
    else:
        rprint("[red]Total files:", results["total_files"])
        rprint("[red]Successfully validated:", results["successful_conversions"])
        rprint("[red]Failed validations:", results["failed_conversions"])
    
    return results

if __name__ == "__main__":
    converter_validation()
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

def validate_metadata(file_path, verbose=True):
    """
    Validates a single layout specification file and returns validation results
    """
    import polars as pl
    from rich.console import Console
    import os
    
    console = Console()
    if verbose:
        console.print(f"\n[bold blue]Validating: {os.path.basename(file_path)}[/bold blue]")
    
    issues_dict = {
        "duplicates": [],
        "position_errors": [],
        "length_mismatch": False,
        "total_issues": 0
    }
    
    # Load file
    try:
        df = pl.read_excel(file_path)
        if verbose:
            console.print(f"[green]Loaded file: {len(df)} rows[/green]")
    except Exception as e:
        if verbose:
            console.print(f"[red]Error: {e}[/red]")
        issues_dict["load_error"] = str(e)
        issues_dict["total_issues"] += 1
        return False, issues_dict
    
    # Standardize column names
    if "Aantal posities" in df.columns:
        df = df.rename({
            "Startpositie": "Start_Positie",
            "Aantal posities": "Aantal_Posities"
        })
    
    # Convert to correct types
    try:
        df = df.with_columns([
            pl.col("Start_Positie").cast(pl.Int64),
            pl.col("Aantal_Posities").cast(pl.Int64)
        ])
    except Exception as e:
        if verbose:
            console.print(f"[red]Column type conversion error: {e}[/red]")
            console.print("[yellow]Columns found:[/yellow]", df.columns)
        issues_dict["column_error"] = str(e)
        issues_dict["total_issues"] += 1
        return False, issues_dict
    
    # 1. Duplicate check with detailed information
    duplicate_names = df.filter(df["Naam"].is_duplicated())["Naam"].to_list()
    
    if duplicate_names:
        if verbose:
            console.print(f"[red]Found {len(duplicate_names)} duplicate names[/red]")
        
        # Store detailed information about duplicates with row numbers
        duplicate_details = []
        for dup_name in set(duplicate_names):
            # Get row indices for each duplicate
            dup_rows = df.select(pl.col("Naam")).with_row_index().filter(pl.col("Naam") == dup_name)
            row_indices = dup_rows["index"].to_list()
            
            # Add row numbers (1-based) and display them
            row_numbers = [idx + 1 for idx in row_indices]  # Convert to 1-based indexing
            
            if verbose:
                console.print(f"  Duplicate name '[bold]{dup_name}[/bold]' found in rows: {', '.join(map(str, row_numbers))}")
            
            duplicate_details.append({
                "name": dup_name,
                "row_numbers": row_numbers
            })
        
        issues_dict["duplicates"] = duplicate_details
        issues_dict["total_issues"] += len(duplicate_names)
    elif verbose:
        console.print("[green]No duplicates[/green]")
    
    # 2. Position check (each field should start right after the previous one ends)
    df = df.sort("Start_Positie")
    position_errors = []
    
    for i in range(1, len(df)):
        prev_end = df["Start_Positie"][i-1] + df["Aantal_Posities"][i-1]
        curr_start = df["Start_Positie"][i]
        prev_field = df["Naam"][i-1]
        curr_field = df["Naam"][i]
        
        if prev_end != curr_start:
            # Store more detailed information about the error
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
        if verbose:
            console.print(f"[red]Found {len(position_errors)} position gaps/overlaps[/red]")
            for error in position_errors[:5]:  # Show max 5 errors
                gap_type = "gap" if error["gap_size"] > 0 else "overlap"
                size = abs(error["gap_size"])
                console.print(f"  Row {error['row']}: {gap_type} of {size} positions between '{error['previous_field']}' and '{error['current_field']}'")
                console.print(f"    Expected start: {error['expected_start']}, Actual: {error['actual_start']}")
        
        issues_dict["position_errors"] = position_errors
        issues_dict["total_issues"] += len(position_errors)
    elif verbose:
        console.print("[green]Field positions are consecutive[/green]")
    
    # 3. Sum check
    sum_positions = df["Aantal_Posities"].sum()
    last_pos = df["Start_Positie"].max() + df.filter(pl.col("Start_Positie") == df["Start_Positie"].max())["Aantal_Posities"][0] - 1
    
    if last_pos == sum_positions:
        if verbose:
            console.print(f"[green]Length check passed: {sum_positions}[/green]")
    else:
        if verbose:
            console.print(f"[red]Length mismatch: Sum={sum_positions}, Last={last_pos}[/red]")
        issues_dict["length_mismatch"] = True
        issues_dict["length_sum"] = sum_positions
        issues_dict["length_last"] = last_pos
        issues_dict["total_issues"] += 1
    
    # Summary
    issues_count = issues_dict["total_issues"]
    if issues_count == 0:
        if verbose:
            console.print("[green]All checks passed![/green]")
        return True, issues_dict
    else:
        if verbose:
            console.print(f"[red]Found {issues_count} issues[/red]")
        return False, issues_dict

def validate_metadata_folder(metadata_folder="data/00-metadata", return_dict=False, save_log=True):
    """
    Validates all Excel files in a metadata_folder, returns a summary, and optionally saves results to a log file
    
    Parameters:
    -----------
    metadata_folder : str
        Path to the folder containing metadata Excel files
    return_dict : bool
        Whether to return the validation results as a dictionary
    save_log : bool
        Whether to save the validation results to a log file in {metadata_folder}/logs
        
    Returns:
    --------
    dict or None
        Dictionary of validation results if return_dict=True, None otherwise
    """
    import os
    import glob
    import json
    import datetime
    from rich.console import Console
    from rich.table import Table
    
    console = Console()
    console.print(f"[bold]Validating all Excel files in {metadata_folder}[/bold]")
    
    # Find all Excel files
    excel_files = glob.glob(os.path.join(metadata_folder, "*.xlsx"))
    
    if not excel_files:
        console.print(f"[yellow]No Excel files found in {metadata_folder}[/yellow]")
        return {} if return_dict else None
    
    console.print(f"Found {len(excel_files)} Excel files")
    
    # Create a table for results
    table = Table(title="Layout Validation Results")
    table.add_column("Metadata File (.xlsx)", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Issues", style="yellow")
    
    # Validate each file
    results = {}
    for file_path in excel_files:
        file_name = os.path.basename(file_path)
        success, issues = validate_metadata(file_path, verbose=False)
        results[file_name] = {"success": success, "issues": issues}
        
        # Add to table
        status = "[green]✓ Passed[/green]" if success else f"[red]✗ Failed ({issues['total_issues']} issues)[/red]"
        
        # Create issue summary
        issue_notes = []
        if issues.get("duplicates"):
            duplicate_count = len(issues['duplicates'])
            issue_notes.append(f"{duplicate_count} duplicate fields")
        if issues.get("position_errors"):
            issue_notes.append(f"{len(issues['position_errors'])} position gaps/overlaps")
        if issues.get("length_mismatch"):
            issue_notes.append(f"Length mismatch (sum={issues['length_sum']}, last={issues['length_last']})")
        if issues.get("load_error"):
            issue_notes.append(f"File load error: {issues['load_error']}")
        if issues.get("column_error"):
            issue_notes.append(f"Column error: {issues['column_error']}")
            
        issue_text = "; ".join(issue_notes) if issue_notes else "[green]None[/green]"
        
        table.add_row(file_name, status, issue_text)
    
    # Print the table
    console.print(table)
    
    # Summary of all files
    console.print("\n[bold]Summary:[/bold]")
    passed = sum(1 for res in results.values() if res["success"])
    console.print(f"Validated {len(results)} files: [green]{passed} passed[/green], [red]{len(results) - passed} failed[/red]")
    
    if len(results) - passed > 0:
        console.print("\n[yellow]Failed files with detailed errors:[/yellow]")
        for file_name, res in results.items():
            if not res["success"]:
                console.print(f"\n[bold red]{file_name}[/bold red]")
                
                # Display detailed error information
                issues = res["issues"]
                
                # Show duplicate fields with row numbers
                if issues.get("duplicates"):
                    console.print("  [yellow]Duplicate fields:[/yellow]")
                    for dup in issues["duplicates"]:
                        console.print(f"    Field '[bold]{dup['name']}[/bold]' in rows: {', '.join(map(str, dup['row_numbers']))}")
                
                # Show position gaps/overlaps
                if issues.get("position_errors"):
                    console.print("  [yellow]Position errors:[/yellow]")
                    for i, error in enumerate(issues["position_errors"][:5]):  # Show max 5 errors
                        gap_type = "gap" if error["gap_size"] > 0 else "overlap"
                        size = abs(error["gap_size"])
                        console.print(f"    Error {i+1}: {gap_type} of {size} positions between fields in row {error['row']}")
                        console.print(f"      From '{error['previous_field']}' to '{error['current_field']}'")
                    
                    if len(issues["position_errors"]) > 5:
                        console.print(f"    ... and {len(issues['position_errors']) - 5} more position errors")
                
                # Show length mismatch
                if issues.get("length_mismatch"):
                    console.print("  [yellow]Length mismatch:[/yellow]")
                    console.print(f"    Sum of field lengths: {issues['length_sum']}")
                    console.print(f"    Last position: {issues['length_last']}")
                    console.print(f"    Difference: {abs(issues['length_sum'] - issues['length_last'])}")

    # Save results to log file if requested
    if save_log:
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(metadata_folder, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        # Get current timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create a serializable version of the results
        serializable_results = {
            "timestamp": timestamp,
            "metadata_folder": metadata_folder,
            "total_files": len(results),
            "passed_files": passed,
            "failed_files": len(results) - passed,
            "file_results": {}
        }
        
        # Add each file's results
        for file_name, res in results.items():
            # Clean up the issues to make them JSON serializable
            clean_issues = {}
            
            for key, value in res["issues"].items():
                if key in ["duplicates", "position_errors"]:
                    # These are lists of dicts that might need conversion
                    clean_issues[key] = []
                    for item in value:
                        clean_item = {}
                        for k, v in item.items():
                            # Convert any non-serializable types
                            if isinstance(v, (int, float, str, bool, type(None))):
                                clean_item[k] = v
                            else:
                                clean_item[k] = str(v)
                        clean_issues[key].append(clean_item)
                else:
                    # For simple values, just convert non-serializable types
                    if isinstance(value, (int, float, str, bool, type(None))):
                        clean_issues[key] = value
                    else:
                        clean_issues[key] = str(value)
            
            serializable_results["file_results"][file_name] = {
                "success": res["success"],
                "issues": clean_issues
            }
        
        # Save only the latest results to a fixed filename for easy access
        latest_log_file = os.path.join(logs_dir, "validation_results_latest.json")
        with open(latest_log_file, 'w') as f:
            json.dump(serializable_results, f, indent=2)
        
        console.print(f"\n[bold green]Validation results saved to:[/bold green] {latest_log_file}")
        
        # Code for timestamped files is commented out
        """
        # Create a timestamp for the log file
        log_file = os.path.join(logs_dir, f"validation_results_{timestamp}.json")
        
        # Save to JSON file
        with open(log_file, 'w') as f:
            json.dump(serializable_results, f, indent=2)
        
        console.print(f"[bold green]Validation results also saved to:[/bold green] {log_file}")
        """

    if return_dict:
        return results
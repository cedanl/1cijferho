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
    [N] export_to_csv(df, output_path)
        - Export the matching results to a CSV file
"""

import validation.extractor_validation as ex_val
import os
import re
import polars as pl
from rich.console import Console
from rich.table import Table
from difflib import SequenceMatcher

def match_metadata_inputs(input_folder="data/01-input", metadata_folder="data/00-metadata"):
    """
    Matches metadata files with input files and displays validation status.
    Returns a polars DataFrame with the matching and validation results.
    """
    console = Console()
    
    # Validate directories
    if not os.path.exists(metadata_folder):
        console.print(f"[bold red]Error:[/bold red] Metadata directory '{metadata_folder}' does not exist!")
        return pl.DataFrame()
        
    if not os.path.exists(input_folder):
        console.print(f"[bold red]Error:[/bold red] Input directory '{input_folder}' does not exist!")
        return pl.DataFrame()
    
    # Get validation results
    validation_results = ex_val.validate_metadata_folder(metadata_folder, return_dict=True)
    
    # Create table
    table = Table(title="Metadata to Input Files Matching with Validation Status")
    table.add_column("Metadata File (.xlsx)", style="cyan")
    table.add_column("Input File", style="green")
    table.add_column("Match Type", style="yellow")
    table.add_column("Validation Status", style="magenta")
    
    # Get files
    try:
        metadata_files = sorted([f for f in os.listdir(metadata_folder) if f.endswith('.xlsx')])
        
        all_input_files = os.listdir(input_folder)
        ignored_extensions = ['.txt', '.zip', '.xlsx']
        input_files = [f for f in all_input_files 
                       if not any(f.lower().endswith(ext) for ext in ignored_extensions)]
        
        ignored_files = [f for f in all_input_files 
                        if any(f.lower().endswith(ext) for ext in ignored_extensions)]
        
        console.print(f"Found {len(input_files)} relevant input files in '{input_folder}'")
        console.print(f"Ignored {len(ignored_files)} files with extensions .txt, .zip, .xlsx")
    except Exception as e:
        console.print(f"[bold red]Error reading directories:[/bold red] {str(e)}")
        return pl.DataFrame()
    
    # Results structures
    results = []
    matched_metadata = {}
    
    # Match each metadata file with input files
    for metadata_file in metadata_files:
        # Get validation status directly from the dictionary
        validation_info = validation_results.get(metadata_file, {})
        is_valid = validation_info.get("success", False)
        status_style = "green" if is_valid else "red"
        
        if is_valid:
            status_text = "✓ Passed"
        else:
            issue_count = validation_info.get("issues", {}).get("total_issues", 0)
            status_text = f"✗ Failed ({issue_count} issues)"
        
        # Find all matches for this metadata file
        matches = find_matches(metadata_file, input_files)
        
        if matches:
            # Add each match to results
            for input_file, match_type in matches:
                results.append({
                    "metadata_file": metadata_file,
                    "input_file": input_file,
                    "match_type": match_type,
                    "validation_status": status_text,
                    "is_valid": is_valid
                })
                
                # Add to table
                table.add_row(metadata_file, input_file, match_type, 
                              f"[{status_style}]{status_text}[/{status_style}]")
                
                # Track matched files
                if metadata_file not in matched_metadata:
                    matched_metadata[metadata_file] = []
                matched_metadata[metadata_file].append(input_file)
        else:
            # No match found
            table.add_row(metadata_file, "No matching file", "✗ No match", 
                         f"[{status_style}]{status_text}[/{status_style}]")
            
            # Add to results for CSV export
            results.append({
                "metadata_file": metadata_file,
                "input_file": "No matching file",
                "match_type": "✗ No match",
                "validation_status": status_text,
                "is_valid": is_valid
            })
    
    # Print table and summary
    console.print(table)
    print_summary(console, metadata_files, input_files, ignored_files, 
                  matched_metadata, validation_results)
    
    # Create DataFrame
    df = pl.DataFrame() if not results else pl.DataFrame(results)
    
    # Export to CSV
    if not df.is_empty():
        logs_dir = os.path.join(metadata_folder, "logs")
        export_to_csv(df, os.path.join(logs_dir, "match.csv"))
    
    return df

def find_matches(metadata_file, input_files):
    """Find all matching input files for a given metadata file."""
    matches = []
    
    # Special case matching
    if "1cyferho" in metadata_file and "_Lay-out" in metadata_file:
        matches.extend([(f, "✓ Special match") for f in input_files if f.startswith("EV")])
    
    elif "Vakgegevens" in metadata_file:
        matches.extend([(f, "✓ Special match") for f in input_files if "VAKHAVW" in f])
    
    # If special matching found results, return them
    if matches:
        return matches
    
    # Extract patterns from metadata filename
    patterns = extract_key_pattern(metadata_file)
    
    # Try exact matching
    for input_file in input_files:
        input_base = os.path.splitext(input_file)[0].lower()
        
        # Try exact matches
        for pattern in patterns:
            if input_base == pattern.lower():
                matches.append((input_file, "✓ Exact match"))
                break
    # If exact matches found, return them
    if matches:
        return matches
    
    # Try partial matching
    for input_file in input_files:
        input_base = os.path.splitext(input_file)[0].lower()
        
        for pattern in patterns:
            if pattern.lower() in input_base:
                matches.append((input_file, "✓ Partial match"))
                break
    
    # If partial matches found, return them
    if matches:
        return matches
    
    # Try fuzzy matching as last resort
    best_match = None
    best_score = 0
    
    for input_file in input_files:
        input_base = os.path.splitext(input_file)[0].lower()
        main_pattern = patterns[0] if patterns else metadata_file.split('_')[0]
        
        similarity = SequenceMatcher(None, main_pattern.lower(), input_base.lower()).ratio()
        
        if similarity > 0.65 and similarity > best_score:
            best_score = similarity
            best_match = (input_file, f"🔍 Fuzzy match ({best_score:.2f})")
    
    if best_match:
        matches.append(best_match)
    
    return matches

def extract_key_pattern(filename):
    """Extract key patterns from a metadata filename."""
    # Remove common prefixes
    cleaned = filename.replace("Bestandsbeschrijving_", "")
    patterns = []
    
    # Extract exact ASC filename references
    asc_matches = re.findall(r'(Dec_[a-zA-Z0-9_-]+)\.asc', cleaned, re.IGNORECASE)
    if asc_matches:
        patterns.extend(asc_matches)
    
    # Extract exact Croho patterns
    croho_matches = re.findall(r'(Croho(?:_vest)?)\.asc', cleaned, re.IGNORECASE)
    if croho_matches:
        patterns.extend(croho_matches)
    
    # Look for explicit filename references
    filename_ref_matches = re.findall(r'Bestandsbeschrijving\s+([A-Za-z0-9_.-]+\.asc)', cleaned, re.IGNORECASE)
    if filename_ref_matches:
        patterns.insert(0, filename_ref_matches[0])
    
    # Fallback to the first part of the filename
    if not patterns and '_' in cleaned:
        parts = cleaned.split('_')
        if len(parts) > 1:
            patterns.append(parts[0])
            
            # For Dec-bestanden files, also add the specific Dec_ pattern
            if parts[0] == "Dec-bestanden" and len(parts) > 2:
                patterns.append("Dec_" + parts[2].lower())
    
    return patterns

def print_summary(console, metadata_files, input_files, ignored_files, 
                 matched_metadata, validation_results):
    """Print summary statistics about matching results."""
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"Total metadata files: {len(metadata_files)}")
    console.print(f"Total relevant input files: {len(input_files)} (excluding {len(ignored_files)} ignored files)")
    console.print(f"Matched metadata files: {len(matched_metadata)}")
    console.print(f"Unmatched metadata files: {len(metadata_files) - len(matched_metadata)}")
    
    # List unmatched files
    unmatched = [f for f in metadata_files if f not in matched_metadata]
    if unmatched:
        console.print("\n[bold red]Unmatched Metadata Files:[/bold red]")
        for f in sorted(unmatched):
            console.print(f"  - {f}")
    
    # Find files with multiple matches
    multi_matches = {k: v for k, v in matched_metadata.items() if len(v) > 1}
    if multi_matches:
        console.print("\n[bold yellow]Metadata Files with Multiple Matches:[/bold yellow]")
        for meta_file, matches in multi_matches.items():
            console.print(f"  - {meta_file} → {', '.join(matches)}")

def export_to_csv(df, output_path):
    """Export the matching results to a CSV file."""
    try:
        # Ensure the directory exists
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Clean up status text for CSV (remove Rich formatting)
        if "validation_status" in df.columns:
            df = df.with_columns(pl.col("validation_status").str.replace(r"\(\d+ issues\)", "Failed", literal=False))
        
        # Export to CSV
        df.write_csv(output_path)
        
        print(f"\nMatching results exported to: {output_path}")
        return True
    except Exception as e:
        print(f"\n[Error] Failed to export results to CSV: {str(e)}")
        return False
            
# Run the matching function if executed as a script
if __name__ == "__main__":
    match_metadata_inputs()
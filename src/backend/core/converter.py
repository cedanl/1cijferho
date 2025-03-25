# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Fixed-width to CSV converter for 1CHO data files. Contains functionality for efficient conversion 
of fixed-width format files to CSV format using multiprocessing.

Functions:
    [x] process_chunk(chunk_data) - Processes a chunk of lines in a fixed-width file
        - Process a chunk of lines and return the converted output
    [M] converter(input_file, metadata_file) - Converts a fixed-width file to CSV using a metadata specification -> Main function
        - Convert fixed-width file to CSV using multiprocessing for better performance
    [N] run_conversions_from_matches(matches_csv, input_folder) - Run the converter for each valid match
        - Processes all valid matches in the CSV file, applying the converter function
"""

import multiprocessing as mp
import os
import time
from functools import partial
import polars as pl
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# TODO: Add Test (Line Length, Add to table returned by converter_match.py)

################################################################
#                       HELPER FUNCTIONS                          
################################################################

def format_file_size(size_bytes):
    """Format file size from bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def format_elapsed_time(seconds):
    """Format elapsed time in seconds to a readable format"""
    if seconds < 60:
        return f"{seconds:.2f} sec"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} min"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hr"

################################################################
#                       COMPUTER MAGIC                          
################################################################

def process_chunk(chunk_data):
    """
    Process a chunk of lines and return the converted output
    """
    positions, chunk = chunk_data
    output_lines = []
    for line in chunk:
        if isinstance(line, bytes):
            line = line.decode('latin1')  # Adjust encoding as needed
        if line.strip():  # Skip empty lines
            fields = [line[start:end].strip() for start, end in positions]
            output_lines.append('|'.join(fields))
    return output_lines


def converter(input_file, metadata_file):
    """
    Convert fixed-width file to CSV using multiprocessing for better performance
    Input file is encoded in Latin-1
    Output file will have the same name and be saved in data/02-output
    """
    # Determine output file path - same name but in data/02-output
    input_filename = os.path.basename(input_file)
    base_name = os.path.splitext(input_filename)[0]  # Get filename without extension
    output_file = os.path.join('data', '02-output', f"{base_name}.csv")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Load metadata from Excel file
    metadata_df = pl.read_excel(metadata_file)
    
    # Convert widths to integers explicitly
    widths = [int(w) for w in metadata_df["Aantal posities"].to_list()]
    column_names = metadata_df["Naam"].to_list()
    
    # Calculate positions for each field
    positions = [(sum(widths[:i]), sum(widths[:i+1])) for i in range(len(widths))]
    
    # Count total lines
    with open(input_file, 'rb') as f:
        total_lines = sum(1 for _ in f.readlines())
    
    console = Console()
    
    # Write header first
    with open(output_file, 'w', encoding='latin1', newline='') as f_out:
        f_out.write('|'.join(column_names) + '\n')
    
    # Read the entire file into memory (if it's not too large)
    with open(input_file, 'r', encoding='latin1') as f_in:
        all_lines = f_in.readlines()
    
    # Guard to prevent recursive multiprocessing
    # This will only allow multiprocessing in the main process
    is_main_process = mp.current_process().name == 'MainProcess'
    
    if is_main_process:
        # Set up multiprocessing safely
        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]Converting..."),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("", total=total_lines)
            
            try:
                # Determine chunk size and number of processes
                num_processes = max(1, mp.cpu_count() - 1)  # Leave one core free
                chunk_size = max(1, len(all_lines) // (num_processes * 4))  # Create 4x as many chunks as processes
                
                # Split data into chunks
                chunks = [all_lines[i:i + chunk_size] for i in range(0, len(all_lines), chunk_size)]
                chunk_data = [(positions, chunk) for chunk in chunks]
                
                # Process in parallel with proper cleanup
                with mp.Pool(processes=num_processes) as pool:
                    results_iter = pool.imap_unordered(process_chunk, chunk_data)
                    
                    # Write results as they come in
                    lines_processed = 0
                    with open(output_file, 'a', encoding='latin1', newline='') as f_out:
                        for result in results_iter:
                            if result:
                                f_out.write('\n'.join(result) + '\n')
                            lines_processed += len(result) if result else 0
                            progress.update(task, completed=min(lines_processed, total_lines))
            
            except Exception as e:
                console.print(f"[red]Error during conversion: {str(e)}")
                raise
    else:
        # Process the data serially if we're in a child process
        console.print("[yellow]Running in serial mode (child process detected)")
        
        results = process_chunk((positions, all_lines))
        with open(output_file, 'a', encoding='latin1', newline='') as f_out:
            if results:
                f_out.write('\n'.join(results) + '\n')
    
    console.print(f"[green]Conversion completed successfully! Output saved to {output_file} ✨")
    return output_file

# -----------------------------------------------------------------------------
# Script to run converter for each valid matched file pair
# -----------------------------------------------------------------------------

def run_conversions_from_matches(matches_csv="data/00-metadata/logs/match.csv", input_folder="data/01-input", metadata_folder="data/00-metadata"):
    """
    Run the converter function for each valid matched file pair in the CSV.
    Only processes rows where:
    1. is_valid is True
    2. There is an actual input file (not "No matching file")
    
    Args:
        matches_csv: Path to the match.csv file containing matching results
        input_folder: Path to the folder containing input files
        metadata_folder: Path to the folder containing metadata files
    """
    console = Console()
    
    # Check if matches CSV exists
    if not os.path.exists(matches_csv):
        console.print(f"[bold red]Error:[/bold red] Matches CSV file '{matches_csv}' not found!")
        return
    
    # Check if input folder exists
    if not os.path.exists(input_folder):
        console.print(f"[bold red]Error:[/bold red] Input folder '{input_folder}' does not exist!")
        return
    
    # Check if metadata folder exists
    if not os.path.exists(metadata_folder):
        console.print(f"[bold red]Error:[/bold red] Metadata folder '{metadata_folder}' does not exist!")
        return
    
    try:
        # Read the matches CSV
        df = pl.read_csv(matches_csv)
        
        # Ensure required columns exist
        required_cols = ["metadata_file", "input_file", "is_valid"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            console.print(f"[bold red]Error:[/bold red] Missing required columns in CSV: {', '.join(missing_cols)}")
            return
        
        # Filter valid rows and exclude "No matching file" entries
        valid_matches = df.filter(
            (pl.col("is_valid") == True) & 
            (pl.col("input_file") != "No matching file")
        )
        
        if valid_matches.is_empty():
            console.print("[yellow]No valid file matches found to process.[/yellow]")
            return
        
        # Print summary
        total_matches = len(valid_matches)
        console.print(f"[bold]Found {total_matches} valid matches to process.[/bold]")
        
        # Create a table to display progress
        table = Table(title="Conversion Progress")
        table.add_column("Input File", style="cyan")
        table.add_column("Metadata File", style="green")
        table.add_column("Input Size", style="blue")
        table.add_column("Output Size", style="blue")
        table.add_column("Elapsed Time", style="yellow")
        table.add_column("Status", style="magenta")
        
        # Process each valid match
        success_count = 0
        error_count = 0
        total_elapsed_time = 0  # Track total elapsed time
        
        for row in valid_matches.iter_rows(named=True):
            metadata_path = os.path.join(metadata_folder, row["metadata_file"])
            input_path = os.path.join(input_folder, row["input_file"])
            
            console.print(f"\n[bold]Processing:[/bold] {row['input_file']} with {row['metadata_file']}")
            
            try:
                # Check if both files exist
                if not os.path.exists(input_path):
                    raise FileNotFoundError(f"Input file not found: {input_path}")
                if not os.path.exists(metadata_path):
                    raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
                
                # Get input file size before conversion
                input_size_bytes = os.path.getsize(input_path)
                input_file_size = format_file_size(input_size_bytes)
                
                # Determine output file path
                input_filename = os.path.basename(input_path)
                output_file = os.path.join('data', '02-output', input_filename)
                
                # Track time elapsed during conversion
                start_time = time.time()
                
                # Run converter on this file pair
                converter(input_path, metadata_path)
                
                # Calculate elapsed time
                elapsed_time = time.time() - start_time
                total_elapsed_time += elapsed_time  # Add to total
                formatted_time = format_elapsed_time(elapsed_time)
                
                # Get output file size after conversion
                if os.path.exists(output_file):
                    output_size_bytes = os.path.getsize(output_file)
                    output_file_size = format_file_size(output_size_bytes)
                else:
                    output_file_size = "File not found"
                
                success_count += 1
                status = "[green]✓ Successfully converted[/green]"
            except Exception as e:
                error_count += 1
                status = f"[red]✗ Error: {str(e)}[/red]"
                input_file_size = "N/A"
                output_file_size = "N/A"
                formatted_time = "N/A"
            
            table.add_row(
                row["input_file"], 
                row["metadata_file"], 
                input_file_size,
                output_file_size,
                formatted_time, 
                status
            )
        
        # Print final summary
        console.print(table)
        
        # Print total elapsed time summary
        total_formatted_time = format_elapsed_time(total_elapsed_time)
        console.print(f"\n[bold]Conversion complete:[/bold] {success_count} successful, {error_count} failed")
        console.print(f"[bold]Total processing time:[/bold] {total_formatted_time}")
        
    except Exception as e:
        console.print(f"[bold red]Error processing matches CSV:[/bold red] {str(e)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run converter for matched metadata and input files")
    parser.add_argument("--matches", default="data/00-metadata/logs/match.csv", help="Path to the match.csv file")
    parser.add_argument("--input", default="data/01-input", help="Path to the input folder")
    parser.add_argument("--metadata", default="data/00-metadata", help="Path to the metadata folder")
    
    args = parser.parse_args()
    
    run_conversions_from_matches(
        matches_csv=args.matches,
        input_folder=args.input,
        metadata_folder=args.metadata
    )
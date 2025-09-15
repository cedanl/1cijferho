"""
Fixed-width to CSV converter for 1CHO data files. Contains functionality for efficient conversion
of fixed-width format files to CSV format using multiprocessing.

Functions:
    [x] process_chunk(chunk_data) - Processes a chunk of lines in a fixed-width file
        - Process a chunk of lines and return the converted output
    [M] converter(input_file, metadata_file) - Converts a fixed-width file to CSV using a metadata specification
        - Convert fixed-width file to CSV using multiprocessing for better performance
    [N] run_conversions_from_matches(input_folder, metadata_folder, match_log_fileatch_log_file) - Run the converter for each valid match in the JSON log
        - Processes all valid matches in the JSON file, applying the converter function
"""

import multiprocessing as mp
import os
import json
import polars as pl
import datetime
import argparse
import re
import unicodedata
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# TODO: Add Test (Line Length, Add to table returned by converter_match.py)

################################################################
#                       UTILITY FUNCTIONS
################################################################

def normalize_text(text):
    """
    Normalize text by converting accents and special characters to ASCII equivalents
    Examples: ó → o, ë → e, ñ → n, etc.
    """
    # Use NFD normalization to decompose characters
    normalized = unicodedata.normalize('NFD', text)
    # Filter out combining characters (accents, diacritics)
    ascii_text = ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')
    return ascii_text

def to_snake_case(name):
    """
    Convert a string to snake_case
    """
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    # Convert camelCase to snake_case
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    # Replace multiple underscores with single underscore
    name = re.sub('_+', '_', name)
    # Remove leading/trailing underscores and convert to lowercase
    return name.strip('_').lower()

def to_camel_case(name):
    """
    Convert a string to camelCase
    """
    # First convert any existing camelCase/PascalCase to words
    # Split on spaces, underscores, and capital letters
    words = re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\b)|\d+', name)
    # Also split on spaces and underscores if no camelCase detected
    if not words or len(words) == 1:
        words = re.split(r'[\s_]+', name.strip())

    # Filter out empty strings and clean words
    words = [word.strip() for word in words if word.strip()]
    if not words:
        return name

    # First word lowercase, rest title case
    result = words[0].lower()
    for word in words[1:]:
        result += word.capitalize()
    return result

def to_pascal_case(name):
    """
    Convert a string to PascalCase
    """
    # First convert any existing camelCase/PascalCase to words
    # Split on spaces, underscores, and capital letters
    words = re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\b)|\d+', name)
    # Also split on spaces and underscores if no camelCase detected
    if not words or len(words) == 1:
        words = re.split(r'[\s_]+', name.strip())

    # Filter out empty strings and clean words
    words = [word.strip() for word in words if word.strip()]
    if not words:
        return name

    # All words title case
    result = ''.join(word.capitalize() for word in words)
    return result

def convert_case(name, case_style):
    """
    Convert a string to the specified case style
    Always normalizes accents/special characters to ASCII first
    """
    # Always normalize accents and special characters first
    normalized_name = normalize_text(name)

    if case_style == 'snake_case':
        return to_snake_case(normalized_name)
    elif case_style == 'camelCase':
        return to_camel_case(normalized_name)
    elif case_style == 'PascalCase':
        return to_pascal_case(normalized_name)
    else:  # 'original' - keep original case but normalized
        return normalized_name

################################################################
#                       COMPUTER MAGIC
################################################################

def process_chunk(chunk_data):
    """
    Process a chunk of lines and return the converted output
    """
    positions, chunk, separator = chunk_data
    output_lines = []
    for line in chunk:
        if isinstance(line, bytes):
            line = line.decode('latin1')  # Adjust encoding as needed
        if line.strip():  # Skip empty lines
            fields = [line[start:end].strip() for start, end in positions]
            output_lines.append(separator.join(fields))
    return output_lines


def converter(input_file, metadata_file, case_style='snake_case', separator=','):

    # Determine output file path - same name but in data/02-processed
    input_filename = os.path.basename(input_file)
    base_name = os.path.splitext(input_filename)[0]  # Get filename without extension
    output_file = f"data/02-processed/{base_name}.csv"


    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Load metadata from Excel file
    metadata_df = pl.read_excel(metadata_file)

    # Convert widths to integers explicitly
    widths = [int(w) for w in metadata_df["Aantal posities"].to_list()]
    column_names = metadata_df["Naam"].to_list()

    # Convert column names to specified case style
    column_names = [convert_case(name, case_style) for name in column_names]

    # Calculate positions for each field
    positions = [(sum(widths[:i]), sum(widths[:i+1])) for i in range(len(widths))]

    # Count total lines
    with open(input_file, 'rb') as f:
        total_lines = sum(1 for _ in f.readlines())

    # Write header first
    with open(output_file, 'w', encoding='latin1', newline='') as f_out:
        f_out.write(separator.join(column_names) + '\n')

    # Read the entire file into memory (if it's not too large)
    with open(input_file, 'r', encoding='latin1') as f_in:
        all_lines = f_in.readlines()

    # Guard to prevent recursive multiprocessing
    # This will only allow multiprocessing in the main process
    is_main_process = mp.current_process().name == 'MainProcess'

    if is_main_process:
        # Determine chunk size and number of processes
        num_processes = max(1, mp.cpu_count() - 1)  # Leave one core free
        chunk_size = max(1, len(all_lines) // (num_processes * 4))  # Create 4x as many chunks as processes

        # Split data into chunks
        chunks = [all_lines[i:i + chunk_size] for i in range(0, len(all_lines), chunk_size)]
        chunk_data = [(positions, chunk, separator) for chunk in chunks]

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
    else:
        # Process the data serially if we're in a child process
        results = process_chunk((positions, all_lines, separator))
        with open(output_file, 'a', encoding='latin1', newline='') as f_out:
            if results:
                f_out.write('\n'.join(results) + '\n')

    return output_file, total_lines


def run_conversions_from_matches(input_folder, metadata_folder="data/00-metadata", match_log_file = "data/00-metadata/logs/(4)_file_matching_log_latest.json", case_style='snake_case', separator=','):

    console = Console()
    console.print(f"[cyan]Starting conversion based on match log: {match_log_file}")
    console.print(f"[cyan]Settings: case_style={case_style}, separator='{separator}'")

    # Setup logging
    log_folder = "data/00-metadata/logs"
    os.makedirs(log_folder, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(log_folder, f"conversion_log_{timestamp}.json")
    latest_log_file = os.path.join(log_folder, "(5)_conversion_log_latest.json")

    # Check if log file exists
    if not os.path.exists(match_log_file):
        console.print(f"[red]Match log file not found: {match_log_file}")
        return {"status": "failed", "reason": "Log file not found"}

    # Load the JSON log file
    try:
        with open(match_log_file, 'r') as f:
            log_data = json.load(f)
    except Exception as e:
        console.print(f"[red]Error loading JSON log file: {str(e)}")
        return {"status": "failed", "reason": f"Error loading JSON: {str(e)}"}

    results = {
        "timestamp": timestamp,
        "match_log_file": match_log_file,
        "settings": {
            "case_style": case_style,
            "separator": separator
        },
        "total_files": 0,
        "successful_conversions": 0,
        "failed_conversions": 0,
        "skipped_files": 0,
        "details": [],
        "skipped_file_pairs": []  # Add this to track skipped file pairs
    }

    # Iterate through processed files in the log
    with Progress(
        SpinnerColumn(),
        TextColumn("[cyan]Processing files..."),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        # Count files with successful validation
        valid_files = [f for f in log_data["processed_files"]
                      if f["status"] == "matched" and
                      any(m["validation_status"] == "success" for m in f["matches"])]

        total_files = len(valid_files)
        results["total_files"] = total_files

        task = progress.add_task("", total=total_files)

        for file_info in log_data["processed_files"]:
            input_file_name = file_info["input_file"]
            file_result = {
                "input_file": input_file_name,
                "status": "skipped",
                "reason": ""
            }

            # Check if file has matches with successful validation
            if file_info["status"] == "matched":
                valid_matches = [m for m in file_info["matches"] if m["validation_status"] == "success"]

                if valid_matches:
                    # Take the first successful match for processing
                    validation_file_name = valid_matches[0]["validation_file"]

                    # Construct full paths
                    input_file_path = os.path.join(input_folder, input_file_name)
                    validation_file_path = os.path.join(metadata_folder, validation_file_name)

                    # Check if files exist
                    if not os.path.exists(input_file_path):
                        console.print(f"[red]Input file not found: {input_file_path}")
                        file_result["status"] = "failed"
                        file_result["reason"] = "Input file not found"
                        results["failed_conversions"] += 1
                        continue

                    if not os.path.exists(validation_file_path):
                        console.print(f"[red]Validation file not found: {validation_file_path}")
                        file_result["status"] = "failed"
                        file_result["reason"] = "Validation file not found"
                        results["failed_conversions"] += 1
                        continue

                    try:
                        # Call the converter function with new parameters
                        output_file, total_lines = converter(input_file_path, validation_file_path, case_style, separator)

                        if output_file:
                            file_result["status"] = "success"
                            file_result["output_file"] = output_file
                            file_result["total_lines"] = total_lines  # Add total lines to the file result
                            results["successful_conversions"] += 1
                        else:
                            file_result["status"] = "failed"
                            file_result["reason"] = "Conversion returned None"
                            results["failed_conversions"] += 1
                    except Exception as e:
                        file_result["status"] = "failed"
                        file_result["reason"] = f"Error during conversion: {str(e)}"
                        results["failed_conversions"] += 1
                else:
                    file_result["reason"] = "No valid validation files found"
                    results["skipped_files"] += 1
                    # Track skipped file pair
                    results["skipped_file_pairs"].append({
                        "input_file": input_file_name,
                        "reason": "No valid validation files found"
                    })
            else:
                file_result["reason"] = f"File status is {file_info['status']}"
                results["skipped_files"] += 1
                # Track skipped file pair
                results["skipped_file_pairs"].append({
                    "input_file": input_file_name,
                    "reason": f"File status is {file_info['status']}"
                })

            results["details"].append(file_result)
            progress.update(task, advance=1)

    # Set final status
    results["status"] = "completed"

    # Save log file to both locations
    with open(timestamped_log_file, "w", encoding="latin1") as f:
        json.dump(results, f, indent=2)
    with open(latest_log_file, "w", encoding="latin1") as f:
        json.dump(results, f, indent=2)

    # Print summary
    console.print(f"[green]Conversion process completed")
    console.print(f"[green]Total files: {results['total_files']}")
    console.print(f"[green]Successfully converted: {results['successful_conversions']}")

    if results["failed_conversions"] > 0:
        console.print(f"[red]Failed conversions: {results['failed_conversions']}")

    if results["skipped_files"] > 0:
        console.print(f"[yellow]Skipped files: {results['skipped_files']}")

        # Display skipped file pairs
        for idx, skipped in enumerate(results["skipped_file_pairs"], 1):
            console.print(f"[yellow] {idx}. Input: {skipped['input_file']} - Reason: {skipped['reason']}[/yellow]")

    console.print(f"[blue]Log saved to: {os.path.basename(latest_log_file)} and conversion_log_{timestamp}.json in {log_folder}")

    return results  # Return the results

def main():
    """
    Main function to handle command line arguments and run conversions
    """
    parser = argparse.ArgumentParser(description='Convert fixed-width files to CSV format')
    parser.add_argument('--case-style', choices=['original', 'snake_case', 'camelCase', 'PascalCase'],
                       default='snake_case',
                       help='Case style for column names (default: snake_case)')
    parser.add_argument('--separator', choices=[',', ';', '|'], default=',',
                       help='CSV separator to use (default: comma)')
    parser.add_argument('--input-folder', default='data/01-input',
                       help='Input folder path (default: data/01-input)')

    args = parser.parse_args()

    # Run the conversion process
    results = run_conversions_from_matches(
        input_folder=args.input_folder,
        case_style=args.case_style,
        separator=args.separator
    )

    return results

if __name__ == "__main__":
    main()

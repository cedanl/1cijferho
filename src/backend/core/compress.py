# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Claude
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Simple script that converts all CSV files in a directory to Parquet format.
Uses Latin-1 encoding and pipe delimiter.

Usage:
    python csv_to_parquet.py [input_directory]
    
Arguments:
    input_directory: Directory containing CSV files (default: data/02-output)
"""

# TODO: Fix Dec_instellingscode.csv

import os
import sys
import polars as pl
from pathlib import Path

def convert_csv_to_parquet(input_dir="data/02-output"):
    """
    Convert all CSV files in the input directory to Parquet format.
    
    Args:
        input_dir (str): Directory containing CSV files
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        print(f"Error: Directory '{input_dir}' does not exist.")
        return
    
    csv_files = list(input_path.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    for csv_file in csv_files:
        parquet_file = csv_file.with_suffix(".parquet")
        
        print(f"Converting {csv_file.name} to {parquet_file.name}...")
        
        try:
            # Read CSV file with pipe delimiter and Latin-1 encoding
            df = pl.read_csv(
                csv_file,
                separator="|",
                encoding="latin-1",
                infer_schema_length=10000,  # Disable type inference
                ignore_errors=True
            )
            
            # Write to Parquet format
            df.write_parquet(parquet_file)
            
            print(f"Successfully converted {csv_file.name} to {parquet_file.name}")
        except Exception as e:
            print(f"Error converting {csv_file.name}: {str(e)}")

if __name__ == "__main__":
    # Get input directory from command line argument or use default
    input_dir = sys.argv[1] if len(sys.argv) > 1 else "data/02-output"
    convert_csv_to_parquet(input_dir)
    print("Conversion completed!")
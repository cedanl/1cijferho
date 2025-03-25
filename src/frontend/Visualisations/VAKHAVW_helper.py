import polars as pl
from pathlib import Path

def find_and_load_vakhavw_csv(directory):
    """Find and load first CSV file with 'VAKHAVW' in name as Polars DataFrame."""
    file_path = next(Path(directory).glob("*VAKHAVW*.parquet"))
    return pl.read_parquet(file_path)
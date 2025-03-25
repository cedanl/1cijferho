import polars as pl
from pathlib import Path

def find_and_load_ev_csv(directory):
    """Find and load first CSV file with 'EV' in name as Polars DataFrame."""
    file_path = next(Path(directory).glob("*EV*.parquet"))
    return pl.read_parquet(file_path)

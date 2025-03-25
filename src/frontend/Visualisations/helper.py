import polars as pl
from pathlib import Path

def find_and_load_ev_csv(directory):
    """Find and load first CSV file with 'EV' in name as Polars DataFrame."""
    file_path = next(Path(directory).glob("*EV*.parquet"))
    return pl.read_parquet(file_path)

def find_and_load_vakhavw_csv(directory):
    """Find and load first CSV file with 'VAKHAVW' in name as Polars DataFrame."""
    file_path = next(Path(directory).glob("*VAKHAVW*.parquet"))
    return pl.read_parquet(file_path)

df = find_and_load_ev_csv("data/02-output")
df.head()
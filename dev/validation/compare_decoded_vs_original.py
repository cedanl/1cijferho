"""
Script to compare original and decoded CSV files in the 02-output directory.
Checks for column differences and value changes for a quick test.
"""
import polars as pl
from pathlib import Path

OUTPUT_DIR = Path("data/02-output")

# Find all original and decoded CSV pairs
def find_csv_pairs(directory):
    originals = list(directory.glob("*_DEMO.csv"))
    pairs = []
    for orig in originals:
        decoded = orig.with_name(orig.stem + "_decoded.csv")
        if decoded.exists():
            pairs.append((orig, decoded))
    return pairs

def compare_csvs(orig_path, decoded_path):
    print(f"\nComparing: {orig_path.name} <-> {decoded_path.name}")
    df_orig = pl.read_csv(orig_path, separator=";", encoding="latin1")
    df_dec = pl.read_csv(decoded_path, separator=";", encoding="latin1")
    # Always print column counts
    print(f"Original column count: {len(df_orig.columns)}")
    print(f"Decoded column count : {len(df_dec.columns)}")
    # Simple column count check
    if len(df_orig.columns) != len(df_dec.columns):
        print(f"[FAIL] Column count mismatch: original={len(df_orig.columns)}, decoded={len(df_dec.columns)}")
        # print("Original columns:", df_orig.columns)
        # print("Decoded columns :", df_dec.columns)
        print("Columns only in original:", set(df_orig.columns) - set(df_dec.columns))
        print("Columns only in decoded :", set(df_dec.columns) - set(df_orig.columns))
        return
    # Check columns
    if df_orig.columns != df_dec.columns:
        print("[FAIL] Column names differ!")
        print("Columns only in original:", set(df_orig.columns) - set(df_dec.columns))
        print("Columns only in decoded :", set(df_dec.columns) - set(df_orig.columns))
        print("Skipping value comparison due to column name mismatch.")
        return
    # Check for value changes
    # Fill nulls/NaNs, cast to string, and compare using pandas for robust handling
    df_orig_filled = df_orig.fill_null("").fill_nan("").with_columns([pl.col(c).cast(str) for c in df_orig.columns])
    df_dec_filled = df_dec.fill_null("").fill_nan("").with_columns([pl.col(c).cast(str) for c in df_dec.columns])
    pd_orig = df_orig_filled.to_pandas()
    pd_dec = df_dec_filled.to_pandas()
    diffs = (pd_orig.values != pd_dec.values).sum()
    if diffs == 0:
        print("[WARN] No differences found between original and decoded!")
    else:
        print(f"[OK] {diffs} value(s) differ between original and decoded.")
        # Optionally, show a sample of changed rows
        changed = (pd_orig.values != pd_dec.values).any(axis=1)
        if changed.sum() > 0:
            print("Sample changed rows:")
            print(pd_orig[changed].head(3))
            print(pd_dec[changed].head(3))

if __name__ == "__main__":
    pairs = find_csv_pairs(OUTPUT_DIR)
    if not pairs:
        print("No original/decoded CSV pairs found.")
    for orig, decoded in pairs:
        compare_csvs(orig, decoded)

"""
Dataset comparison script using sweetviz.

Generates HTML reports comparing two datasets side-by-side.

Usage:
    uv run python dev/validation/compare_data.py bestand1.csv bestand2.csv
    uv run python dev/validation/compare_data.py input.csv output.csv --labels "Input" "Output"
    uv run python dev/validation/compare_data.py file1.parquet file2.parquet --output vergelijking.html
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import sweetviz as sv


def load_data(file_path: Path) -> pl.DataFrame:
    """Load CSV or Parquet file into Polars DataFrame."""
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(file_path, separator=";", encoding="latin1", infer_schema_length=10000)
    elif suffix == ".parquet":
        return pl.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}. Use .csv or .parquet")


def main():
    parser = argparse.ArgumentParser(
        description="Compare two datasets using sweetviz"
    )
    parser.add_argument(
        "file1",
        type=Path,
        help="Path to first CSV or Parquet file",
    )
    parser.add_argument(
        "file2",
        type=Path,
        help="Path to second CSV or Parquet file",
    )
    parser.add_argument(
        "--labels",
        nargs=2,
        default=None,
        metavar=("LABEL1", "LABEL2"),
        help="Labels for the two datasets (default: filenames)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output HTML file path (default: dev/reports/compare_<timestamp>.html)",
    )

    args = parser.parse_args()

    if not args.file1.exists():
        print(f"Error: File not found: {args.file1}")
        sys.exit(1)
    if not args.file2.exists():
        print(f"Error: File not found: {args.file2}")
        sys.exit(1)

    print(f"Loading dataset 1: {args.file1}")
    df1 = load_data(args.file1)
    print(f"  -> {df1.shape[0]:,} rows x {df1.shape[1]} columns")

    print(f"Loading dataset 2: {args.file2}")
    df2 = load_data(args.file2)
    print(f"  -> {df2.shape[0]:,} rows x {df2.shape[1]} columns")

    # Convert to pandas for sweetviz
    pdf1 = df1.to_pandas()
    pdf2 = df2.to_pandas()

    # Set labels
    if args.labels:
        label1, label2 = args.labels
    else:
        label1 = args.file1.stem
        label2 = args.file2.stem

    print(f"Generating comparison report: '{label1}' vs '{label2}'...")
    report = sv.compare([pdf1, label1], [pdf2, label2])

    if args.output:
        output_path = args.output
    else:
        reports_dir = Path(__file__).parent.parent / "reports"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = reports_dir / f"compare_{timestamp}.html"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report.show_html(filepath=str(output_path), open_browser=False)
    print(f"Report saved to: {output_path}")


if __name__ == "__main__":
    main()

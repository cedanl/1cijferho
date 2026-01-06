"""
Data profiling script using ydata-profiling.

Generates HTML reports with data statistics, missing values, correlations, etc.

Usage:
    uv run python dev/validation/profile_data.py data/02-output/bestand.csv
    uv run python dev/validation/profile_data.py data/02-output/bestand.parquet --minimal
    uv run python dev/validation/profile_data.py data/02-output/bestand.csv --output rapport.html
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
from ydata_profiling import ProfileReport


def load_data(file_path: Path) -> pl.DataFrame:
    """Load CSV or Parquet file into Polars DataFrame."""
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(file_path, separator=";", encoding="latin1", infer_schema_length=10000)
    elif suffix == ".parquet":
        return pl.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}. Use .csv or .parquet")


def generate_profile(
    df: pl.DataFrame,
    title: str,
    minimal: bool = False,
) -> ProfileReport:
    """Generate ydata-profiling report from DataFrame."""
    pandas_df = df.to_pandas()

    if minimal:
        return ProfileReport(
            pandas_df,
            title=title,
            minimal=True,
            progress_bar=True,
        )
    else:
        return ProfileReport(
            pandas_df,
            title=title,
            explorative=True,
            progress_bar=True,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Generate data profiling report using ydata-profiling"
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Path to CSV or Parquet file to profile",
    )
    parser.add_argument(
        "--minimal",
        action="store_true",
        help="Generate minimal report (faster, less detailed)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output HTML file path (default: dev/reports/<filename>_profile.html)",
    )

    args = parser.parse_args()

    if not args.file.exists():
        print(f"Error: File not found: {args.file}")
        sys.exit(1)

    print(f"Loading data from: {args.file}")
    df = load_data(args.file)
    print(f"Loaded {df.shape[0]:,} rows x {df.shape[1]} columns")

    title = f"Data Profile: {args.file.name}"
    print(f"Generating {'minimal ' if args.minimal else ''}profile report...")
    profile = generate_profile(df, title, minimal=args.minimal)

    if args.output:
        output_path = args.output
    else:
        reports_dir = Path(__file__).parent.parent / "reports"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = reports_dir / f"{args.file.stem}_profile_{timestamp}.html"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    profile.to_file(output_path)
    print(f"Report saved to: {output_path}")


if __name__ == "__main__":
    main()

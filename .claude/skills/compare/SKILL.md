---
name: compare
description: Compare two datasets side-by-side using sweetviz. Creates HTML reports showing differences in distributions, statistics, and data quality between two CSV or Parquet files.
---

# Dataset Comparison Skill

Compare two datasets to identify differences in structure, distributions, and data quality.

## Usage

Run the comparison script on two data files:

```bash
uv run python dev/validation/compare_data.py <file1> <file2>
```

## Options

- `--labels <label1> <label2>` - Custom labels for the datasets (default: filenames)
- `--output <path>` or `-o <path>` - Custom output path (default: `dev/reports/compare_<timestamp>.html`)

## Examples

```bash
# Compare two versions of a dataset
uv run python dev/validation/compare_data.py data/02-output/old.csv data/02-output/new.csv

# Compare with custom labels
uv run python dev/validation/compare_data.py data/02-output/v1.csv data/02-output/v2.csv --labels "Versie 1" "Versie 2"

# Compare input vs output
uv run python dev/validation/compare_data.py data/01-input/raw.csv data/02-output/processed.csv --labels "Input" "Output"
```

## Use Cases

- **Before/After**: Vergelijk data voor en na een transformatie
- **Version comparison**: Vergelijk verschillende versies van dezelfde dataset
- **Quality check**: Controleer of output overeenkomt met verwachtingen

## Supported Formats

- CSV files (semicolon-delimited, latin1 encoding)
- Parquet files

## Output

Reports are saved to `dev/reports/` with ISO timestamps. The HTML report shows:
- Side-by-side column comparisons
- Distribution differences
- Statistical summaries
- Missing value patterns
- Data type comparisons

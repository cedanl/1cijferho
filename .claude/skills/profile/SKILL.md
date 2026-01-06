---
name: profile
description: Generate a data profiling report for CSV or Parquet files using ydata-profiling. Creates HTML reports with statistics, missing values, correlations, and data quality insights.
---

# Data Profiling Skill

Generate comprehensive data profiling reports for 1CijferHO output files.

## Usage

Run the profiling script on a data file:

```bash
uv run python dev/validation/profile_data.py <file_path>
```

## Options

- `--minimal` - Generate a faster, less detailed report
- `--output <path>` or `-o <path>` - Custom output path (default: `dev/reports/<filename>_profile_<timestamp>.html`)

## Examples

```bash
# Full profile report
uv run python dev/validation/profile_data.py data/02-output/inschrijvingen.csv

# Minimal report (faster)
uv run python dev/validation/profile_data.py data/02-output/inschrijvingen.csv --minimal

# Custom output location
uv run python dev/validation/profile_data.py data/02-output/inschrijvingen.csv -o my_report.html
```

## Supported Formats

- CSV files (semicolon-delimited, latin1 encoding)
- Parquet files

## Output

Reports are saved to `dev/reports/` with ISO timestamps. Open the HTML file in a browser to view:
- Data types and statistics per column
- Missing value analysis
- Correlation matrices
- Distribution plots
- Data quality warnings

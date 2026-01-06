# Claude Configuration - 1CijferHO

## Project Overview
1CijferHO is a CEDA/Npuls tool for transforming DUO educational data (fixed-width ASCII files) into research-ready CSV/Parquet formats. Built with Streamlit for an accessible UI that requires no programming knowledge from end users.

## Tech Stack
- **Python 3.13+** with **uv** for dependency management
- **Streamlit** for the web interface
- **Polars/Pandas** for data processing
- **Rich** for CLI output formatting

## Project Structure
```
src/
├── main.py              # Streamlit entrypoint
├── pipeline.py          # Processing pipeline
├── backend/
│   ├── core/            # Core processing (converter, decoder, extractor)
│   └── utils/           # Utilities (compression, validation, encryption)
├── frontend/
│   ├── Overview/        # Home, Documentation pages
│   ├── Files/           # Upload functionality
│   └── Modules/         # Extract, Validate, Turbo Convert, Tip
└── assets/              # Static assets (logos, demo)
data/
├── 01-input/            # Input files (fixed-width ASCII + metadata .txt)
└── 02-output/           # Output files (CSV/Parquet)
```

## Development Commands
```bash
# Run the application
uv run streamlit run src/main.py

# Lint with ruff
uv run ruff check .

# Format with ruff
uv run ruff format .

# Install dev dependencies (validation tools)
uv sync --extra dev
```

## Data Validation (Development)
Tools for quick visual validation of output during development. Location: `dev/`

### Claude Code Skills
Use the built-in skills for data validation:
- `/profile` - Generate a data profiling report for a file
- `/compare` - Compare two datasets side-by-side

### CLI Commands
```bash
# Profiling report (ydata-profiling)
uv run python dev/validation/profile_data.py data/02-output/file.csv
uv run python dev/validation/profile_data.py data/02-output/file.csv --minimal

# Compare datasets (sweetviz)
uv run python dev/validation/compare_data.py file1.csv file2.csv

# Interactive validation
uv run jupyter notebook dev/notebooks/validate_output.ipynb
```

Reports are saved to `dev/reports/` (not tracked in git).

## Data Pipeline Flow
1. **Upload** - Place DUO files in `data/01-input/`
2. **Extract Metadata** - Parse field positions from .txt metadata files
3. **Validate** - Check data integrity and field matching
4. **Turbo Convert** - Multiprocessing conversion to CSV/Parquet
5. **Output** - Clean files in `data/02-output/`

## Code Conventions
- Use Polars for large data operations (performance)
- Multiprocessing for file conversion (see `converter.py`)
- Latin1 encoding for DUO files
- Semicolon (`;`) as CSV delimiter
- Privacy: BSN and sensitive data are auto-anonymized

## Key Dependencies
- `polars` - Fast DataFrame operations
- `streamlit` / `streamlit-extras` - Web UI
- `pyjanitor` - Data cleaning utilities
- `rich` - Console output formatting

## Notes
- Demo mode: Place `*_DEMO*` files in `data/01-input/` for testing
- Version check: Compares local VERSION file against GitHub releases
- No tests currently - be careful with refactoring

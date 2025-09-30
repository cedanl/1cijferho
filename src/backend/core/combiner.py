"""
Data combiner for joining decoder files with main data.
Reads YAML configuration and performs all specified joins.

Functions:
    [M] load_yaml_config() - Load and parse YAML configuration
    [M] load_main_data() - Load processed main data from 02-processed
    [M] load_decoder_data() - Load and process decoder files
    [M] check_unique_join_columns() - Verify join columns are unique
    [M] generate_column_prefix() - Generate prefixes based on case style
    [M] perform_simple_join() - Execute simple column joins
    [M] perform_complex_join() - Execute multi-column joins
    [M] combine_all_data() - Main orchestrator function
    [M] main() - Command line entry point
"""

import os
import sys
import yaml
import polars as pl
import datetime
import argparse
import json
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from io import StringIO

# Add project root to Python path for imports
project_root = Path(__file__).resolve().parents[3]  # Go up 3 levels: core -> backend -> src -> project_root
sys.path.insert(0, str(project_root))

from src.backend.core.converter import convert_case

console = Console()

class CombinerLogger:
    """
    Logger class for capturing combiner output to both console and log files
    """
    def __init__(self, output_dir="data/03-combined"):
        self.output_dir = Path(output_dir)
        self.log_dir = self.output_dir / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Create timestamp
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # Setup log files
        self.timestamped_log_file = self.log_dir / f"combiner_log_{self.timestamp}.txt"
        self.latest_log_file = self.log_dir / f"combiner_log_latest.txt"

        # Initialize log content
        self.log_content = StringIO()

        # Setup console with file capture
        self.console = Console(file=self.log_content, width=120)

    def print(self, *args, **kwargs):
        """Print to both console and log file"""
        # Print to actual console
        console.print(*args, **kwargs)
        # Print to captured log
        self.console.print(*args, **kwargs)

    def save_logs(self):
        """Save captured logs to files"""
        log_text = self.log_content.getvalue()

        # Write timestamped log
        with open(self.timestamped_log_file, 'w', encoding='utf-8') as f:
            f.write(log_text)

        # Write latest log
        with open(self.latest_log_file, 'w', encoding='utf-8') as f:
            f.write(log_text)

        console.print(f"[green]📝 Logs saved to: {self.timestamped_log_file}")
        console.print(f"[green]📝 Latest log: {self.latest_log_file}")

    def get_log_files(self):
        """Return paths to the log files"""
        return {
            "timestamped_log": str(self.timestamped_log_file),
            "latest_log": str(self.latest_log_file),
            "timestamp": self.timestamp
        }

def load_yaml_config(config_path="decoder_mapping_config.yaml", logger=None):
    """
    Load and parse YAML configuration file
    """
    if logger is None:
        logger = console

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.print(f"[green]✅ YAML configuration loaded: {len(config['decoder_mappings'])} mappings found")
        return config
    except Exception as e:
        logger.print(f"[red]❌ Error loading YAML config: {str(e)}")
        raise

def load_main_data(processed_dir="data/02-processed", logger=None):
    """
    Load main data files from processed directory
    Returns dict of dataframes keyed by filename
    """
    if logger is None:
        logger = console

    main_data = {}
    processed_path = Path(processed_dir)

    if not processed_path.exists():
        logger.print(f"[red]❌ Processed directory not found: {processed_dir}")
        return main_data

    # Find CSV files (converted main data) - exclude decoder files
    csv_files = list(processed_path.glob("*.csv"))
    csv_files = [f for f in csv_files if not f.name.endswith('_encrypted.csv') and not f.name.startswith('Dec_')]

    logger.print(f"[cyan]📂 Loading main data files from {processed_dir}")

    for csv_file in csv_files:
        try:
            # Detect case style from first few column names
            df_preview = pl.read_csv(csv_file, n_rows=0)
            logger.print(f"[green]  ✅ Loaded: {csv_file.name} ({len(df_preview.columns)} columns)")

            # Load full data
            df = pl.read_csv(csv_file)
            main_data[csv_file.stem] = df

        except Exception as e:
            logger.print(f"[red]  ❌ Error loading {csv_file.name}: {str(e)}")

    return main_data

def detect_csv_separator(file_path):
    """
    Automatically detect CSV separator by checking the first line
    """
    with open(file_path, 'r', encoding='latin1') as f:
        first_line = f.readline().strip()

    # Count occurrences of potential separators
    separators = [';', ',', '|']
    separator_counts = {sep: first_line.count(sep) for sep in separators}

    # Return the separator with the highest count (most likely to be the delimiter)
    best_separator = max(separator_counts, key=separator_counts.get)
    return best_separator if separator_counts[best_separator] > 0 else ','

def load_decoder_data(decoder_file, input_dir="data/02-processed", logger=None):
    """
    Load and process a decoder file with automatic separator detection
    """
    if logger is None:
        logger = console

    decoder_path = Path(input_dir) / decoder_file

    if not decoder_path.exists():
        logger.print(f"[red]❌ Decoder file not found: {decoder_file}")
        return None

    try:
        # Auto-detect separator
        separator = detect_csv_separator(decoder_path)
        logger.print(f"[cyan]🔍 Detected separator '{separator}' for {decoder_file}")

        # Special handling for Dec_vooropl.csv - force first column as string
        if 'vooropl' in decoder_file.lower():
            # Get column names by reading just the header
            with open(decoder_path, 'r', encoding='latin1') as f:
                header_line = f.readline().strip()
            first_col = header_line.split(separator)[0]

            # Create schema with first column as string, others auto-detect
            schema_overrides = {first_col: pl.String}

            logger.print(f"[yellow]📝 Forcing first column '{first_col}' to string type for {decoder_file}")

            df = pl.read_csv(decoder_path,
                           encoding='latin1',
                           separator=separator,
                           schema_overrides=schema_overrides,
                           try_parse_dates=False)
        else:
            # Standard loading for other files
            df = pl.read_csv(decoder_path,
                           encoding='latin1',
                           separator=separator,
                           try_parse_dates=False,
                           infer_schema_length=None)

        logger.print(f"[green]  ✅ Loaded decoder: {decoder_file} ({len(df.columns)} columns, {len(df)} rows)")
        return df
    except Exception as e:
        logger.print(f"[red]❌ Error loading decoder file {decoder_file}: {str(e)}")
        return None

def load_mapping_tables_config(config_path="mapping_tables_config.yaml", logger=None):
    """
    Load and parse mapping tables configuration file
    """
    if logger is None:
        logger = console

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # Extract active (non-commented) files from the mapping_tables list
        active_files = []
        if 'mapping_tables' in config:
            active_files = [f for f in config['mapping_tables'] if f is not None]

        logger.print(f"[green]✅ Mapping tables configuration loaded: {len(active_files)} active files")
        return config, active_files
    except FileNotFoundError:
        logger.print(f"[yellow]⚠️  Mapping tables config not found: {config_path}, loading all files")
        return None, None
    except Exception as e:
        logger.print(f"[red]❌ Error loading mapping tables config: {str(e)}")
        return None, None

def load_reference_data(reference_dir="data/reference", active_files=None, logger=None):
    """
    Load reference files from reference directory
    If active_files is provided, only load those files that are active in the config
    Returns dict of dataframes keyed by filename (without .csv extension)
    """
    if logger is None:
        logger = console

    reference_data = {}
    reference_path = Path(reference_dir)

    if not reference_path.exists():
        logger.print(f"[yellow]⚠️  Reference directory not found: {reference_dir}")
        return reference_data, [], []

    # Find all CSV files in reference directory
    all_csv_files = list(reference_path.glob("*.csv"))
    all_filenames = [f.name for f in all_csv_files]

    # If active_files is provided, check which ones exist and which are missing
    if active_files is not None:
        # Check which active files exist
        existing_active_files = []
        missing_active_files = []

        for active_file in active_files:
            if active_file in all_filenames:
                existing_active_files.append(active_file)
            else:
                missing_active_files.append(active_file)

        # Check which existing files are not in the config
        files_not_in_config = [f for f in all_filenames if f not in active_files]

        logger.print(f"[cyan]📚 Loading reference files from {reference_dir}")
        logger.print(f"[cyan]Found {len(all_csv_files)} total files, {len(existing_active_files)} active in config")

        if missing_active_files:
            logger.print(f"[yellow]⚠️  Missing active files: {missing_active_files}")

        if files_not_in_config:
            logger.print(f"[blue]ℹ️  Files not in config (inactive): {files_not_in_config}")

        # Only process existing active files
        files_to_process = [f for f in all_csv_files if f.name in existing_active_files]
    else:
        # Load all files if no config provided
        files_to_process = all_csv_files
        existing_active_files = all_filenames
        missing_active_files = []
        files_not_in_config = []

        logger.print(f"[cyan]📚 Loading all reference files from {reference_dir}")
        logger.print(f"[cyan]Found {len(all_csv_files)} reference files")

    # Load the files
    for csv_file in files_to_process:
        try:
            # Load the reference file
            df = pl.read_csv(csv_file, encoding='utf-8')

            # Validate structure (should have 'value' and 'label' columns)
            if 'value' not in df.columns or 'label' not in df.columns:
                logger.print(f"[yellow]⚠️  Skipping {csv_file.name}: missing 'value' or 'label' columns")
                continue

            # Store with filename as key (without .csv extension)
            key = csv_file.stem
            reference_data[key] = df
            logger.print(f"[green]  ✅ Loaded reference: {csv_file.name} ({len(df)} rows)")

        except Exception as e:
            logger.print(f"[red]❌ Error loading reference file {csv_file.name}: {str(e)}")

    return reference_data, missing_active_files, files_not_in_config

def find_matching_reference_column(column_name, reference_data, case_style="snake_case", logger=None):
    """
    Find a matching reference file for a given column name
    Returns (reference_key, reference_df) if found, (None, None) if not found
    """
    if logger is None:
        logger = console


    # Clean column name and convert to snake_case for matching
    clean_column = column_name.replace('_', ' ').strip()
    snake_case_column = convert_case(clean_column, 'snake_case')

    # Look for reference file with _label suffix
    reference_key = f"{snake_case_column}_label"
    if reference_key in reference_data:
        return reference_key, reference_data[reference_key]

    return None, None

def perform_reference_joins(df, reference_data, case_style="snake_case", logger=None):
    """
    Join reference data to add label columns for all matching columns
    Returns (updated_df, successful_joins, failed_joins, non_working_files)
    """
    if logger is None:
        logger = console


    result_df = df.clone()
    successful_joins = 0
    failed_joins = 0
    non_working_files = []

    logger.print(f"[cyan]🔗 Adding reference labels to columns...")

    # Process each column in the dataframe
    for column in df.columns:
        # Find matching reference file
        ref_key, ref_df = find_matching_reference_column(column, reference_data, case_style, logger)

        if ref_df is not None:
            try:
                # Generate label column name with proper case style
                label_column_base = f"{column}_label"
                label_column = convert_case(label_column_base, case_style) if case_style != 'original' else label_column_base

                # Skip if label column already exists
                if label_column in result_df.columns:
                    continue

                # Prepare reference data for join
                ref_for_join = ref_df.select(['value', 'label']).rename({'label': label_column})

                # Check for datatype compatibility
                main_dtype = result_df[column].dtype
                ref_dtype = ref_for_join['value'].dtype

                if main_dtype != ref_dtype:
                    # Try to cast reference 'value' column to match main data type
                    try:
                        ref_for_join = ref_for_join.with_columns(
                            pl.col('value').cast(main_dtype)
                        )
                    except Exception:
                        # Fall back to string casting for both
                        ref_for_join = ref_for_join.with_columns(
                            pl.col('value').cast(pl.String)
                        )
                        # Join will cast main column to string on-the-fly

                # Perform left join
                try:
                    result_df = result_df.join(
                        ref_for_join,
                        left_on=column,
                        right_on='value',
                        how='left'
                    )
                    successful_joins += 1
                    logger.print(f"[green]  ✅ Added {label_column} from {ref_key}.csv")

                except Exception as join_error:
                    # Try string fallback
                    try:
                        result_df = result_df.with_columns(
                            pl.col(column).cast(pl.String).alias(f"{column}_str_temp")
                        ).join(
                            ref_for_join,
                            left_on=f"{column}_str_temp",
                            right_on='value',
                            how='left'
                        ).drop(f"{column}_str_temp")
                        successful_joins += 1
                        logger.print(f"[green]  ✅ Added {label_column} from {ref_key}.csv (string fallback)")
                    except Exception as fallback_error:
                        logger.print(f"[red]❌ Failed to join {ref_key}.csv to {column}: {str(fallback_error)}")
                        failed_joins += 1
                        non_working_files.append({
                            'file': f"{ref_key}.csv",
                            'column': column,
                            'error': str(fallback_error)
                        })

            except Exception as e:
                logger.print(f"[red]❌ Error processing reference {ref_key}.csv for {column}: {str(e)}")
                failed_joins += 1
                non_working_files.append({
                    'file': f"{ref_key}.csv",
                    'column': column,
                    'error': str(e)
                })

    return result_df, successful_joins, failed_joins, non_working_files

def load_manual_mapping_config(config_path="manual_tables_config.yaml", logger=None):
    """
    Load and parse manual mapping tables configuration file
    """
    if logger is None:
        logger = console

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # Extract active mappings from the manual_mapping_tables list
        active_mappings = []
        if 'manual_mapping_tables' in config:
            for mapping in config['manual_mapping_tables']:
                if mapping is not None:
                    # Handle both old format (just filenames) and new format (dict with file, join_on, output_column)
                    if isinstance(mapping, str):
                        # Old format - just filename
                        active_mappings.append({
                            'file': mapping,
                            'join_on': None,  # Will be auto-detected
                            'output_column': None  # Will be auto-generated
                        })
                    elif isinstance(mapping, dict):
                        # New format - dict with configuration
                        active_mappings.append(mapping)

        logger.print(f"[green]✅ Manual mapping config loaded: {len(active_mappings)} manual mapping configurations")
        return config, active_mappings
    except FileNotFoundError:
        logger.print(f"[yellow]⚠️  Manual mapping config not found: {config_path}")
        return None, None
    except Exception as e:
        logger.print(f"[red]❌ Error loading manual mapping config: {str(e)}")
        return None, None

def load_manual_mapping_data(manual_dir="data/reference/manual", active_mappings=None, logger=None):
    """
    Load manual mapping files from manual directory
    Returns dict of dataframes keyed by filename (without .csv extension), and mapping configurations
    """
    if logger is None:
        logger = console

    manual_data = {}
    mapping_configs = {}
    manual_path = Path(manual_dir)

    if not manual_path.exists():
        logger.print(f"[yellow]⚠️  Manual mapping directory not found: {manual_dir}")
        return manual_data, mapping_configs, []

    # Find all CSV files in manual directory
    all_csv_files = list(manual_path.glob("*.csv"))

    if not all_csv_files:
        logger.print(f"[yellow]⚠️  No CSV files found in manual directory: {manual_dir}")
        return manual_data, mapping_configs, []

    # Filter files based on active_mappings configurations
    files_to_load = []
    missing_active_files = []

    if active_mappings:
        for mapping_config in active_mappings:
            filename = mapping_config['file']
            file_path = manual_path / filename

            if file_path.exists():
                files_to_load.append((file_path, mapping_config))
            else:
                missing_active_files.append(filename)
    else:
        # Load all files if no config provided
        for csv_file in all_csv_files:
            # Create default mapping config
            default_config = {
                'file': csv_file.name,
                'join_on': None,  # Will be auto-detected
                'output_column': None  # Will be auto-generated
            }
            files_to_load.append((csv_file, default_config))

    logger.print(f"[cyan]📁 Loading {len(files_to_load)} manual mapping files...")

    for csv_file, mapping_config in files_to_load:
        try:
            # Read the manual mapping file
            df = pl.read_csv(csv_file, separator=',')

            # Validate that it has the expected columns
            if not all(col in df.columns for col in ['value', 'label']):
                logger.print(f"[yellow]⚠️  Manual mapping file {csv_file.name} missing required columns (value, label)")
                continue

            # Store with filename as key (without .csv extension)
            key = csv_file.stem
            manual_data[key] = df
            mapping_configs[key] = mapping_config
            logger.print(f"[green]  ✅ Loaded manual mapping: {csv_file.name} ({len(df)} mappings)")

        except Exception as e:
            logger.print(f"[red]❌ Error loading manual mapping file {csv_file.name}: {str(e)}")

    if missing_active_files:
        logger.print(f"[yellow]⚠️  Manual mapping files not found: {missing_active_files}")

    return manual_data, mapping_configs, missing_active_files

def apply_manual_mappings(df, manual_data, mapping_configs, logger=None):
    """
    Apply manual categorical mappings to dataframe columns
    Uses configuration to determine join columns and output column names
    """
    if logger is None:
        logger = console

    result_df = df.clone()
    successful_mappings = 0
    failed_mappings = 0


    # Process each manual mapping configuration
    for key, manual_df in manual_data.items():
        if key not in mapping_configs:
            logger.print(f"[yellow]⚠️  No configuration found for {key}, skipping")
            continue

        config = mapping_configs[key]
        join_on = config.get('join_on')
        output_column = config.get('output_column')

        # Auto-detect join column if not specified
        if not join_on:
            # Try to find a matching column in the dataframe
            # Use the key (filename without .csv) as the starting point
            potential_columns = [
                key,
                convert_case(key.replace('_cat', '').replace('_NL', '').replace('_profiel_cat', '').replace('_per_jaar', '').replace('_per_5_jaar_cat', ''), 'snake_case')
            ]

            join_on = None
            for potential_col in potential_columns:
                if potential_col in df.columns:
                    join_on = potential_col
                    break

            if not join_on:
                logger.print(f"[yellow]⚠️  Could not find matching column for {key}, skipping")
                continue

        # Auto-generate output column name if not specified
        if not output_column:
            output_column = key  # Use filename without .csv as default

        # Check if join column exists
        if join_on not in df.columns:
            logger.print(f"[yellow]⚠️  Join column '{join_on}' not found for {key}, skipping")
            continue

        try:
            # Perform the mapping join
            result_df = result_df.join(
                manual_df.select([
                    pl.col('value'),
                    pl.col('label').alias(output_column)
                ]),
                left_on=join_on,
                right_on='value',
                how='left'
            )

            successful_mappings += 1
            logger.print(f"[green]  ✅ Added manual mapping: {join_on} → {output_column}")

        except Exception as e:
            # Try string fallback
            try:
                result_df = result_df.with_columns(
                    pl.col(join_on).cast(pl.String).alias(f"{join_on}_str_temp")
                ).join(
                    manual_df.select([
                        pl.col('value').cast(pl.String),
                        pl.col('label').alias(output_column)
                    ]),
                    left_on=f"{join_on}_str_temp",
                    right_on='value',
                    how='left'
                ).drop(f"{join_on}_str_temp")

                successful_mappings += 1
                logger.print(f"[green]  ✅ Added manual mapping: {join_on} → {output_column} (string fallback)")

            except Exception as fallback_error:
                logger.print(f"[red]❌ Failed manual mapping {key}: {str(fallback_error)}")
                failed_mappings += 1

    return result_df, successful_mappings, failed_mappings

def check_unique_join_columns(df, join_columns):
    """
    Check if join columns form unique combinations
    """
    if isinstance(join_columns, str):
        join_columns = [join_columns]

    # Check if all join columns exist
    missing_cols = [col for col in join_columns if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"

    # Check uniqueness
    unique_count = df.select(join_columns).unique().height
    total_count = df.height

    if unique_count != total_count:
        return False, f"Join columns not unique: {unique_count} unique vs {total_count} total rows"

    return True, "OK"

def generate_column_prefix(join_column, target_column, case_style="snake_case"):
    """
    Generate column prefix based on join column, target column and case style

    Formats:
    - snake_case: {join_column}_{target_column}
    - camelCase: {join_column}{TargetColumn}
    - PascalCase: {JoinColumn}{TargetColumn}
    - original: {join_column} {target_column}
    """

    # Clean up column names (remove special characters)
    join_clean = join_column.replace('(', '').replace(')', '').replace('-', ' ').strip()
    target_clean = target_column.replace('(', '').replace(')', '').replace('-', ' ').strip()

    if case_style == "snake_case":
        # Convert to snake_case and join with underscore
        join_part = convert_case(join_clean, "snake_case")
        target_part = convert_case(target_clean, "snake_case")
        return f"{join_part}_{target_part}"

    elif case_style == "camelCase":
        # First part lowercase, second part title case, no separator
        join_part = convert_case(join_clean, "camelCase")
        target_part = convert_case(target_clean, "PascalCase")  # PascalCase for second part
        return f"{join_part}{target_part}"

    elif case_style == "PascalCase":
        # Both parts title case, no separator
        join_part = convert_case(join_clean, "PascalCase")
        target_part = convert_case(target_clean, "PascalCase")
        return f"{join_part}{target_part}"

    else:  # "original"
        # Keep original case and join with space
        return f"{join_clean} {target_clean}"

def perform_simple_join(main_df, decoder_df, mapping_config, case_style="snake_case", logger=None):
    """
    Perform a simple join operation
    """
    if logger is None:
        logger = console

    main_column = mapping_config['main_column']
    join_column = mapping_config['join_column']
    additional_columns = mapping_config['additional_columns']

    # Apply case conversion to column names for matching
    main_column_converted = convert_case(main_column, case_style)
    join_column_converted = convert_case(join_column, case_style)
    additional_columns_converted = [convert_case(col, case_style) for col in additional_columns]

    # Check if main column exists
    if main_column_converted not in main_df.columns:
        logger.print(f"[yellow]⚠️  Main column not found: {main_column_converted}")
        return main_df

    # Dynamic column matching - try different case conversions if exact match fails
    if join_column_converted not in decoder_df.columns:
        logger.print(f"[yellow]⚠️  Exact join column not found: {join_column_converted}")
        logger.print(f"[cyan]🔍 Trying dynamic column matching...")

        # Try different case styles to find matching column
        for try_case in ['snake_case', 'camelCase', 'PascalCase', 'original']:
            if try_case == case_style:
                continue  # Skip the one we already tried

            try_join_column = convert_case(join_column, try_case)
            if try_join_column in decoder_df.columns:
                logger.print(f"[green]✅ Found matching column with {try_case}: {try_join_column}")
                join_column_converted = try_join_column
                break
        else:
            # No match found with any case style
            logger.print(f"[red]❌ No matching column found in any case style")
            logger.print(f"[cyan]Available columns: {decoder_df.columns}")
            return main_df

    # Check uniqueness of join column in decoder
    is_unique, msg = check_unique_join_columns(decoder_df, join_column_converted)
    if not is_unique:
        logger.print(f"[red]❌ {msg}")
        return main_df

    # Prepare decoder columns with prefixes
    decoder_select_cols = [join_column_converted]
    rename_mapping = {}

    for add_col in additional_columns_converted:
        if add_col in decoder_df.columns:
            # Generate prefix using MAIN COLUMN (not join column) to ensure uniqueness
            prefix = generate_column_prefix(main_column_converted, add_col.replace('_', ' '), case_style)
            new_name = f"{prefix}"
            decoder_select_cols.append(add_col)
            rename_mapping[add_col] = new_name

    # Select and rename decoder columns
    decoder_subset = decoder_df.select(decoder_select_cols)
    if rename_mapping:
        decoder_subset = decoder_subset.rename(rename_mapping)

    # Fix datatype mismatch by casting join columns to compatible types
    try:
        main_dtype = main_df[main_column_converted].dtype
        decoder_dtype = decoder_subset[join_column_converted].dtype

        if main_dtype != decoder_dtype:
            logger.print(f"[yellow]⚠️  Datatype mismatch detected: main={main_dtype}, decoder={decoder_dtype}")

            # Try casting decoder to main type first
            try:
                logger.print(f"[cyan]🔧 Attempting to cast decoder column {join_column_converted} to {main_dtype}")
                decoder_subset = decoder_subset.with_columns(
                    pl.col(join_column_converted).cast(main_dtype)
                )
                logger.print(f"[green]✅ Successfully cast decoder column to {main_dtype}")
            except Exception as cast_error:
                # If that fails, try casting both to string as fallback
                logger.print(f"[yellow]⚠️  Decoder→main casting failed: {str(cast_error)}")
                logger.print(f"[cyan]🔧 Falling back to String casting for both columns")

                decoder_subset = decoder_subset.with_columns(
                    pl.col(join_column_converted).cast(pl.String)
                )
                # We'll perform join with main column cast to string on-the-fly
                main_column_for_join = main_column_converted
                logger.print(f"[green]✅ Both columns will be joined as String type")

    except Exception as e:
        logger.print(f"[yellow]⚠️  Could not fix datatype mismatch: {str(e)}")

    # Perform left join (with string fallback if needed)
    try:
        result_df = main_df.join(
            decoder_subset,
            left_on=main_column_converted,
            right_on=join_column_converted,
            how="left"
        )
    except Exception as join_error:
        # Last resort: cast main column to string on-the-fly for join
        logger.print(f"[yellow]⚠️  Join failed, trying string fallback: {str(join_error)}")
        try:
            result_df = main_df.with_columns(
                pl.col(main_column_converted).cast(pl.String).alias(f"{main_column_converted}_str_temp")
            ).join(
                decoder_subset,
                left_on=f"{main_column_converted}_str_temp",
                right_on=join_column_converted,
                how="left"
            ).drop(f"{main_column_converted}_str_temp")
            logger.print(f"[green]✅ String fallback join successful")
        except Exception as fallback_error:
            logger.print(f"[red]❌ Both join attempts failed: {str(fallback_error)}")
            return main_df

    logger.print(f"[green]  ✅ Joined {len(additional_columns)} columns from {mapping_config.get('decoder_file', 'decoder')}")
    return result_df

def perform_complex_join(main_df, decoder_df, mapping_config, case_style="snake_case", logger=None):
    """
    Perform a complex multi-column join operation
    """
    if logger is None:
        logger = console

    main_column = mapping_config['main_column']
    join_columns = mapping_config['join_columns']
    additional_columns = mapping_config['additional_columns']


    # Apply case conversion
    main_column_converted = convert_case(main_column, case_style)
    join_columns_converted = [convert_case(col, case_style) for col in join_columns]
    additional_columns_converted = [convert_case(col, case_style) for col in additional_columns]

    # For complex joins, we need to map main columns to decoder columns
    # For vestiging example: we join on [Instelling, Vestigingsnummer] -> [Brinnummer, Vestigingsnummer]

    # Map join columns to their corresponding main data columns
    main_join_columns = []
    for i, join_col in enumerate(join_columns):
        if join_col == "Brinnummer":
            # Map to "Instelling van de hoogste vooropleiding"
            main_join_columns.append(convert_case("Instelling van de hoogste vooropleiding", case_style))
        elif join_col == "Vestigingsnummer":
            # Use the main column itself
            main_join_columns.append(main_column_converted)
        else:
            # Default: use the join column name
            main_join_columns.append(convert_case(join_col, case_style))

    # Check if all main columns exist
    missing_main_cols = [col for col in main_join_columns if col not in main_df.columns]
    if missing_main_cols:
        logger.print(f"[yellow]⚠️  Main columns not found: {missing_main_cols}")
        return main_df

    # Check if join columns exist in decoder
    missing_decoder_cols = [col for col in join_columns_converted if col not in decoder_df.columns]
    if missing_decoder_cols:
        logger.print(f"[yellow]⚠️  Decoder columns not found: {missing_decoder_cols}")
        return main_df

    # Check uniqueness of join columns in decoder
    is_unique, msg = check_unique_join_columns(decoder_df, join_columns_converted)
    if not is_unique:
        logger.print(f"[red]❌ {msg}")
        return main_df

    # Prepare decoder columns with prefixes
    decoder_select_cols = join_columns_converted.copy()
    rename_mapping = {}

    for add_col in additional_columns_converted:
        if add_col in decoder_df.columns:
            # Generate prefix using MAIN COLUMN to ensure uniqueness
            prefix = generate_column_prefix(main_column_converted, add_col.replace('_', ' '), case_style)
            new_name = f"{prefix}"
            decoder_select_cols.append(add_col)
            rename_mapping[add_col] = new_name

    # Select and rename decoder columns
    decoder_subset = decoder_df.select(decoder_select_cols)
    if rename_mapping:
        decoder_subset = decoder_subset.rename(rename_mapping)

    # Fix datatype mismatches by casting decoder join columns to match main data types
    try:
        need_casting = False
        cast_operations = []

        for main_col, decoder_col in zip(main_join_columns, join_columns_converted):
            main_dtype = main_df[main_col].dtype
            decoder_dtype = decoder_subset[decoder_col].dtype

            if main_dtype != decoder_dtype:
                logger.print(f"[yellow]⚠️  Datatype mismatch detected: {decoder_col} main={main_dtype}, decoder={decoder_dtype}")
                logger.print(f"[cyan]🔧 Casting decoder column {decoder_col} to {main_dtype}")
                cast_operations.append(pl.col(decoder_col).cast(main_dtype))
                need_casting = True

        if need_casting:
            decoder_subset = decoder_subset.with_columns(cast_operations)
    except Exception as e:
        logger.print(f"[yellow]⚠️  Could not fix datatype mismatches: {str(e)}")

    # Perform left join on multiple columns
    result_df = main_df.join(
        decoder_subset,
        left_on=main_join_columns,
        right_on=join_columns_converted,
        how="left"
    )

    logger.print(f"[green]  ✅ Complex join: {len(additional_columns)} columns from {mapping_config.get('decoder_file', 'decoder')}")
    return result_df

def combine_all_data(config_path="decoding_files_config.yaml",
                    mapping_tables_config_path="mapping_tables_config.yaml",
                    manual_tables_config_path="manual_tables_config.yaml",
                    processed_dir="data/02-processed",
                    input_dir="data/02-processed",
                    output_dir="data/03-combined",
                    case_style="snake_case"):
    """
    Main function to combine all data according to YAML configuration
    """
    # Initialize logger
    logger = CombinerLogger(output_dir)

    logger.print("[bold blue]🔄 Starting data combination process...[/bold blue]")
    logger.print(f"[cyan]Settings: case_style={case_style}")

    # Load decoder configuration
    config = load_yaml_config(config_path, logger)
    mappings = config['decoder_mappings']
    settings = config.get('settings', {})

    # Load main data
    main_data = load_main_data(processed_dir, logger)
    if not main_data:
        logger.print("[red]❌ No main data found")
        return

    # Load mapping tables configuration and reference data
    logger.print(f"\n[bold magenta]📚 Loading reference data configuration...[/bold magenta]")
    mapping_config, active_files = load_mapping_tables_config(mapping_tables_config_path, logger)

    # Determine reference directory
    reference_dir = "data/reference"
    if mapping_config and 'settings' in mapping_config and 'reference_dir' in mapping_config['settings']:
        reference_dir = mapping_config['settings']['reference_dir']

    # Load reference data with active file filtering
    reference_data, missing_files, inactive_files = load_reference_data(reference_dir, active_files, logger)

    # Load manual mapping tables configuration and data
    logger.print(f"\n[bold yellow]🎯 Loading manual mapping configuration...[/bold yellow]")
    manual_config, manual_active_mappings = load_manual_mapping_config(manual_tables_config_path, logger)

    # Determine manual mapping directory
    manual_dir = "data/reference/manual"
    if manual_config and 'settings' in manual_config and 'manual_dir' in manual_config['settings']:
        manual_dir = manual_config['settings']['manual_dir']

    # Load manual mapping data with configurations
    manual_data, mapping_configs, manual_missing_files = load_manual_mapping_data(manual_dir, manual_active_mappings, logger)

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Global tracking for reference joins
    all_non_working_files = []
    total_reference_joins = 0
    total_reference_successes = 0
    total_reference_failures = 0

    # Global tracking for manual mappings
    total_manual_mappings = 0
    total_manual_successes = 0
    total_manual_failures = 0

    # Process each main data file
    for data_name, main_df in main_data.items():
        logger.print(f"\n[bold cyan]📊 Processing: {data_name}[/bold cyan]")

        combined_df = main_df.clone()
        successful_joins = 0
        failed_joins = 0

        # Process each mapping
        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]Applying joins..."),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:

            task = progress.add_task("", total=len(mappings))

            for mapping_name, mapping_config in mappings.items():
                decoder_file = mapping_config.get('decoder_file')
                decoder_files = mapping_config.get('decoder_files')

                # Handle both single decoder_file and multiple decoder_files
                files_to_process = []
                if decoder_file:
                    files_to_process.append(decoder_file)
                elif decoder_files:
                    # For simplified version, just take the first file
                    files_to_process.append(decoder_files[0]['file'])

                for dec_file in files_to_process:
                    # Load decoder data
                    decoder_df = load_decoder_data(dec_file, input_dir, logger)
                    if decoder_df is None:
                        failed_joins += 1
                        continue

                    try:
                        # Check if it's a complex join
                        if 'join_columns' in mapping_config:
                            combined_df = perform_complex_join(combined_df, decoder_df, mapping_config, case_style, logger)
                        else:
                            combined_df = perform_simple_join(combined_df, decoder_df, mapping_config, case_style, logger)

                        successful_joins += 1
                    except Exception as e:
                        logger.print(f"[red]❌ Join failed for {mapping_name}: {str(e)}")
                        failed_joins += 1

                progress.update(task, advance=1)

        # Add reference data joins
        logger.print(f"\n[bold magenta]📚 Adding reference labels for {data_name}...[/bold magenta]")
        combined_df, ref_successes, ref_failures, non_working = perform_reference_joins(
            combined_df, reference_data, case_style, logger
        )

        # Update global tracking
        total_reference_joins += (ref_successes + ref_failures)
        total_reference_successes += ref_successes
        total_reference_failures += ref_failures
        all_non_working_files.extend(non_working)

        # Apply manual categorical mappings
        logger.print(f"\n[bold yellow]🎯 Applying manual categorical mappings for {data_name}...[/bold yellow]")
        combined_df, manual_successes, manual_failures = apply_manual_mappings(
            combined_df, manual_data, mapping_configs, logger
        )

        # Update global manual mapping tracking
        total_manual_mappings += (manual_successes + manual_failures)
        total_manual_successes += manual_successes
        total_manual_failures += manual_failures

        # Save combined data
        output_file = output_path / f"{data_name}_combined.csv"
        combined_df.write_csv(output_file)

        # Summary
        logger.print(f"\n[bold green]✅ Completed: {data_name}[/bold green]")
        logger.print(f"  📊 Original columns: {len(main_df.columns)}")
        logger.print(f"  📊 Final columns: {len(combined_df.columns)}")
        logger.print(f"  📊 Added columns: {len(combined_df.columns) - len(main_df.columns)}")
        logger.print(f"  ✅ Successful decoder joins: {successful_joins}")
        logger.print(f"  ❌ Failed decoder joins: {failed_joins}")
        logger.print(f"  ✅ Successful reference joins: {ref_successes}")
        logger.print(f"  ❌ Failed reference joins: {ref_failures}")
        logger.print(f"  🎯 Successful manual mappings: {manual_successes}")
        logger.print(f"  ❌ Failed manual mappings: {manual_failures}")
        logger.print(f"  💾 Saved to: {output_file}")

    # Final summary for reference data
    logger.print(f"\n[bold magenta]📚 Reference Data Summary[/bold magenta]")
    logger.print(f"  📊 Total reference files loaded: {len(reference_data)}")
    logger.print(f"  ✅ Total successful reference joins: {total_reference_successes}")
    logger.print(f"  ❌ Total failed reference joins: {total_reference_failures}")

    # Report configuration status
    if active_files is not None:
        logger.print(f"  📋 Files configured as active: {len(active_files)}")
        if missing_files:
            logger.print(f"  ⚠️  Missing configured files: {missing_files}")
        if inactive_files:
            logger.print(f"  💤 Files not in config (inactive): {len(inactive_files)} files")

    # Final summary for manual mappings
    logger.print(f"\n[bold yellow]🎯 Manual Mapping Summary[/bold yellow]")
    logger.print(f"  📊 Total manual mapping files loaded: {len(manual_data)}")
    logger.print(f"  ✅ Total successful manual mappings: {total_manual_successes}")
    logger.print(f"  ❌ Total failed manual mappings: {total_manual_failures}")

    # Report manual mapping configuration status
    if manual_active_mappings is not None:
        logger.print(f"  📋 Manual mappings configured as active: {len(manual_active_mappings)}")
        if manual_missing_files:
            logger.print(f"  ⚠️  Missing manual configured files: {manual_missing_files}")

    # Report non-working files
    if all_non_working_files:
        logger.print(f"\n[bold red]❌ Non-working reference files ({len(all_non_working_files)}):[/bold red]")
        for nw in all_non_working_files:
            logger.print(f"  [red]• {nw['file']} (column: {nw['column']}): {nw['error']}")
    else:
        logger.print(f"[bold green]🎉 All reference files processed successfully![/bold green]")

    # Save logs to files
    logger.save_logs()

    # Return log info with reference summary
    log_info = logger.get_log_files()
    log_info['reference_summary'] = {
        'total_files': len(reference_data),
        'successful_joins': total_reference_successes,
        'failed_joins': total_reference_failures,
        'non_working_files': all_non_working_files,
        'missing_configured_files': missing_files,
        'inactive_files': inactive_files,
        'active_files_count': len(active_files) if active_files else 0
    }
    return log_info

def main():
    """
    Command line entry point
    """
    parser = argparse.ArgumentParser(description='Combine main data with decoder files')
    parser.add_argument('--config', default='decoding_files_config.yaml',
                       help='Path to YAML decoder configuration file')
    parser.add_argument('--mapping-tables-config', default='mapping_tables_config.yaml',
                       help='Path to YAML mapping tables configuration file')
    parser.add_argument('--manual-tables-config', default='manual_tables_config.yaml',
                       help='Path to YAML manual mapping tables configuration file')
    parser.add_argument('--processed-dir', default='data/02-processed',
                       help='Directory with processed main data')
    parser.add_argument('--input-dir', default='data/02-processed',
                       help='Directory with decoder files')
    parser.add_argument('--output-dir', default='data/03-combined',
                       help='Output directory for combined data')
    parser.add_argument('--case-style', default='snake_case',
                       choices=['original', 'snake_case', 'camelCase', 'PascalCase'],
                       help='Case style used in processed data')

    args = parser.parse_args()

    log_info = combine_all_data(
        config_path=args.config,
        mapping_tables_config_path=args.mapping_tables_config,
        manual_tables_config_path=args.manual_tables_config,
        processed_dir=args.processed_dir,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        case_style=args.case_style
    )

    if log_info:
        print(f"Logs saved: {log_info['timestamped_log']}")
        return log_info

if __name__ == "__main__":
    main()

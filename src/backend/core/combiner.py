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
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from converter import convert_case

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
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from converter import convert_case
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

    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from converter import convert_case

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

def combine_all_data(config_path="decoder_mapping_config.yaml",
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

    # Load configuration
    config = load_yaml_config(config_path, logger)
    mappings = config['decoder_mappings']
    settings = config.get('settings', {})

    # Load main data
    main_data = load_main_data(processed_dir, logger)
    if not main_data:
        logger.print("[red]❌ No main data found")
        return

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

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

        # Save combined data
        output_file = output_path / f"{data_name}_combined.csv"
        combined_df.write_csv(output_file)

        # Summary
        logger.print(f"\n[bold green]✅ Completed: {data_name}[/bold green]")
        logger.print(f"  📊 Original columns: {len(main_df.columns)}")
        logger.print(f"  📊 Final columns: {len(combined_df.columns)}")
        logger.print(f"  📊 Added columns: {len(combined_df.columns) - len(main_df.columns)}")
        logger.print(f"  ✅ Successful joins: {successful_joins}")
        logger.print(f"  ❌ Failed joins: {failed_joins}")
        logger.print(f"  💾 Saved to: {output_file}")

    # Save logs to files
    logger.save_logs()

    # Return log info
    return logger.get_log_files()

def main():
    """
    Command line entry point
    """
    parser = argparse.ArgumentParser(description='Combine main data with decoder files')
    parser.add_argument('--config', default='decoder_mapping_config.yaml',
                       help='Path to YAML configuration file')
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

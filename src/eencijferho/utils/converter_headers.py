import polars as pl
import re
import unicodedata
from rich.console import Console
from collections.abc import Callable
from eencijferho.io.decorators import with_storage

def normalize_name(name: str, naming_func: Callable[[str], str] | None = None) -> str:
    """
    Normalize variable names using the provided naming convention function (e.g., snake_case).
    If no function is provided, defaults to snake_case.
    """
    if naming_func:
        return naming_func(name)
    # First, convert accented letters to ASCII equivalents
    name = strip_accents(name)
    name = name.lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    return name

def normalize_polars_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rename all columns of a Polars DataFrame using normalize_name and strip_accents.
    """
    return df.rename({col: strip_accents(normalize_name(col)) for col in df.columns})

def clean_header_name(name: str) -> str:
    """
    Cleans and normalizes a header name by removing diacritics and replacing problematic characters.

    Args:
        name (str): The header name to clean.

    Returns:
        str: Cleaned header name.

    Edge Cases:
        - Handles fraction slashes (U+2044 → /).
        - Strips leading/trailing whitespace.

    Example:
        >>> clean_header_name('vóór')
        'voor'
    """
    name = unicodedata.normalize('NFKD', str(name))
    name = ''.join(c for c in name if not unicodedata.combining(c))
    name = name.replace('⁄', '/')  # Replace fraction slash if present
    name = name.strip()
    return name

def strip_accents(text: str) -> str:
    """Remove accents from text (e.g., 'vóór' -> 'voor', 'é' -> 'e')."""
    normalized = unicodedata.normalize('NFKD', text)
    # Encode to ASCII, ignoring errors (drops accents, keeps base letter)
    return normalized.encode('ascii', 'ignore').decode('ascii')

console = Console()


@with_storage
def convert_csv_headers_to_snake_case(
    storage,
    input_dir: str | None = None,
    delimiter: str = ";",
    encoding: str = "utf-8",
    quote_char: str = '"',
    infer_schema_length: int | None = 0
) -> None:
    """
    Convert all CSV file headers in the input directory to snake_case.

    Args:
        input_dir: Path to directory containing CSV files
        delimiter: CSV delimiter (default: ";")
        encoding: File encoding (default: "utf-8")
        quote_char: Quote character to use. Use "" to disable quoting (default: "")
        infer_schema_length: Number of rows to scan for schema inference (default: None - scan all)
    """
    # Use dynamic config default if not provided
    if input_dir is None:
        from eencijferho.config import get_output_dir
        input_dir = get_output_dir()

    # Find all CSV files
    csv_files = storage.list_files(f"{input_dir}/*.csv")

    if not csv_files:
        console.print(f"[yellow]No CSV files found in '{input_dir}'[/yellow]")
        return

    console.print(f"[cyan]Found {len(csv_files)} CSV file(s) in '{input_dir}'[/cyan]\n")

    failed_files = []

    for filepath in csv_files:
        fname = filepath.rsplit("/", 1)[-1] if "/" in filepath else filepath
        try:
            console.print(f"Processing: [bold]{fname}[/bold]")

            # Read the CSV via storage — try with quoting first, fall back to
            # disabled quoting for files that have literal " in values (e.g. Dec tables)
            try:
                df = storage.read_dataframe(
                    filepath, format="csv",
                    encoding=encoding,
                    quote_char=quote_char if quote_char else None,
                    infer_schema_length=infer_schema_length,
                    truncate_ragged_lines=True,
                )
            except Exception:
                df = storage.read_dataframe(
                    filepath, format="csv",
                    encoding=encoding,
                    quote_char=None,
                    infer_schema_length=infer_schema_length,
                    truncate_ragged_lines=True,
                )

            # Get original column names
            original_columns = df.columns

            # Clean column names using project-standard normalization
            df_cleaned = normalize_polars_columns(df)

            # --- Clean all string columns for latin-1 compatibility ---
            try:
                from eencijferho.core.decoder import clean_for_latin1
                df_cleaned = clean_for_latin1(df_cleaned)
            except Exception as e:
                console.print(f"  [yellow]Warning: Could not apply clean_for_latin1: {e}[/yellow]")

            # Get new column names
            new_columns = df_cleaned.columns

            # Check if any changes were made
            changes = [(old, new) for old, new in zip(original_columns, new_columns) if old != new]

            if not changes:
                console.print("  [dim]No changes needed - headers already in snake_case[/dim]\n")
                continue

            # Show changes
            console.print("  [green]Changes:[/green]")
            for old, new in changes:
                console.print(f"    {old} → {new}")

            # Write back to the same file
            csv_string = df_cleaned.write_csv(separator=delimiter)
            storage.write_text(csv_string, filepath)

            console.print("  [green]✓ Updated successfully[/green]\n")

        except Exception as e:
            console.print(f"  [red]✗ Error processing {fname}: {e}[/red]\n")
            failed_files.append((fname, str(e)))

    # Summary
    console.print("[bold green]Conversion complete![/bold green]")

    if failed_files:
        console.print(f"\n[bold red]Failed files ({len(failed_files)}):[/bold red]")
        for filename, error in failed_files:
            console.print(f"  [red]• {filename}[/red]")
            console.print(f"    [dim]{error}[/dim]")


if __name__ == "__main__":
    # Example usage - uses dynamic config defaults
    convert_csv_headers_to_snake_case()

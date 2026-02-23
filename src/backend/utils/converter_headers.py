import polars as pl
import re
import unicodedata
from pathlib import Path
import polars as pl
from rich.console import Console

def normalize_name(name, naming_func=None):
    """
    Normalize variable names using the provided naming convention function (e.g., snake_case).
    If no function is provided, defaults to snake_case.
    """
    if naming_func:
        return naming_func(name)
    # Default: snake_case, remove special chars
    name = name.lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    return name

def normalize_polars_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rename all columns of a Polars DataFrame using normalize_name and strip_accents.
    """
    return df.rename({col: strip_accents(normalize_name(col)) for col in df.columns})

def clean_header_name(name):
    # Normalize to NFKD, remove diacritics, replace problematic chars
    name = unicodedata.normalize('NFKD', str(name))
    name = ''.join(c for c in name if not unicodedata.combining(c))
    name = name.replace('\u2044', '/')  # Replace fraction slash if present
    name = name.replace('�', 'e')  # Replace common corruption with 'e'
    name = name.replace('3a3', 'a')  # Fix specific corruption pattern
    name = name.encode('utf-8', errors='replace').decode('utf-8')
    name = name.strip()
    return name

def strip_accents(text: str) -> str:
    """Remove accents from text (e.g., 'vóór' -> 'voor')."""
    normalized = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in normalized if not unicodedata.combining(c))

console = Console()


def convert_csv_headers_to_snake_case(
    input_dir: str = "data/02-output",
    delimiter: str = ";",
    encoding: str = "latin-1",
    quote_char: str = "",
    infer_schema_length: int | None = None
) -> None:
    """
    Convert all CSV file headers in the input directory to snake_case.
    
    Args:
        input_dir: Path to directory containing CSV files
        delimiter: CSV delimiter (default: ";")
        encoding: File encoding (default: "latin-1")
        quote_char: Quote character to use. Use "" to disable quoting (default: "")
        infer_schema_length: Number of rows to scan for schema inference (default: None - scan all)
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        console.print(f"[red]Error: Directory '{input_dir}' does not exist![/red]")
        return
    
    if not input_path.is_dir():
        console.print(f"[red]Error: '{input_dir}' is not a directory![/red]")
        return
    
    # Find all CSV files
    csv_files = list(input_path.glob("*.csv"))
    
    if not csv_files:
        console.print(f"[yellow]No CSV files found in '{input_dir}'[/yellow]")
        return
    
    console.print(f"[cyan]Found {len(csv_files)} CSV file(s) in '{input_dir}'[/cyan]\n")
    
    failed_files = []
    
    for csv_file in csv_files:
        try:
            console.print(f"Processing: [bold]{csv_file.name}[/bold]")
            
            # Read the CSV
            df = pl.read_csv(
                csv_file,
                separator=delimiter,
                encoding=encoding,
                quote_char=quote_char,
                infer_schema_length=infer_schema_length,
                truncate_ragged_lines=True
            )
            
            # Get original column names
            original_columns = df.columns
            

            # Clean column names using project-standard normalization
            df_cleaned = normalize_polars_columns(df)

            # --- Clean all string columns for latin-1 compatibility ---
            try:
                from backend.core.decoder import clean_for_latin1
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
            
            # Write back to the same file with the same encoding
            csv_string = df_cleaned.write_csv(separator=delimiter)
            
            # Write with the specified encoding
            with open(csv_file, 'w', encoding=encoding) as f:
                f.write(csv_string)
            
            console.print("  [green]✓ Updated successfully[/green]\n")
            
        except Exception as e:
            console.print(f"  [red]✗ Error processing {csv_file.name}: {e}[/red]\n")
            failed_files.append((csv_file.name, str(e)))
    
    # Summary
    console.print("[bold green]Conversion complete![/bold green]")
    
    if failed_files:
        console.print(f"\n[bold red]Failed files ({len(failed_files)}):[/bold red]")
        for filename, error in failed_files:
            console.print(f"  [red]• {filename}[/red]")
            console.print(f"    [dim]{error}[/dim]")


if __name__ == "__main__":
    # Example usage - now uses defaults that work for your files
    convert_csv_headers_to_snake_case("data/02-output")
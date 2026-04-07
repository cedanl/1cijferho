import polars as pl
from rich.console import Console
import hashlib
import os
from typing import Any
from eencijferho.config import get_output_dir
from eencijferho.io.decorators import with_storage


@with_storage
def encryptor(storage, input_dir: str | None = None, output_dir: str | None = None) -> None:
    # Use dynamic config defaults if not provided
    if input_dir is None:
        input_dir = get_output_dir()
    if output_dir is None:
        output_dir = get_output_dir()
    console = Console()

    # Columns to encrypt
    columns_to_encrypt = ["Persoonsgebonden nummer", "Burgerservicenummer", "Onderwijsnummer"]

    # Specifically target only files starting with EV or VAKHAVW
    target_files = []
    for pattern in ["EV*.csv", "VAKHAVW*.csv"]:
        target_files.extend(storage.list_files(f"{input_dir}/{pattern}"))

    console.print(f"[bold blue]Found {len(target_files)} target files for encryption[/]")

    from eencijferho.utils.converter_headers import clean_header_name
    for filepath in target_files:
        fname = filepath.rsplit("/", 1)[-1] if "/" in filepath else filepath
        try:
            # Always read as UTF-8 (mirroring decoded output)
            df = storage.read_dataframe(filepath, format="csv")
            # Check if any columns to encrypt exist in this file
            columns_found = [col for col in columns_to_encrypt if col in df.columns]
            if columns_found:
                console.print(f"[cyan]⚙[/] Processing {fname}, found columns: {columns_found}")
                # Define SHA256 function for Polars
                def sha256_hash(x: Any) -> str | None:
                    if x is None:
                        return None
                    return hashlib.sha256(str(x).encode()).hexdigest()
                # Encrypt each found column
                for col in columns_found:
                    df = df.with_columns(
                        pl.col(col).map_elements(sha256_hash, return_dtype=pl.Utf8).alias(col)
                    )
                # Clean headers for output (remove diacritics, fix encoding issues)
                df.columns = [clean_header_name(col) for col in df.columns]
                # Create output filename with _encrypted suffix
                stem = os.path.splitext(fname)[0]
                output_file = os.path.join(output_dir, f"{stem}_encrypted.csv")
                # Always write as UTF-8
                storage.write_text(df.write_csv(separator=';'), output_file)
                console.print(f"[green]✓[/] Encrypted {len(columns_found)} columns in {fname}")
            else:
                console.print(f"[blue]ℹ[/] No columns to encrypt in {fname}")
        except Exception as e:
            console.print(f"[bold red]✗[/] Error processing {fname}: {str(e)}")

    console.print("[bold green]Encryption completed![/]")


if __name__ == "__main__":
    encryptor()

import polars as pl
from pathlib import Path
from rich.console import Console
import hashlib

def encryptor(input_dir="data/02-output", output_dir="data/02-output"):
    console = Console()
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Columns to encrypt
    columns_to_encrypt = ["Persoonsgebonden nummer", "Burgerservicenummer", "Onderwijsnummer"]
    
    # Specifically target only files starting with EV or VAKHAVW
    target_files = []
    for pattern in ["EV*.csv", "VAKHAVW*.csv"]:
        target_files.extend(list(input_path.glob(pattern)))
    
    console.print(f"[bold blue]Found {len(target_files)} target files for encryption[/]")
    
    for csv_file in target_files:
        try:
            # Load CSV file
            df = pl.read_csv(csv_file, separator=";", encoding="latin-1")
            
            # Check if any columns to encrypt exist in this file
            columns_found = [col for col in columns_to_encrypt if col in df.columns]
            
            if columns_found:
                console.print(f"[cyan]⚙[/] Processing {csv_file.name}, found columns: {columns_found}")
                
                # Define SHA256 function for Polars
                def sha256_hash(x):
                    if x is None:
                        return None
                    return hashlib.sha256(str(x).encode()).hexdigest()
                
                # Encrypt each found column
                for col in columns_found:
                    df = df.with_columns(
                        pl.col(col).map_elements(sha256_hash, return_dtype=pl.Utf8).alias(col)
                    )
                
                # Create output filename with _encrypted suffix
                output_file = output_path / f"{csv_file.stem}_encrypted.csv"
                # Clean for latin-1 compatibility before writing
                from backend.core.decoder import clean_for_latin1
                df = clean_for_latin1(df)
                df.write_csv(output_file, separator=";")
                console.print(f"[green]✓[/] Encrypted {len(columns_found)} columns in {csv_file.name}")
            else:
                console.print(f"[blue]ℹ[/] No columns to encrypt in {csv_file.name}")
                
        except Exception as e:
            console.print(f"[bold red]✗[/] Error processing {csv_file.name}: {str(e)}")
    
    console.print("[bold green]Encryption completed![/]")

if __name__ == "__main__":
    encryptor()
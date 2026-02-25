import polars as pl
from pathlib import Path
from rich.console import Console
from rich.progress import track

def convert_csv_to_parquet(input_dir: str = "data/02-output") -> None:
    console = Console()
    input_path = Path(input_dir)
    csv_files = list(input_path.glob("*.csv"))
    
    console.print(f"[bold green]Converting CSV files in {input_dir}[/]")
    
    for csv_file in track(csv_files, description="Converting files"):
        # Skip files with "dec" in their name (case-insensitive)
        if "dec" in csv_file.name.lower():
            console.print(f"[yellow]↷[/] Skipping {csv_file.name}")
            continue
            
        parquet_file = csv_file.with_suffix(".parquet")
        try:
            df = pl.read_csv(csv_file, separator=";", encoding="latin-1")
            df.write_parquet(parquet_file)
            console.print(f"[green]✓[/] {csv_file.name}")
        except Exception as e:
            console.print(f"[bold red]✗[/] {csv_file.name}: {str(e)}")
    
    console.print("[bold green]Conversion completed![/]")
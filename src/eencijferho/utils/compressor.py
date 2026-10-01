import os
from rich.console import Console
from rich.progress import track

from eencijferho.io.decorators import with_storage


@with_storage
def convert_csv_to_parquet(
    storage, input_dir: str | None = None, *,
    filenames: set[str] | None = None, strict: bool = False,
) -> None:
    if input_dir is None:
        from eencijferho.config import get_output_dir
        input_dir = get_output_dir()
    console = Console()
    csv_files = storage.list_files(f"{input_dir}/*.csv")

    console.print(f"[bold green]Converting CSV files in {input_dir}[/]")

    for csv_file in track(csv_files, description="Converting files"):
        # Skip Dec lookup table files (Dec_*.csv) — not output data files
        filename = os.path.basename(csv_file)
        if filenames is not None and filename not in filenames:
            continue
        if filename.lower().startswith("dec_"):
            console.print(f"[yellow]↷[/] Skipping {filename}")
            continue

        parquet_file = csv_file.rsplit(".", 1)[0] + ".parquet"
        try:
            # infer_schema_length=0: every column stays a String, so identifiers
            # keep their fixed width and their leading zeros. DUO writes a BSN as
            # 9 digits; read as an int, 000186662 and 186662 are the same number.
            df = storage.read_dataframe(csv_file, infer_schema_length=0)
            storage.write_dataframe(df, parquet_file, format="parquet")
            console.print(f"[green]✓[/] {filename}")
        except Exception as e:
            if strict:
                raise ValueError(f"Parquet-conversie mislukt voor {filename}: {e}") from e
            console.print(f"[bold red]✗[/] {filename}: {str(e)}")

    console.print("[bold green]Conversion completed![/]")

from rich.console import Console
from rich.progress import track

from eencijferho.io.decorators import with_storage


@with_storage
def convert_csv_to_parquet(storage, input_dir: str | None = None) -> None:
    if input_dir is None:
        from eencijferho.config import get_output_dir
        input_dir = get_output_dir()
    console = Console()
    csv_files = storage.list_files(f"{input_dir}/*.csv")

    console.print(f"[bold green]Converting CSV files in {input_dir}[/]")

    for csv_file in track(csv_files, description="Converting files"):
        # Skip files with "dec" in their name (case-insensitive)
        filename = csv_file.rsplit("/", 1)[-1] if "/" in csv_file else csv_file
        if "dec" in filename.lower():
            console.print(f"[yellow]↷[/] Skipping {filename}")
            continue

        parquet_file = csv_file.rsplit(".", 1)[0] + ".parquet"
        try:
            df = storage.read_dataframe(csv_file)
            storage.write_dataframe(df, parquet_file, format="parquet")
            console.print(f"[green]✓[/] {filename}")
        except Exception as e:
            console.print(f"[bold red]✗[/] {filename}: {str(e)}")

    console.print("[bold green]Conversion completed![/]")

import os

import polars as pl
from rich.console import Console
from rich.progress import track

from eencijferho.config import ENRICHED_SUFFIX
from eencijferho.core.decoder import CODE_COLUMN_SUFFIX, CODE_COLUMN_VARIABLES
from eencijferho.io.decorators import with_storage
from eencijferho.utils.converter_headers import normalize_name

# Integer columns the decoder adds to _enriched next to a label column (#233).
# Every other column stays a String (see below).
INTEGER_CODE_COLUMNS = frozenset(
    f"{normalize_name(v)}{CODE_COLUMN_SUFFIX}" for v in CODE_COLUMN_VARIABLES
)


def _cast_integer_code_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Give the decoder's integer code columns their integer type back."""
    return df.with_columns(
        pl.col(c).cast(pl.Int64, strict=False)
        for c in df.columns
        if c in INTEGER_CODE_COLUMNS
    )


@with_storage
def convert_csv_to_parquet(storage, input_dir: str | None = None) -> None:
    if input_dir is None:
        from eencijferho.config import get_output_dir

        input_dir = get_output_dir()
    console = Console()
    csv_files = storage.list_files(f"{input_dir}/*.csv")

    console.print(f"[bold green]Converting CSV files in {input_dir}[/]")

    for csv_file in track(csv_files, description="Converting files"):
        # Skip Dec lookup table files (Dec_*.csv) — not output data files
        filename = os.path.basename(csv_file)
        if filename.lower().startswith("dec_"):
            console.print(f"[yellow]↷[/] Skipping {filename}")
            continue

        parquet_file = csv_file.rsplit(".", 1)[0] + ".parquet"
        try:
            # infer_schema_length=0: every column stays a String, so identifiers
            # keep their fixed width and their leading zeros. DUO writes a BSN as
            # 9 digits; read as an int, 000000012 and 12 are the same number.
            df = storage.read_dataframe(csv_file, infer_schema_length=0)
            if csv_file.endswith(ENRICHED_SUFFIX):
                df = _cast_integer_code_columns(df)
            storage.write_dataframe(df, parquet_file, format="parquet")
            console.print(f"[green]✓[/] {filename}")
        except Exception as e:
            console.print(f"[bold red]✗[/] {filename}: {e!s}")

    console.print("[bold green]Conversion completed![/]")

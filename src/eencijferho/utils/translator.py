import os

import polars as pl
from rich.console import Console

from eencijferho.io.decorators import with_storage

_PGN_COL_IN_DATA = "Persoonsgebonden nummer"


@with_storage
def translate_pgn_to_local_id(
    storage,
    output_dir: str,
    mapping_file: str,
    mapping_pgn_col: str = "persoonsgebonden_nummer",
    mapping_id_col: str = "studentnummer",
) -> str:
    """Voeg een lokaal studentnummer toe aan elk EV/VAKHAVW-bestand via een koppelbestand.

    Het koppelbestand (CSV of Parquet) moet minstens twee kolommen bevatten:
    één voor het persoonsgebonden nummer en één voor het lokale ID.
    De join is een left-join; het aantal rijen mag niet wijzigen (cardinaliteitscheck).
    """
    console = Console()
    log = ""

    ext = os.path.splitext(mapping_file)[1].lower()
    if ext == ".parquet":
        mapping_df = pl.read_parquet(mapping_file)
    elif ext in (".csv", ".tsv", ".txt"):
        # Try semicolon first (project convention), fall back to comma
        raw = pl.read_csv(mapping_file, separator=";", infer_schema_length=0)
        if len(raw.columns) == 1:
            raw = pl.read_csv(mapping_file, separator=",", infer_schema_length=0)
        mapping_df = raw
    else:
        raise ValueError(
            f"Onbekend koppelbestand-formaat '{ext}'. Gebruik .csv of .parquet."
        )

    missing = [c for c in (mapping_pgn_col, mapping_id_col) if c not in mapping_df.columns]
    if missing:
        raise ValueError(
            f"Kolom(men) {missing} niet gevonden in koppelbestand '{mapping_file}'. "
            f"Beschikbare kolommen: {mapping_df.columns}"
        )

    lookup = mapping_df.select([mapping_pgn_col, mapping_id_col])

    target_files: list[str] = []
    for pattern in ["EV*.csv", "VAKHAVW*.csv"]:
        target_files.extend(storage.list_files(f"{output_dir}/{pattern}"))

    count = 0
    for filepath in target_files:
        fname = os.path.basename(filepath)
        try:
            df = storage.read_dataframe(filepath, format="csv", infer_schema_length=0)
            if _PGN_COL_IN_DATA not in df.columns:
                log += f"[translator] {fname}: geen '{_PGN_COL_IN_DATA}' kolom, overgeslagen.\n"
                continue

            n_before = len(df)
            joined = df.join(
                lookup,
                left_on=_PGN_COL_IN_DATA,
                right_on=mapping_pgn_col,
                how="left",
            )
            if len(joined) != n_before:
                raise ValueError(
                    f"Koppeling heeft het aantal rijen gewijzigd ({n_before} → {len(joined)}). "
                    "Controleer het koppelbestand op dubbele persoonsgebonden nummers."
                )

            storage.write_text(joined.write_csv(separator=";"), filepath)
            count += 1
            console.print(f"[green]✓[/] {fname}: '{mapping_id_col}' toegevoegd")
            log += f"[translator] {fname}: '{mapping_id_col}' kolom toegevoegd.\n"
        except Exception as exc:
            console.print(f"[red]✗[/] {fname}: {exc}")
            log += f"[translator] {fname}: fout — {exc}\n"

    log += f"[translator] {count} bestand(en) bijgewerkt met lokaal ID.\n"
    return log

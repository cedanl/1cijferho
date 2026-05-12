import os

import polars as pl
from rich.console import Console

from eencijferho.config import DUO_BSN_COLUMN
from eencijferho.io.decorators import with_storage


def _load_mapping_df(
    mapping_file: str,
    mapping_bsn_col: str,
    mapping_id_col: str,
) -> pl.DataFrame:
    """Read and validate the local mapping file. Returns a two-column lookup DataFrame."""
    ext = os.path.splitext(mapping_file)[1].lower()
    if ext == ".parquet":
        raw = pl.read_parquet(mapping_file)
    elif ext in (".csv", ".tsv", ".txt"):
        # Semicolon is the project convention; fall back to comma for files from
        # other tools. A single-column result signals the wrong separator.
        raw = pl.read_csv(mapping_file, separator=";", infer_schema_length=0)
        if len(raw.columns) == 1:
            raw = pl.read_csv(mapping_file, separator=",", infer_schema_length=0)
    else:
        raise ValueError(
            f"Onbekend koppelbestand-formaat '{ext}'. Gebruik .csv of .parquet."
        )

    missing = [c for c in (mapping_bsn_col, mapping_id_col) if c not in raw.columns]
    if missing:
        raise ValueError(
            f"Kolom(men) {missing} niet gevonden in koppelbestand '{mapping_file}'. "
            f"Beschikbare kolommen: {raw.columns}"
        )

    return raw.select([mapping_bsn_col, mapping_id_col])


@with_storage
def translate_bsn_to_local_id(
    storage,
    output_dir: str,
    mapping_file: str,
    mapping_bsn_col: str = "burgerservicenummer",
    mapping_id_col: str = "studentnummer",
) -> str:
    """Voeg een lokaal studentnummer toe aan elk EV/VAKHAVW-bestand via een koppelbestand.

    Het koppelbestand bevat een vertaling van burgerservicenummer → lokaal studentnummer.
    Ondersteunde formaten: ``.parquet`` en ``.csv`` (puntkomma- of kommagescheiden,
    automatisch gedetecteerd).

    Het koppelbestand moet altijd een **lokaal pad** zijn — het wordt direct
    ingelezen met Polars, niet via de actieve storage-backend. De output-bestanden
    in ``output_dir`` worden wél via de storage-backend gelezen en geschreven.

    De functie werkt in twee fasen: eerst worden alle joins gevalideerd, dan
    pas geschreven. Als één bestand faalt, worden **geen** bestanden gewijzigd.
    Dit voorkomt inconsistente output bij fouten halverwege.
    """
    console = Console()
    log = ""

    lookup = _load_mapping_df(mapping_file, mapping_bsn_col, mapping_id_col)

    target_files: list[str] = []
    for pattern in ["EV*.csv", "VAKHAVW*.csv"]:
        target_files.extend(storage.list_files(f"{output_dir}/{pattern}"))

    # Phase 1: perform all joins and collect results — nothing is written yet.
    # If any join fails the entire step is aborted so output stays consistent.
    pending: list[tuple[str, str, pl.DataFrame]] = []
    errors: list[str] = []

    for filepath in target_files:
        fname = os.path.basename(filepath)
        df = storage.read_dataframe(filepath, format="csv", infer_schema_length=0)
        if DUO_BSN_COLUMN not in df.columns:
            log += f"[translator] {fname}: geen '{DUO_BSN_COLUMN}' kolom, overgeslagen.\n"
            continue

        n_before = len(df)
        joined = df.join(
            lookup,
            left_on=DUO_BSN_COLUMN,
            right_on=mapping_bsn_col,
            how="left",
        )
        if len(joined) != n_before:
            errors.append(
                f"{fname}: koppeling heeft het aantal rijen gewijzigd "
                f"({n_before} → {len(joined)}). "
                "Controleer het koppelbestand op dubbele burgerservicenummers."
            )
        else:
            pending.append((filepath, fname, joined))

    if errors:
        raise ValueError(
            f"Koppeling mislukt voor {len(errors)} bestand(en) — geen bestanden gewijzigd:\n"
            + "\n".join(f"  • {e}" for e in errors)
        )

    # Phase 2: all joins validated — write everything.
    for filepath, fname, joined_df in pending:
        storage.write_text(joined_df.write_csv(separator=";"), filepath)
        console.print(f"[green]✓[/] {fname}: '{mapping_id_col}' toegevoegd")
        log += f"[translator] {fname}: '{mapping_id_col}' kolom toegevoegd.\n"

    log += f"[translator] {len(pending)} bestand(en) bijgewerkt met lokaal ID.\n"
    return log

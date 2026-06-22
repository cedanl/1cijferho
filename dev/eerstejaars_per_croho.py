"""
Aggregeer eerstejaars en hoofdinschrijvingen per CROHO-opleiding uit 1CijferHO output.

Input:  geconverteerde CSV/Parquet zonder decodering of verrijking (pipeline output)
Output: CSV met per collegejaar / isatcode / examentype / herkomstgroep de aantallen

Gebruik:
    uv run python dev/eerstejaars_per_croho.py
    uv run python dev/eerstejaars_per_croho.py --input data/02-output/DEMO --output data/02-output/DEMO/eerstejaars_croho.csv
"""

import argparse
import sys
from pathlib import Path

import re
import unicodedata

import polars as pl


# Nederlandse nationaliteitscode in DUO-data
NL_NATIONALITEITSCODE = "0001"


def _to_snake(name: str) -> str:
    """Zelfde logica als normalize_name in converter_headers.py."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def find_ev_file(input_dir: Path) -> Path:
    """Zoek het eerste EV*.csv of EV*.parquet bestand in input_dir."""
    for pattern in ("EV*.csv", "EV*.parquet"):
        matches = sorted(input_dir.glob(pattern))
        if matches:
            return matches[0]
    raise FileNotFoundError(f"Geen EV*.csv of EV*.parquet gevonden in {input_dir}")


def find_dec_opleidingscode(input_dir: Path) -> Path | None:
    """Zoek Dec_opleidingscode.csv of .parquet in input_dir."""
    for pattern in ("Dec_opleidingscode.parquet", "Dec_opleidingscode.csv"):
        matches = list(input_dir.glob(pattern))
        if matches:
            return matches[0]
    return None


def read_file(path: Path) -> pl.DataFrame:
    if path.suffix == ".parquet":
        df = pl.read_parquet(path)
    else:
        df = pl.read_csv(path, separator=";", infer_schema_length=0)
    # normaliseer naar snake_case zodat CSV en Parquet beide werken
    return df.rename({col: _to_snake(col) for col in df.columns})


def derive_eer_nl_niet_eer(df: pl.DataFrame) -> pl.DataFrame:
    """
    Maak kolom eer_nl_niet_eer op basis van nationaliteit_1 en indicatie_eer_op_peildatum_1_oktober.

    NL           = nationaliteit_1 == "0001"
    EER (niet NL) = indicatie_eer == "J" AND niet NL
    niet-EER     = indicatie_eer == "N"
    """
    return df.with_columns(
        pl.when(pl.col("nationaliteit_1") == NL_NATIONALITEITSCODE)
        .then(pl.lit("NL"))
        .when(pl.col("indicatie_eer_op_peildatum_1_oktober") == "J")
        .then(pl.lit("EER"))
        .otherwise(pl.lit("niet-EER"))
        .alias("eer_nl_niet_eer")
    )


def aggregate(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by(
            [
                "collegejaar",
                "isatcode",
                "groepeernaam_croho",
                "examentype_code",
                "eer_nl_niet_eer",
            ]
        )
        .agg(
            (pl.col("indicatie_eerstejaars_opl_actueel_equivalent") == "1")
            .sum()
            .alias("aantal_eerstejaars_croho"),
            pl.len().alias("aantal_hoofdinschrijvingen"),
        )
        .sort(["collegejaar", "isatcode", "examentype_code", "eer_nl_niet_eer"])
    )


def build(input_dir: Path, output_path: Path) -> pl.DataFrame:
    ev_path = find_ev_file(input_dir)
    print(f"[eerstejaars] Lees: {ev_path.name}")
    df = read_file(ev_path)

    # --- filters ---
    df = (
        df
        # alleen actief op peildatum 1 oktober
        .filter(pl.col("indicatie_actief_op_peildatum") == "1")
        # alleen hoofdinschrijvingen (incl. uitwisselingsstudenten + aangewezen)
        .filter(pl.col("soort_inschrijving_hoger_onderwijs").is_in(["1", "6", "A"]))
        # alleen bachelor en master
        .filter(pl.col("opleidingsfase_actueel").is_in(["B", "M"]))
    )

    # --- herkomstgroep afleiden ---
    df = derive_eer_nl_niet_eer(df)

    # --- renames voor tussenliggende bewerkingen ---
    df = df.rename({
        "inschrijvingsjaar": "collegejaar",
        "opleiding_actueel_equivalent": "isatcode",
        "opleidingsfase_actueel": "examentype_code",
    })

    # --- join opleidingsnaam uit Dec_opleidingscode ---
    dec_path = find_dec_opleidingscode(input_dir)
    if dec_path:
        print(f"[eerstejaars] Join opleidingsnaam uit: {dec_path.name}")
        dec = read_file(dec_path).select(
            pl.col("opleidingscode").alias("isatcode"),
            pl.col("naam_opleiding").alias("groepeernaam_croho"),
        )
        df = df.join(dec, on="isatcode", how="left")
    else:
        print("[eerstejaars] Waarschuwing: Dec_opleidingscode niet gevonden — groepeernaam_croho leeg")
        df = df.with_columns(pl.lit(None).cast(pl.String).alias("groepeernaam_croho"))

    # --- aggregatie ---
    result = aggregate(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.write_csv(output_path, separator=";")
    print(f"[eerstejaars] Geschreven: {output_path}  ({len(result)} rijen)")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Eerstejaars per CROHO aggregatie")
    parser.add_argument(
        "--input",
        default="data/02-output/DEMO",
        help="Map met pipeline output CSV/Parquet bestanden",
    )
    parser.add_argument(
        "--output",
        default="data/02-output/DEMO/eerstejaars_croho.csv",
        help="Pad voor output CSV",
    )
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_path = Path(args.output)

    if not input_dir.exists():
        print(f"[eerstejaars] Fout: inputmap bestaat niet: {input_dir}", file=sys.stderr)
        sys.exit(1)

    result = build(input_dir, output_path)
    print(result.head(10))


if __name__ == "__main__":
    main()

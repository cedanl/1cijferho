"""The preset for Staat van de Onderwijsinstelling (staat1cho) must convert everything.

staat1cho reads the enriched EV file with snake_case column names and uses many
more columns than the NFWA and Evaluatietool presets decode. With those presets
it stops on a missing column, so its users had to set every option by hand.
"""

import argparse
import json
from unittest.mock import patch

import polars as pl

from eencijferho.cli import _build_output_config
from eencijferho.core.pipeline import run_turbo_convert_pipeline


def test_svo_preset_converts_both_file_types_and_every_column():
    args = argparse.Namespace(preset="svo", bsn_mapping_file=None)

    cfg = _build_output_config(args)

    assert cfg.convert_ev is True
    assert cfg.convert_vakhavw is True
    assert cfg.variants == ["decoded", "enriched"]
    assert cfg.formats == ["parquet"]
    assert cfg.column_casing == "snake_case"
    # None means: no column filter, every available column.
    assert cfg.decode_columns is None
    assert cfg.enrich_variables is None


def test_svo_preset_writes_diplomajaar_code_as_integer_next_to_the_label(
    tmp_path, make_csv
):
    """#233: SVO computes with the diploma year; 0000 must stay the number 0."""
    out = tmp_path / "out"
    out.mkdir()
    json_dir = tmp_path / "metadata" / "json"
    json_dir.mkdir(parents=True)
    (json_dir / "Bestandsbeschrijving_Dec-bestanden.json").write_text(
        json.dumps({"tables": []}), encoding="utf-8"
    )
    (json_dir / "variable_metadata.json").write_text(
        json.dumps(
            [{"name": "Diplomajaar", "values": {"0000": "geen examen geregistreerd"}}]
        ),
        encoding="utf-8",
    )
    # Synthetic values only.
    make_csv(
        out / "EV2023.csv",
        [
            {
                "Burgerservicenummer": "000000012",
                "Diplomajaar": "0000",
                "Inschrijvingsjaar": "2020",
            },
            {
                "Burgerservicenummer": "000000048",
                "Diplomajaar": "2023",
                "Inschrijvingsjaar": "2021",
            },
            {
                "Burgerservicenummer": "000000051",
                "Diplomajaar": "",
                "Inschrijvingsjaar": "2022",
            },
        ],
    )
    cfg = _build_output_config(argparse.Namespace(preset="svo", bsn_mapping_file=None))

    with (
        patch("eencijferho.core.pipeline.converter"),
        patch("eencijferho.core.pipeline.cv"),
        patch("eencijferho.core.pipeline.tr"),
    ):
        run_turbo_convert_pipeline(
            input_dir=str(tmp_path / "in"),
            output_dir=str(out),
            metadata_dir=str(tmp_path / "metadata"),
            output_config=cfg,
        )

    enriched = pl.read_csv(out / "EV2023_enriched.csv", separator=";")
    assert enriched.columns == [
        "burgerservicenummer",
        "diplomajaar",
        "diplomajaar_code",
        "inschrijvingsjaar",
    ]
    assert enriched["diplomajaar"].to_list() == [
        "geen examen geregistreerd",
        "2023",
        None,
    ]
    assert enriched["diplomajaar_code"].dtype in (pl.Int64, pl.Int32)
    assert enriched["diplomajaar_code"].to_list() == [0, 2023, None]

    parquet = pl.read_parquet(out / "EV2023_enriched.parquet")
    assert parquet.columns == enriched.columns
    assert parquet["diplomajaar_code"].dtype == pl.Int64
    assert parquet["diplomajaar_code"].to_list() == [0, 2023, None]
    assert parquet["burgerservicenummer"].to_list() == [
        "000000012",
        "000000048",
        "000000051",
    ]

    decoded = pl.read_csv(
        out / "EV2023_decoded.csv", separator=";", infer_schema_length=0
    )
    assert "diplomajaar_code" not in decoded.columns
    assert decoded["diplomajaar"].to_list() == ["0000", "2023", None]

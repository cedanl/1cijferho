"""The preset for Staat van de Onderwijsinstelling (staat1cho) must convert everything.

staat1cho reads the enriched EV file with snake_case column names and uses many
more columns than the NFWA and Evaluatietool presets decode. With those presets
it stops on a missing column, so its users had to set every option by hand.
"""

import argparse

from eencijferho.cli import _build_output_config


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

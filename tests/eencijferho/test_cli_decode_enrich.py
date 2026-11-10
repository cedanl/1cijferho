"""The standalone ``decode`` and ``enrich`` commands keep identifiers as strings (#200).

They read pipeline CSV output and write new CSV files from it. With type
inference a zero-padded BSN such as ``000000012`` would come back as ``12``.
The decoder itself is stubbed: these tests only check how the CLI reads and
writes the files.
"""

import argparse
from unittest.mock import patch

import polars as pl
import pytest

from eencijferho.cli import cmd_decode, cmd_enrich

# Synthetic values only.
ROWS = [
    {"Burgerservicenummer": "000000012", "Onderwijsnummer": "000000036", "Geslacht": "1"},
    {"Burgerservicenummer": "000000048", "Onderwijsnummer": "000000063", "Geslacht": "2"},
]


@pytest.fixture
def output_dir(tmp_path, monkeypatch, make_csv):
    """A relative output dir (the CLI rejects paths outside the cwd) with one EV CSV."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("STORAGE_BACKEND", "disk")
    monkeypatch.setenv("STORAGE_DISK_BASE_PATH", ".")
    (tmp_path / "in").mkdir()
    json_dir = tmp_path / "out" / "metadata" / "json"
    json_dir.mkdir(parents=True)
    (json_dir / "Bestandsbeschrijving_Dec-bestanden_test.json").write_text("{}", encoding="utf-8")
    make_csv(tmp_path / "out" / "EV2023.csv", ROWS)
    return tmp_path / "out"


def _args():
    return argparse.Namespace(input="in", output="out")


def _read(path):
    return pl.read_csv(path, separator=";", infer_schema_length=0)


def test_decode_reads_every_column_as_string(output_dir):
    seen = {}

    def fake_decode(df, *args, **kwargs):
        seen["dtypes"] = set(df.dtypes)
        return df

    with (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}),
        patch("eencijferho.core.decoder.decode_fields_dec_only", side_effect=fake_decode),
    ):
        cmd_decode(_args())

    assert seen["dtypes"] == {pl.String}
    decoded = _read(output_dir / "EV2023_decoded.csv")
    assert decoded["Burgerservicenummer"].to_list() == ["000000012", "000000048"]
    assert decoded["Onderwijsnummer"].to_list() == ["000000036", "000000063"]


def test_enrich_reads_every_column_as_string(output_dir, make_csv):
    make_csv(output_dir / "EV2023_decoded.csv", ROWS)
    seen = {}

    def fake_enrich(df, *args, **kwargs):
        seen["dtypes"] = set(df.dtypes)
        return df.with_columns(pl.col("Geslacht").replace({"1": "man", "2": "vrouw"}))

    with (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}),
        patch(
            "eencijferho.core.decoder.load_variable_mappings",
            return_value={"geslacht": {"orig_name": "Geslacht", "mapping": {}}},
        ),
        patch("eencijferho.core.decoder.decode_fields", side_effect=fake_enrich),
    ):
        cmd_enrich(_args())

    assert seen["dtypes"] == {pl.String}
    enriched = _read(output_dir / "EV2023_enriched.csv")
    assert enriched["Burgerservicenummer"].to_list() == ["000000012", "000000048"]
    assert enriched["Onderwijsnummer"].to_list() == ["000000036", "000000063"]
    assert enriched["Geslacht"].to_list() == ["man", "vrouw"]


def test_enrich_skips_when_identical_to_decoded(output_dir, make_csv, capsys):
    """With both files read as strings, an unchanged result is recognised as identical."""
    make_csv(output_dir / "EV2023_decoded.csv", ROWS)

    with (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}),
        patch(
            "eencijferho.core.decoder.load_variable_mappings",
            return_value={"geslacht": {"orig_name": "Geslacht", "mapping": {}}},
        ),
        patch("eencijferho.core.decoder.decode_fields", side_effect=lambda df, *a, **k: df),
    ):
        cmd_enrich(_args())

    assert not (output_dir / "EV2023_enriched.csv").exists()
    assert "identiek aan decoded" in capsys.readouterr().out


def _vakken_json(output_dir):
    """Write an (empty) Vakkenbestanden JSON next to the Dec JSON."""
    path = output_dir / "metadata" / "json" / "Bestandsbeschrijving_Vakkenbestanden_test.json"
    path.write_text("{}", encoding="utf-8")
    return path.name


def test_decode_passes_the_vakken_json_on(output_dir):
    """cmd_decode finds the Vakkenbestanden JSON and hands it to the decoder (#204)."""
    name = _vakken_json(output_dir)

    with (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}) as load,
        patch(
            "eencijferho.core.decoder.decode_fields_dec_only", side_effect=lambda df, *a, **k: df,
        ) as decode,
    ):
        cmd_decode(_args())

    assert load.call_args.kwargs["vakken_metadata_json"].endswith(name)
    assert decode.call_args.kwargs["vakken_metadata_json"].endswith(name)


def test_enrich_passes_the_vakken_json_on(output_dir, make_csv):
    """cmd_enrich finds the Vakkenbestanden JSON and hands it to the decoder (#204)."""
    name = _vakken_json(output_dir)
    make_csv(output_dir / "EV2023_decoded.csv", ROWS)

    with (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}) as load,
        patch(
            "eencijferho.core.decoder.load_variable_mappings",
            return_value={"geslacht": {"orig_name": "Geslacht", "mapping": {}}},
        ),
        patch(
            "eencijferho.core.decoder.decode_fields", side_effect=lambda df, *a, **k: df,
        ) as enrich,
    ):
        cmd_enrich(_args())

    assert load.call_args.kwargs["vakken_metadata_json"].endswith(name)
    assert enrich.call_args.kwargs["vakken_metadata_json"].endswith(name)


def test_decode_without_vakken_json_passes_none(output_dir):
    """Without a Vakkenbestanden JSON the decoder gets None, as before (#204)."""
    with (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}) as load,
        patch("eencijferho.core.decoder.decode_fields_dec_only", side_effect=lambda df, *a, **k: df),
    ):
        cmd_decode(_args())

    assert load.call_args.kwargs["vakken_metadata_json"] is None


# --- Decode/enrich on pipeline output (#206) ---------------------------------

# Headers as a default pipeline run writes them (snake_case).
SNAKE_ROWS = [
    {"burgerservicenummer": "000000012", "opleidingscode": "56604", "geslacht": "1"},
    {"burgerservicenummer": "000000048", "opleidingscode": "34567", "geslacht": "2"},
]


def _add_dec_column(df, *args, **kwargs):
    """Stub decoder: adds one Dec column the way the decoder names it ({var}__{col})."""
    var = df.columns[1]
    return df.with_columns(pl.Series(f"{var}__iscedf2013_rubriek", ["041305", "000123"]))


def _patch_decode(side_effect=_add_dec_column):
    return (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}),
        patch("eencijferho.core.decoder.decode_fields_dec_only", side_effect=side_effect),
    )


def _patch_enrich(side_effect):
    return (
        patch("eencijferho.core.decoder.load_dec_tables_from_metadata", return_value={}),
        patch(
            "eencijferho.core.decoder.load_variable_mappings",
            return_value={"geslacht": {"orig_name": "Geslacht", "mapping": {}}},
        ),
        patch("eencijferho.core.decoder.decode_fields", side_effect=side_effect),
    )


def test_decode_skips_decoded_and_enriched_files(output_dir, make_csv, capsys):
    """Only the main file is decoded; the _enriched file is neither read nor touched."""
    make_csv(output_dir / "EV2023_decoded.csv", ROWS)
    make_csv(output_dir / "EV2023_enriched.csv", ROWS)
    enriched_bytes = (output_dir / "EV2023_enriched.csv").read_bytes()
    load, decode = _patch_decode(side_effect=lambda df, *a, **k: df)

    with load, decode as decoder:
        cmd_decode(_args())

    assert decoder.call_count == 1
    assert not (output_dir / "EV2023_enriched_decoded.csv").exists()
    assert not (output_dir / "EV2023_decoded_decoded.csv").exists()
    assert (output_dir / "EV2023_enriched.csv").read_bytes() == enriched_bytes
    assert "1 bestand(en) gedecodeerd" in capsys.readouterr().out


def test_enrich_ignores_decoded_files_of_non_main_files(output_dir, make_csv):
    """A stray X_enriched_decoded.csv (written by older versions) is not enriched again."""
    make_csv(output_dir / "EV2023_decoded.csv", ROWS)
    make_csv(output_dir / "EV2023_enriched.csv", ROWS)
    make_csv(output_dir / "EV2023_enriched_decoded.csv", ROWS)
    load, maps, enrich = _patch_enrich(
        lambda df, *a, **k: df.with_columns(pl.col("Geslacht").replace({"1": "man"}))
    )

    with load, maps, enrich as decoder:
        cmd_enrich(_args())

    assert decoder.call_count == 1
    assert not (output_dir / "EV2023_enriched_enriched.csv").exists()
    assert (output_dir / "EV2023_enriched.csv").exists()


def test_decode_snake_case_source_gets_pipeline_names(output_dir, make_csv):
    """On snake_case pipeline output the new Dec columns get single underscores."""
    make_csv(output_dir / "EV2023.csv", SNAKE_ROWS)
    load, decode = _patch_decode()

    with load, decode:
        cmd_decode(_args())

    decoded = _read(output_dir / "EV2023_decoded.csv")
    assert decoded.columns == [
        "burgerservicenummer", "opleidingscode", "geslacht", "opleidingscode_iscedf2013_rubriek",
    ]
    assert decoded["burgerservicenummer"].to_list() == ["000000012", "000000048"]
    assert decoded["opleidingscode_iscedf2013_rubriek"].to_list() == ["041305", "000123"]


def test_decode_original_headers_keep_decoder_names(output_dir):
    """Source with original headers (pipeline --skip-snake-case): names stay as the decoder made them."""
    load, decode = _patch_decode()

    with load, decode:
        cmd_decode(_args())

    decoded = _read(output_dir / "EV2023_decoded.csv")
    assert decoded.columns == [
        "Burgerservicenummer", "Onderwijsnummer", "Geslacht", "Onderwijsnummer__iscedf2013_rubriek",
    ]
    assert decoded["Onderwijsnummer__iscedf2013_rubriek"].to_list() == ["041305", "000123"]


def test_decode_snake_case_name_collision_is_not_swallowed(output_dir, make_csv):
    """Collapsing __ into an existing name raises, as the pipeline's rename would."""
    make_csv(output_dir / "EV2023.csv", SNAKE_ROWS)
    load, decode = _patch_decode(
        side_effect=lambda df, *a, **k: df.with_columns(pl.col("geslacht").alias("geslacht__"))
    )

    with load, decode, pytest.raises(pl.exceptions.DuplicateError):
        cmd_decode(_args())

    assert not (output_dir / "EV2023_decoded.csv").exists()


def test_enrich_snake_case_source_identical_is_skipped(output_dir, make_csv, capsys):
    """The re-decoded frame is compared to the decoded file under the file's (snake_case) names."""
    make_csv(output_dir / "EV2023.csv", SNAKE_ROWS)
    make_csv(
        output_dir / "EV2023_decoded.csv",
        [
            {**row, "opleidingscode_iscedf2013_rubriek": code}
            for row, code in zip(SNAKE_ROWS, ["041305", "000123"])
        ],
    )
    load, maps, enrich = _patch_enrich(_add_dec_column)

    with load, maps, enrich:
        cmd_enrich(_args())

    assert not (output_dir / "EV2023_enriched.csv").exists()
    assert "identiek aan decoded" in capsys.readouterr().out


def test_enrich_snake_case_source_writes_pipeline_names(output_dir, make_csv):
    make_csv(output_dir / "EV2023.csv", SNAKE_ROWS)
    make_csv(output_dir / "EV2023_decoded.csv", SNAKE_ROWS)

    def fake_enrich(df, *args, **kwargs):
        df = _add_dec_column(df)
        return df.with_columns(pl.col("geslacht").replace({"1": "man", "2": "vrouw"}))

    load, maps, enrich = _patch_enrich(fake_enrich)

    with load, maps, enrich:
        cmd_enrich(_args())

    enriched = _read(output_dir / "EV2023_enriched.csv")
    assert enriched.columns == [
        "burgerservicenummer", "opleidingscode", "geslacht", "opleidingscode_iscedf2013_rubriek",
    ]
    assert enriched["burgerservicenummer"].to_list() == ["000000012", "000000048"]
    assert enriched["geslacht"].to_list() == ["man", "vrouw"]

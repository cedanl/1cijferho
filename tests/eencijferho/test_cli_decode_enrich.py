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

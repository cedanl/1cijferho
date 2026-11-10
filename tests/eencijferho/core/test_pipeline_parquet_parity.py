"""The pipeline writes a Parquet file that is the same data as its CSV (#200).

Header normalisation (snake_case) must run before the Parquet step, otherwise
the Parquet keeps the raw DUO headers and shares no column name with the CSV.
These tests run the real header and Parquet steps of ``run_turbo_convert_pipeline``
on a small synthetic CSV; conversion, decoding, validation and BSN translation
are stubbed out.
"""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from eencijferho.config import OutputConfig
from eencijferho.core.pipeline import run_turbo_convert_pipeline

# Raw DUO headers with zero-padded identifiers. Synthetic values only.
DUO_ROWS = [
    {
        "Burgerservicenummer": "000000012",
        "Persoonsgebonden nummer": "000000024",
        "Onderwijsnummer": "000000036",
        "Inschrijvingsjaar": "2023",
    },
    {
        "Burgerservicenummer": "000000048",
        "Persoonsgebonden nummer": "000000051",
        "Onderwijsnummer": "000000063",
        "Inschrijvingsjaar": "2024",
    },
]


@pytest.fixture
def stub_pipeline_steps():
    """Stub every step except header normalisation and Parquet compression."""
    with (
        patch("eencijferho.core.pipeline.converter") as converter,
        patch("eencijferho.core.pipeline.decoder") as decoder,
        patch("eencijferho.core.pipeline.cv") as cv,
        patch("eencijferho.core.pipeline.tr") as tr,
    ):
        decoder.load_dec_tables_from_metadata.return_value = {}
        decoder.load_variable_mappings.return_value = {}
        yield {"converter": converter, "decoder": decoder, "cv": cv, "tr": tr}


def _run(tmp_path, make_csv, **config):
    out = tmp_path / "out"
    out.mkdir()
    make_csv(out / "EV2023.csv", DUO_ROWS)
    progress: list[int] = []
    run_turbo_convert_pipeline(
        input_dir=str(tmp_path / "in"),
        output_dir=str(out),
        metadata_dir=str(tmp_path / "metadata"),
        progress_callback=progress.append,
        output_config=OutputConfig(variants=[], **config),
    )
    return out, progress


def _read_pair(out):
    csv = pl.read_csv(out / "EV2023.csv", separator=";", infer_schema_length=0)
    parquet = pl.read_parquet(out / "EV2023.parquet")
    return csv, parquet


def test_parquet_has_the_snake_case_headers_of_the_csv(tmp_path, make_csv, stub_pipeline_steps):
    out, _ = _run(tmp_path, make_csv)
    csv, parquet = _read_pair(out)

    assert csv.columns == [
        "burgerservicenummer",
        "persoonsgebonden_nummer",
        "onderwijsnummer",
        "inschrijvingsjaar",
    ]
    assert parquet.columns == csv.columns


def test_parquet_equals_the_csv_in_dtypes_and_values(tmp_path, make_csv, stub_pipeline_steps):
    out, _ = _run(tmp_path, make_csv)
    csv, parquet = _read_pair(out)

    assert set(parquet.dtypes) == {pl.String}
    assert parquet.equals(csv)
    assert parquet["burgerservicenummer"].to_list() == ["000000012", "000000048"]
    assert parquet["persoonsgebonden_nummer"].to_list() == ["000000024", "000000051"]
    assert parquet["onderwijsnummer"].to_list() == ["000000036", "000000063"]


def test_skip_snake_case_keeps_original_headers_in_both(tmp_path, make_csv, stub_pipeline_steps):
    out, _ = _run(tmp_path, make_csv, column_casing="none")
    csv, parquet = _read_pair(out)

    assert csv.columns == list(DUO_ROWS[0].keys())
    assert parquet.columns == csv.columns
    assert parquet.equals(csv)


def test_skip_parquet_still_normalises_csv_headers(tmp_path, make_csv, stub_pipeline_steps):
    out, _ = _run(tmp_path, make_csv, formats=[])

    assert not (out / "EV2023.parquet").exists()
    csv = pl.read_csv(out / "EV2023.csv", separator=";", infer_schema_length=0)
    assert csv.columns[0] == "burgerservicenummer"


def test_header_step_runs_before_parquet_step_between_progress_marks(tmp_path, stub_pipeline_steps):
    """Order is 75 → snake_case → 90 → parquet → 100."""
    events: list = []
    ch = MagicMock()
    ch.convert_csv_headers_to_snake_case.side_effect = lambda *a, **k: events.append("snake_case")
    co = MagicMock()
    co.convert_csv_to_parquet.side_effect = lambda *a, **k: events.append("parquet")

    with patch("eencijferho.core.pipeline.ch", ch), patch("eencijferho.core.pipeline.co", co):
        run_turbo_convert_pipeline(
            input_dir=str(tmp_path / "in"),
            output_dir=str(tmp_path / "out"),
            metadata_dir=str(tmp_path / "metadata"),
            progress_callback=events.append,
            output_config=OutputConfig(variants=[]),
        )

    assert events[events.index(75):] == [75, "snake_case", 90, "parquet", 100]

# Tests for eencijferho.utils.compressor

import pytest
import polars as pl

from eencijferho.utils.compressor import convert_csv_to_parquet


def test_compressor_creates_parquet(tmp_path, make_csv):
    make_csv(tmp_path / "EV2023.csv", [{"Naam": "Jan"}, {"Naam": "Piet"}])
    convert_csv_to_parquet(str(tmp_path))
    assert (tmp_path / "EV2023.parquet").exists()


@pytest.mark.parametrize("filename", ["Dec_geslacht.csv", "DEC_LANDCODE.csv"])
def test_compressor_skips_dec_files(tmp_path, make_csv, filename):
    make_csv(tmp_path / filename, [{"Code": "1"}])
    convert_csv_to_parquet(str(tmp_path))
    assert not (tmp_path / filename.replace(".csv", ".parquet")).exists()


def test_compressor_data_round_trips(tmp_path, make_csv):
    make_csv(
        tmp_path / "EV2023.csv",
        [{"Naam": "Jan", "Waarde": "42"}, {"Naam": "Piet", "Waarde": "7"}],
    )
    convert_csv_to_parquet(str(tmp_path))
    df = pl.read_parquet(tmp_path / "EV2023.parquet")
    assert df.shape == (2, 2)
    assert list(df.columns) == ["Naam", "Waarde"]


def test_compressor_empty_directory_no_error(tmp_path):
    convert_csv_to_parquet(str(tmp_path))

# Tests for eencijferho.utils.compressor

import pytest
import polars as pl
from pathlib import Path

from eencijferho.utils.compressor import convert_csv_to_parquet


def _make_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    lines = [";".join(cols)] + [";".join(str(row[c]) for c in cols) for row in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def test_compressor_creates_parquet(tmp_path):
    _make_csv(tmp_path / "EV2023.csv", [{"Naam": "Jan"}, {"Naam": "Piet"}])
    convert_csv_to_parquet(str(tmp_path))
    assert (tmp_path / "EV2023.parquet").exists()


@pytest.mark.parametrize("filename", ["Dec_geslacht.csv", "DEC_LANDCODE.csv"])
def test_compressor_skips_dec_files(tmp_path, filename):
    _make_csv(tmp_path / filename, [{"Code": "1"}])
    convert_csv_to_parquet(str(tmp_path))
    assert not (tmp_path / filename.replace(".csv", ".parquet")).exists()


def test_compressor_data_round_trips(tmp_path):
    _make_csv(
        tmp_path / "EV2023.csv",
        [{"Naam": "Jan", "Waarde": "42"}, {"Naam": "Piet", "Waarde": "7"}],
    )
    convert_csv_to_parquet(str(tmp_path))
    df = pl.read_parquet(tmp_path / "EV2023.parquet")
    assert df.shape == (2, 2)
    assert list(df.columns) == ["Naam", "Waarde"]


def test_compressor_empty_directory_no_error(tmp_path):
    convert_csv_to_parquet(str(tmp_path))

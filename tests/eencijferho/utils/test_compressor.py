
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


def test_compressor_preserves_utf8_characters(tmp_path, make_csv):
    """Accented characters written as utf-8 must survive the CSV→Parquet roundtrip."""
    make_csv(
        tmp_path / "EV2023.csv",
        [
            {"Land": "België", "Nationaliteit": "Oekraïense"},
            {"Land": "Indonesië", "Nationaliteit": "Israëlische"},
        ],
    )
    convert_csv_to_parquet(str(tmp_path))
    df = pl.read_parquet(tmp_path / "EV2023.parquet")
    assert df["Land"].to_list() == ["België", "Indonesië"]
    assert df["Nationaliteit"].to_list() == ["Oekraïense", "Israëlische"]


# ---------------------------------------------------------------------------
# Parquet must be the same data as the CSV
# ---------------------------------------------------------------------------

# Identifiers as DUO delivers them: fixed width, zero padded. Read as numbers
# they lose the padding and stop being the identifier they are.
IDENTIFIER_ROWS = [
    {"burgerservicenummer": "000186662", "onderwijsnummer": "100000000", "opleidingscode": "041305"},
    {"burgerservicenummer": "000186199", "onderwijsnummer": "100000012", "opleidingscode": "041307"},
]


def test_compressor_keeps_leading_zeros_in_identifiers(tmp_path, make_csv):
    """A BSN is 9 digits wide. Read as an int, 000186662 and 186662 collide."""
    make_csv(tmp_path / "EV2023.csv", IDENTIFIER_ROWS)
    convert_csv_to_parquet(str(tmp_path))
    df = pl.read_parquet(tmp_path / "EV2023.parquet")
    assert df["burgerservicenummer"].to_list() == ["000186662", "000186199"]
    assert df["onderwijsnummer"].to_list() == ["100000000", "100000012"]
    assert df["opleidingscode"].to_list() == ["041305", "041307"]


def test_compressor_keeps_every_column_as_string(tmp_path, make_csv):
    """Every CSV column is a String, so every parquet column must be one too."""
    make_csv(tmp_path / "EV2023.csv", IDENTIFIER_ROWS)
    convert_csv_to_parquet(str(tmp_path))
    df = pl.read_parquet(tmp_path / "EV2023.parquet")
    assert set(df.dtypes) == {pl.String}


def test_compressor_output_matches_the_source_csv(tmp_path, make_csv):
    """The parquet holds the same names and the same values as the CSV it came from."""
    make_csv(tmp_path / "EV2023.csv", IDENTIFIER_ROWS)
    convert_csv_to_parquet(str(tmp_path))
    parquet = pl.read_parquet(tmp_path / "EV2023.parquet")
    csv = pl.read_csv(tmp_path / "EV2023.csv", separator=";", infer_schema_length=0)
    assert parquet.columns == csv.columns
    for col in csv.columns:
        assert parquet[col].to_list() == csv[col].to_list(), col

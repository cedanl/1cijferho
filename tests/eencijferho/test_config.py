"""Tests voor OutputConfig validatie in eencijferho.config."""

import pytest
from eencijferho.config import OutputConfig


# ---------------------------------------------------------------------------
# Geldige configuraties
# ---------------------------------------------------------------------------

def test_default_config_is_valid():
    cfg = OutputConfig()
    assert cfg.variants == ["decoded", "enriched"]
    assert cfg.formats == ["parquet"]
    assert cfg.encrypt is True
    assert cfg.column_casing == "snake_case"
    assert cfg.convert_ev is True
    assert cfg.convert_vakhavw is True
    assert cfg.decode_columns is None
    assert cfg.enrich_variables is None


def test_decoded_only_is_valid():
    cfg = OutputConfig(variants=["decoded"])
    assert cfg.variants == ["decoded"]


def test_no_variants_is_valid():
    cfg = OutputConfig(variants=[])
    assert cfg.variants == []


def test_no_formats_is_valid():
    cfg = OutputConfig(formats=[])
    assert cfg.formats == []


def test_snake_case_none_is_valid():
    cfg = OutputConfig(column_casing="none")
    assert cfg.column_casing == "none"


def test_decode_columns_subset():
    cfg = OutputConfig(decode_columns=["geboorteland", "nationaliteit"])
    assert cfg.decode_columns == ["geboorteland", "nationaliteit"]


def test_enrich_variables_subset():
    cfg = OutputConfig(enrich_variables=["Geslacht"])
    assert cfg.enrich_variables == ["Geslacht"]


def test_skip_ev_vakhavw_independent():
    cfg = OutputConfig(convert_ev=False, convert_vakhavw=True)
    assert cfg.convert_ev is False
    assert cfg.convert_vakhavw is True


# ---------------------------------------------------------------------------
# Invariant: enriched vereist decoded
# ---------------------------------------------------------------------------

def test_enriched_without_decoded_raises():
    with pytest.raises(ValueError, match="enriched"):
        OutputConfig(variants=["enriched"])


def test_enriched_with_decoded_is_valid():
    cfg = OutputConfig(variants=["decoded", "enriched"])
    assert "enriched" in cfg.variants


# ---------------------------------------------------------------------------
# Ongeldige waarden
# ---------------------------------------------------------------------------

def test_invalid_variant_raises():
    with pytest.raises(ValueError, match="variant"):
        OutputConfig(variants=["decoded", "onbekend"])


def test_invalid_format_raises():
    with pytest.raises(ValueError, match="format"):
        OutputConfig(formats=["csv"])


def test_invalid_column_casing_raises():
    with pytest.raises(ValueError, match="column_casing"):
        OutputConfig(column_casing="camelCase")


# ---------------------------------------------------------------------------
# pgn_mapping_* velden
# ---------------------------------------------------------------------------

def test_pgn_mapping_defaults_to_none():
    cfg = OutputConfig()
    assert cfg.pgn_mapping_file is None
    assert cfg.pgn_mapping_right_on == "persoonsgebonden_nummer"
    assert cfg.pgn_mapping_id_col == "studentnummer"


def test_pgn_mapping_file_accepted():
    cfg = OutputConfig(pgn_mapping_file="data/mapping.csv")
    assert cfg.pgn_mapping_file == "data/mapping.csv"


def test_pgn_mapping_empty_right_on_raises():
    with pytest.raises(ValueError, match="pgn_mapping_right_on"):
        OutputConfig(pgn_mapping_file="data/mapping.csv", pgn_mapping_right_on="")


def test_pgn_mapping_empty_id_col_raises():
    with pytest.raises(ValueError, match="pgn_mapping_id_col"):
        OutputConfig(pgn_mapping_file="data/mapping.csv", pgn_mapping_id_col="")

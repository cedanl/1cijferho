"""Tests voor _build_output_config in eencijferho.cli."""

import argparse
import pytest
from eencijferho.cli import _build_output_config


def _args(**kwargs) -> argparse.Namespace:
    """Bouw een Namespace met alle vlaggen op hun standaardwaarde (False/None)."""
    defaults = dict(
        skip_decode=False,
        skip_enrich=False,
        skip_parquet=False,
        skip_encrypt=False,
        skip_snake_case=False,
        skip_ev=False,
        skip_vakhavw=False,
        decode_columns=None,
        enrich_variables=None,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# Standaard (geen vlaggen)
# ---------------------------------------------------------------------------

def test_defaults_produce_full_config():
    cfg = _build_output_config(_args())
    assert cfg.variants == ["decoded", "enriched"]
    assert cfg.formats == ["parquet"]
    assert cfg.encrypt is True
    assert cfg.column_casing == "snake_case"
    assert cfg.convert_ev is True
    assert cfg.convert_vakhavw is True


# ---------------------------------------------------------------------------
# skip-decode / skip-enrich nesting (hier zat eerder een bug)
# ---------------------------------------------------------------------------

def test_skip_decode_removes_both_variants():
    cfg = _build_output_config(_args(skip_decode=True))
    assert cfg.variants == []


def test_skip_enrich_keeps_decoded():
    cfg = _build_output_config(_args(skip_enrich=True))
    assert cfg.variants == ["decoded"]
    assert "enriched" not in cfg.variants


def test_skip_decode_ignores_skip_enrich():
    """Als decoded overgeslagen wordt, mag enriched er niet stiekem in sluipen."""
    cfg = _build_output_config(_args(skip_decode=True, skip_enrich=False))
    assert "enriched" not in cfg.variants
    assert "decoded" not in cfg.variants


# ---------------------------------------------------------------------------
# Overige vlaggen
# ---------------------------------------------------------------------------

def test_skip_parquet_produces_empty_formats():
    cfg = _build_output_config(_args(skip_parquet=True))
    assert cfg.formats == []


def test_skip_encrypt():
    cfg = _build_output_config(_args(skip_encrypt=True))
    assert cfg.encrypt is False


def test_skip_snake_case():
    cfg = _build_output_config(_args(skip_snake_case=True))
    assert cfg.column_casing == "none"


def test_skip_ev_only():
    cfg = _build_output_config(_args(skip_ev=True))
    assert cfg.convert_ev is False
    assert cfg.convert_vakhavw is True


def test_skip_vakhavw_only():
    cfg = _build_output_config(_args(skip_vakhavw=True))
    assert cfg.convert_vakhavw is False
    assert cfg.convert_ev is True


def test_skip_ev_and_vakhavw_independent():
    cfg = _build_output_config(_args(skip_ev=True, skip_vakhavw=True))
    assert cfg.convert_ev is False
    assert cfg.convert_vakhavw is False


# ---------------------------------------------------------------------------
# decode_columns / enrich_variables
# ---------------------------------------------------------------------------

def test_decode_columns_passed_through():
    cfg = _build_output_config(_args(decode_columns=["geboorteland"]))
    assert cfg.decode_columns == ["geboorteland"]


def test_decode_columns_empty_list_becomes_none():
    cfg = _build_output_config(_args(decode_columns=[]))
    assert cfg.decode_columns is None


def test_enrich_variables_passed_through():
    cfg = _build_output_config(_args(enrich_variables=["Geslacht"]))
    assert cfg.enrich_variables == ["Geslacht"]


def test_enrich_variables_empty_list_becomes_none():
    cfg = _build_output_config(_args(enrich_variables=[]))
    assert cfg.enrich_variables is None

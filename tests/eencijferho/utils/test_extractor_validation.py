"""
Unit tests for eencijferho.utils.extractor_validation.validate_metadata

Tests:
    - test_valid_layout_passes: aaneengesloten layout -> success, geen issues
    - test_gap_between_fields_is_detected: gat tussen twee velden wordt gemeld
    - test_dec_landcode_consistent_layout_passes: consistent layout slaagt zonder
      enige correctie, ook niet voor de bestanden waar vroeger een speciale
      behandeling voor bestond
    - test_duplicate_field_names_are_detected
"""

import polars as pl
import pytest

from eencijferho.utils.extractor_validation import validate_metadata


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_layout(path, names, starts, widths, opmerkingen=None):
    """Write a DUO-style layout specification (.xlsx) to path."""
    opmerkingen = opmerkingen or [""] * len(names)
    pl.DataFrame(
        {
            "ID": list(range(1, len(names) + 1)),
            "Naam": names,
            "Startpositie": starts,
            "Aantal posities": widths,
            "Opmerking": opmerkingen,
        }
    ).write_excel(str(path))
    return path


@pytest.fixture
def consistent_layout(tmp_path):
    """Two adjacent fields, 4 positions each, starting at position 1."""
    return _write_layout(
        tmp_path / "Bestandsbeschrijving_test.xlsx",
        names=["Code land", "Naam land"],
        starts=[1, 5],
        widths=[4, 4],
    )


@pytest.fixture
def layout_with_gap(tmp_path):
    """Same fields, but the second one starts one position too late."""
    return _write_layout(
        tmp_path / "Bestandsbeschrijving_gat.xlsx",
        names=["Code land", "Naam land"],
        starts=[1, 6],
        widths=[4, 4],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_valid_layout_passes(consistent_layout):
    success, issues = validate_metadata(consistent_layout)

    assert success is True
    assert issues["total_issues"] == 0
    assert issues["position_errors"] == []
    assert issues["length_mismatch"] is False


def test_gap_between_fields_is_detected(layout_with_gap):
    """The position check is the guard against fields silently overlapping.

    A gap means the next field starts later than the previous one ends, so the
    converter would read the wrong columns. That has to be reported, not
    swallowed.
    """
    success, issues = validate_metadata(layout_with_gap)

    assert success is False
    assert len(issues["position_errors"]) == 1
    assert issues["position_errors"][0]["gap_size"] == 1
    assert issues["position_errors"][0]["previous_field"] == "Code land"
    assert issues["position_errors"][0]["current_field"] == "Naam land"


def test_dec_landcode_consistent_layout_passes(tmp_path):
    """A consistent layout passes for the files that once needed a fixup.

    The removed code shifted Start_Positie by one on the first row of
    Dec_landcode/Dec_nationaliteitscode. These tables are in fact already
    consistent, so that shift could only ever have hidden a real error.
    """
    path = _write_layout(
        tmp_path / "Dec_landcode.xlsx",
        names=["Code land", "Naam land"],
        starts=[1, 5],
        widths=[4, 4],
    )

    success, issues = validate_metadata(path)

    assert success is True, issues
    assert issues["total_issues"] == 0


def test_dec_landcode_layout_with_gap_is_detected(tmp_path):
    """The gap check keeps working for those files too — no blind spot."""
    path = _write_layout(
        tmp_path / "Dec_nationaliteitscode.xlsx",
        names=["Code land", "Naam land"],
        starts=[1, 6],
        widths=[4, 4],
    )

    success, issues = validate_metadata(path)

    assert success is False
    assert len(issues["position_errors"]) == 1


def test_duplicate_field_names_are_detected(tmp_path):
    path = _write_layout(
        tmp_path / "Bestandsbeschrijving_dubbel.xlsx",
        names=["Code land", "Code land"],
        starts=[1, 5],
        widths=[4, 4],
    )

    success, issues = validate_metadata(path)

    assert success is False
    assert len(issues["duplicates"]) == 1
    assert issues["duplicates"][0]["name"] == "Code land"

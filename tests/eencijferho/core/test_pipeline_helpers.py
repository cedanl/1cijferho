"""Tests for pipeline helper functions."""

import pytest

from eencijferho.core.pipeline import _is_main_csv_file


class TestPipelineHelpers:
    """Test pipeline module helper functions."""

    def test_is_main_csv_file_ev(self):
        """Identify main EV CSV files."""
        assert _is_main_csv_file("EV123_data.csv") is True
        assert _is_main_csv_file("EV_test.csv") is True

    def test_is_main_csv_file_vakhavw(self):
        """Identify main VAKHAVW CSV files."""
        assert _is_main_csv_file("VAKHAVW_data.csv") is True
        assert _is_main_csv_file("VAKHAVW_test.csv") is True

    def test_is_main_csv_file_decoded_variant(self):
        """Reject decoded CSV files."""
        assert _is_main_csv_file("EV123_decoded.csv") is False
        assert _is_main_csv_file("VAKHAVW_decoded.csv") is False

    def test_is_main_csv_file_enriched_variant(self):
        """Reject enriched CSV files."""
        assert _is_main_csv_file("EV123_enriched.csv") is False
        assert _is_main_csv_file("VAKHAVW_enriched.csv") is False

    def test_is_main_csv_file_non_csv(self):
        """Reject non-CSV files."""
        assert _is_main_csv_file("EV123.txt") is False
        assert _is_main_csv_file("VAKHAVW.json") is False

    def test_is_main_csv_file_wrong_prefix(self):
        """Reject files with wrong prefixes."""
        assert _is_main_csv_file("DEC_data.csv") is False
        assert _is_main_csv_file("OTHER_data.csv") is False

    def test_is_main_csv_file_lowercase(self):
        """Reject lowercase prefixes."""
        assert _is_main_csv_file("ev123_data.csv") is False
        assert _is_main_csv_file("vakhavw_data.csv") is False

    def test_is_main_csv_file_with_suffix(self):
        """Identify main files with standard suffixes."""
        assert _is_main_csv_file("EV299XX24_DEMO.csv") is True
        assert _is_main_csv_file("VAKHAVW_99XX_TEST.csv") is True

    def test_is_main_csv_file_both_variants(self):
        """Reject files that have both decoded and enriched markers."""
        assert _is_main_csv_file("EV_decoded_enriched.csv") is False

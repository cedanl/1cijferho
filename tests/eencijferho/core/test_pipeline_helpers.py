"""Tests for pipeline helper functions."""

import pytest
from unittest.mock import MagicMock, patch
import polars as pl

from eencijferho.core.pipeline import _is_main_csv_file, _collect_output_files, _process_enriched_file
from eencijferho.config import OutputConfig, ENRICHED_SUFFIX


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


class TestCollectOutputFiles:
    """Test _collect_output_files function."""

    def test_collect_output_files_empty_directory(self):
        """Return empty list for empty output directory."""
        storage = MagicMock()
        storage.list_files.return_value = []

        result = _collect_output_files(storage, "output_dir")

        assert result == []
        storage.list_files.assert_called_once_with("output_dir/*")

    def test_collect_output_files_single_file(self):
        """Collect single file with size information."""
        storage = MagicMock()
        storage.list_files.return_value = ["output_dir/file.csv"]
        storage.read_bytes.return_value = b"x" * 1024  # 1 KB

        result = _collect_output_files(storage, "output_dir")

        assert len(result) == 1
        assert result[0]["name"] == "file.csv"
        assert result[0]["size"] == 1024
        assert result[0]["size_formatted"] == "1.0 KB"

    def test_collect_output_files_multiple_files(self):
        """Collect multiple files with different sizes."""
        storage = MagicMock()
        storage.list_files.return_value = [
            "output_dir/file1.csv",
            "output_dir/file2.parquet",
        ]
        storage.read_bytes.side_effect = [
            b"x" * 2048,  # 2 KB
            b"x" * 5120,  # 5 KB
        ]

        result = _collect_output_files(storage, "output_dir")

        assert len(result) == 2
        assert result[0]["name"] == "file1.csv"
        assert result[0]["size"] == 2048
        assert result[0]["size_formatted"] == "2.0 KB"
        assert result[1]["name"] == "file2.parquet"
        assert result[1]["size"] == 5120
        assert result[1]["size_formatted"] == "5.0 KB"

    def test_collect_output_files_read_error_handled(self):
        """Handle read errors gracefully by setting size to 0."""
        storage = MagicMock()
        storage.list_files.return_value = [
            "output_dir/accessible.csv",
            "output_dir/inaccessible.csv",
        ]
        storage.read_bytes.side_effect = [
            b"x" * 1024,
            Exception("Read failed"),
        ]

        result = _collect_output_files(storage, "output_dir")

        assert len(result) == 2
        assert result[0]["size"] == 1024
        assert result[1]["size"] == 0
        assert result[1]["size_formatted"] == "0.0 KB"

    def test_collect_output_files_large_files(self):
        """Format large file sizes correctly."""
        storage = MagicMock()
        storage.list_files.return_value = ["output_dir/large.csv"]
        storage.read_bytes.return_value = b"x" * (1024 * 1024)  # 1 MB = 1024 KB

        result = _collect_output_files(storage, "output_dir")

        assert result[0]["size"] == 1024 * 1024
        assert result[0]["size_formatted"] == "1024.0 KB"


class TestProcessEnrichedFile:
    """Test _process_enriched_file function."""

    def test_process_enriched_file_no_variable_mappings(self):
        """Skip enrichment when no variable mappings available."""
        storage = MagicMock()
        main_df = pl.DataFrame({"Col1": [1, 2, 3]})
        log = "[pipeline] Starting..."

        with patch("eencijferho.core.pipeline.ch"):
            result = _process_enriched_file(
                storage=storage,
                main_df=main_df,
                filepath="output/file.csv",
                filename="file.csv",
                log=log,
                dec_metadata_json="metadata.json",
                dec_tables={},
                variable_metadata_json="variables.json",
                var_maps={},  # Empty var_maps
                output_config=OutputConfig(),
                dec_only_df=None,
            )

        # Should add log message and return unchanged
        assert "geen variable_metadata mappings" in result
        assert result.startswith("[pipeline] Starting...")

    def test_process_enriched_file_identical_to_decoded(self):
        """Skip writing enriched file when identical to decoded."""
        storage = MagicMock()
        main_df = pl.DataFrame({"EnrichedCol": [1, 2, 3]})
        dec_only_df = main_df.clone()
        log = "[pipeline] Starting..."

        with patch("eencijferho.core.pipeline.ch") as mock_ch:
            with patch("eencijferho.core.pipeline.decoder") as mock_decoder:
                mock_ch.normalize_name = lambda x: x.lower()
                mock_ch.clean_header_name = lambda x: x
                mock_decoder.decode_fields.return_value = dec_only_df

                result = _process_enriched_file(
                    storage=storage,
                    main_df=main_df,
                    filepath="output/file.csv",
                    filename="file.csv",
                    log=log,
                    dec_metadata_json="metadata.json",
                    dec_tables={},
                    variable_metadata_json="variables.json",
                    var_maps={"enrichedcol": "value"},
                    output_config=OutputConfig(),
                    dec_only_df=dec_only_df,
                )

        # Should log that enriched is identical and skip writing
        assert "identiek aan _decoded" in result
        storage.write_text.assert_not_called()

    def test_process_enriched_file_writes_when_different(self):
        """Write enriched file when different from decoded."""
        storage = MagicMock()
        main_df = pl.DataFrame({"EnrichedCol": [1, 2, 3]})
        enriched_df = pl.DataFrame({"EnrichedCol": [10, 20, 30]})
        dec_only_df = pl.DataFrame({"EnrichedCol": [1, 2, 3]})
        log = "[pipeline] Starting..."

        with patch("eencijferho.core.pipeline.ch") as mock_ch:
            with patch("eencijferho.core.pipeline.decoder") as mock_decoder:
                mock_ch.normalize_name = lambda x: x.lower()
                mock_ch.clean_header_name = lambda x: x
                mock_decoder.decode_fields.return_value = enriched_df

                result = _process_enriched_file(
                    storage=storage,
                    main_df=main_df,
                    filepath="output/file.csv",
                    filename="file.csv",
                    log=log,
                    dec_metadata_json="metadata.json",
                    dec_tables={},
                    variable_metadata_json="variables.json",
                    var_maps={"enrichedcol": "value"},
                    output_config=OutputConfig(),
                    dec_only_df=dec_only_df,
                )

        # Should write the enriched file
        storage.write_text.assert_called_once()
        call_args = storage.write_text.call_args
        # Check that the enriched suffix is used
        assert ENRICHED_SUFFIX in call_args[0][1]

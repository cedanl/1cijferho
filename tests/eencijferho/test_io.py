"""Tests for storage I/O path resolution and configuration."""

import os
import tempfile

from eencijferho.io.backends.disk import DiskBackend
from eencijferho.io.config import StorageConfig


class TestDiskBackendPathResolution:
    """Verify DiskBackend resolves paths without doubling the data/ prefix."""

    def test_default_base_path_is_cwd(self):
        backend = DiskBackend()
        assert str(backend.base_path) == "."

    def test_relative_path_not_doubled(self):
        """data/01-input must NOT become data/data/01-input."""
        backend = DiskBackend()
        resolved = backend._resolve("data/01-input/file.csv")
        assert str(resolved) == "data/01-input/file.csv"

    def test_absolute_path_passes_through(self):
        backend = DiskBackend()
        resolved = backend._resolve("/abs/path/file.csv")
        assert str(resolved) == "/abs/path/file.csv"

    def test_list_files_with_data_prefix(self, tmp_path):
        """list_files should find files when paths include data/ prefix."""
        data_dir = tmp_path / "data" / "01-input"
        data_dir.mkdir(parents=True)
        (data_dir / "EV2023.asc").write_text("test")
        (data_dir / "README.txt").write_text("test")

        backend = DiskBackend(base_path=str(tmp_path))
        files = backend.list_files("data/01-input/*.asc")
        assert len(files) == 1
        assert files[0].endswith("EV2023.asc")

    def test_write_and_read_with_data_prefix(self, tmp_path):
        """Round-trip read/write with data/ in the path."""
        backend = DiskBackend(base_path=str(tmp_path))
        backend.write_text("hello", "data/01-input/test.txt")
        assert backend.read_text("data/01-input/test.txt") == "hello"


class TestStorageConfigDefaults:
    """Verify StorageConfig defaults match expectations."""

    def test_default_disk_base_path_is_cwd(self):
        config = StorageConfig()
        assert config.disk_base_path == "."

    def test_env_override_disk_base_path(self, monkeypatch):
        monkeypatch.setenv("STORAGE_DISK_BASE_PATH", "/custom/path")
        config = StorageConfig.from_env()
        assert config.disk_base_path == "/custom/path"


class TestDataDirSingleSourceOfTruth:
    """Verify DATA_DIR is used consistently across config modules."""

    def test_package_config_uses_data_dir(self):
        from eencijferho.config import DATA_DIR, INPUT_DIR, OUTPUT_DIR, METADATA_DIR

        assert DATA_DIR == "data"
        assert INPUT_DIR.startswith(f"{DATA_DIR}/")
        assert OUTPUT_DIR.startswith(f"{DATA_DIR}/")
        assert METADATA_DIR.startswith(f"{DATA_DIR}/")

    def test_package_getters_use_data_dir(self):
        from eencijferho.config import DATA_DIR, get_input_dir, get_output_dir, get_decoder_input_dir

        assert get_input_dir(demo_mode=False).startswith(f"{DATA_DIR}/")
        assert get_input_dir(demo_mode=True).startswith(f"{DATA_DIR}/")
        assert get_output_dir(demo_mode=False).startswith(f"{DATA_DIR}/")
        assert get_output_dir(demo_mode=True).startswith(f"{DATA_DIR}/")
        assert get_decoder_input_dir().startswith(f"{DATA_DIR}/")

    def test_cli_pipeline_paths_resolve_correctly(self, tmp_path):
        """Simulate the CLI pipeline path resolution to catch doubling."""
        backend = DiskBackend(base_path=".")

        # These are the paths the CLI would pass
        input_dir = "data/01-input/DEMO"
        output_dir = "data/02-output"

        # _resolve should NOT double the data/ prefix
        resolved_input = backend._resolve(input_dir)
        resolved_output = backend._resolve(output_dir)

        assert "data/data" not in str(resolved_input)
        assert "data/data" not in str(resolved_output)

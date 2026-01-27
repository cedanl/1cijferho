"""
Tests for DiskBackend storage implementation
"""

import json

import polars as pl
import pytest


class TestDiskBackendInit:
    """Tests for DiskBackend initialization"""

    def test_default_config(self, clean_env):
        """Test DiskBackend uses default config when none provided"""
        from backend.io.backends.disk import DiskBackend

        backend = DiskBackend()
        assert str(backend.base_path) == "data"

    def test_custom_config(self, temp_dir):
        """Test DiskBackend uses provided config"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)

        backend = DiskBackend(config)
        assert backend.base_path == temp_dir


class TestDiskBackendPathResolution:
    """Tests for path resolution in DiskBackend"""

    def test_resolve_relative_path(self, temp_dir):
        """Test that relative paths are resolved against base_path"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        resolved = backend._resolve_path("subdir/file.csv")
        assert resolved == temp_dir / "subdir" / "file.csv"

    def test_resolve_absolute_path(self, temp_dir):
        """Test that absolute paths are returned unchanged"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        absolute_path = "/absolute/path/file.csv"
        resolved = backend._resolve_path(absolute_path)
        assert str(resolved) == absolute_path

    def test_get_full_path(self, temp_dir):
        """Test get_full_path returns resolved path as string"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        full_path = backend.get_full_path("subdir/file.csv")
        assert full_path == str(temp_dir / "subdir" / "file.csv")


class TestDiskBackendDataFrameOperations:
    """Tests for DataFrame read/write operations"""

    def test_write_and_read_csv(self, temp_dir, sample_dataframe):
        """Test writing and reading CSV files"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        # Write
        path = backend.write_dataframe(sample_dataframe, "test.csv", format="csv")
        assert (temp_dir / "test.csv").exists()

        # Read
        df = backend.read_dataframe("test.csv", format="csv")
        assert df.shape == sample_dataframe.shape
        assert df["name"].to_list() == sample_dataframe["name"].to_list()

    def test_write_and_read_parquet(self, temp_dir, sample_dataframe):
        """Test writing and reading Parquet files"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        # Write
        backend.write_dataframe(sample_dataframe, "test.parquet", format="parquet")
        assert (temp_dir / "test.parquet").exists()

        # Read
        df = backend.read_dataframe("test.parquet", format="parquet")
        assert df.shape == sample_dataframe.shape

    def test_write_creates_parent_directories(self, temp_dir, sample_dataframe):
        """Test that write creates parent directories automatically"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.write_dataframe(sample_dataframe, "nested/deep/test.csv")
        assert (temp_dir / "nested" / "deep" / "test.csv").exists()

    def test_unsupported_format_raises(self, temp_dir, sample_dataframe):
        """Test that unsupported formats raise ValueError"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        with pytest.raises(ValueError, match="Unsupported format"):
            backend.write_dataframe(sample_dataframe, "test.xyz", format="xyz")

        # First create a file
        backend.write_dataframe(sample_dataframe, "test.csv")
        with pytest.raises(ValueError, match="Unsupported format"):
            backend.read_dataframe("test.csv", format="xyz")


class TestDiskBackendJsonOperations:
    """Tests for JSON read/write operations"""

    def test_write_and_read_json(self, temp_dir, sample_json):
        """Test writing and reading JSON files"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        # Write
        path = backend.write_json(sample_json, "test.json")
        assert (temp_dir / "test.json").exists()

        # Read
        data = backend.read_json("test.json")
        assert data == sample_json

    def test_json_creates_parent_directories(self, temp_dir, sample_json):
        """Test that write_json creates parent directories"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.write_json(sample_json, "nested/test.json")
        assert (temp_dir / "nested" / "test.json").exists()

    def test_json_preserves_unicode(self, temp_dir):
        """Test that JSON handles unicode correctly"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        unicode_data = {"name": "Müller", "city": "北京", "emoji": "🎉"}
        backend.write_json(unicode_data, "unicode.json")

        loaded = backend.read_json("unicode.json")
        assert loaded == unicode_data


class TestDiskBackendBytesOperations:
    """Tests for binary read/write operations"""

    def test_write_and_read_bytes(self, temp_dir, sample_bytes):
        """Test writing and reading binary data"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        # Write
        path = backend.write_bytes(sample_bytes, "test.bin")
        assert (temp_dir / "test.bin").exists()

        # Read
        data = backend.read_bytes("test.bin")
        assert data == sample_bytes

    def test_bytes_creates_parent_directories(self, temp_dir, sample_bytes):
        """Test that write_bytes creates parent directories"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.write_bytes(sample_bytes, "nested/test.bin")
        assert (temp_dir / "nested" / "test.bin").exists()


class TestDiskBackendExists:
    """Tests for exists() method"""

    def test_exists_returns_true_for_existing_file(self, temp_dir, sample_bytes):
        """Test that exists returns True for existing files"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.write_bytes(sample_bytes, "test.bin")
        assert backend.exists("test.bin") is True

    def test_exists_returns_false_for_missing_file(self, temp_dir):
        """Test that exists returns False for missing files"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        assert backend.exists("nonexistent.txt") is False


class TestDiskBackendListFiles:
    """Tests for list_files() method"""

    def test_list_csv_files(self, temp_dir, sample_dataframe):
        """Test listing CSV files in a directory"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        # Create test files
        backend.write_dataframe(sample_dataframe, "data/file1.csv")
        backend.write_dataframe(sample_dataframe, "data/file2.csv")
        backend.write_dataframe(sample_dataframe, "data/file3.parquet")

        # List CSV files
        files = backend.list_files("data", "*.csv")
        assert len(files) == 2
        assert all(".csv" in f for f in files)

    def test_list_empty_directory(self, temp_dir):
        """Test listing files in empty/nonexistent directory"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        files = backend.list_files("nonexistent", "*.csv")
        assert files == []

    def test_list_all_files(self, temp_dir, sample_dataframe):
        """Test listing all files with wildcard"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.write_dataframe(sample_dataframe, "data/file1.csv")
        backend.write_dataframe(sample_dataframe, "data/file2.parquet")

        files = backend.list_files("data", "*")
        assert len(files) == 2


class TestDiskBackendMakedirs:
    """Tests for makedirs() method"""

    def test_makedirs_creates_directory(self, temp_dir):
        """Test that makedirs creates directories"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.makedirs("nested/deep/directory")
        assert (temp_dir / "nested" / "deep" / "directory").is_dir()

    def test_makedirs_idempotent(self, temp_dir):
        """Test that makedirs is idempotent (doesn't fail if dir exists)"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.makedirs("existing")
        backend.makedirs("existing")  # Should not raise
        assert (temp_dir / "existing").is_dir()


class TestDiskBackendDelete:
    """Tests for delete() method"""

    def test_delete_existing_file(self, temp_dir, sample_bytes):
        """Test deleting an existing file"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        backend.write_bytes(sample_bytes, "test.bin")
        assert backend.exists("test.bin")

        result = backend.delete("test.bin")
        assert result is True
        assert not backend.exists("test.bin")

    def test_delete_nonexistent_file(self, temp_dir):
        """Test deleting a nonexistent file returns False"""
        from backend.io.backends.disk import DiskBackend
        from backend.io.config import DiskConfig

        config = DiskConfig()
        config.base_path = str(temp_dir)
        backend = DiskBackend(config)

        result = backend.delete("nonexistent.txt")
        assert result is False

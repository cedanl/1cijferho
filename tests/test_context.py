"""
Tests for storage context manager and get_storage function
"""

import pytest

from backend.io.backends.disk import DiskBackend


class TestStorageContext:
    """Tests for storage_context context manager"""

    def test_yields_storage_backend(self, disk_backend_env):
        """Test that storage_context yields a StorageBackend instance"""
        from backend.io import storage_context
        from backend.io.backends.base import StorageBackend

        with storage_context() as storage:
            assert isinstance(storage, StorageBackend)

    def test_yields_disk_backend_by_default(self, disk_backend_env):
        """Test that storage_context yields DiskBackend when STORAGE_BACKEND=disk"""
        from backend.io import storage_context

        with storage_context() as storage:
            assert isinstance(storage, DiskBackend)

    def test_backend_type_override(self, disk_backend_env, monkeypatch):
        """Test that backend_type parameter overrides environment"""
        from backend.io import storage_context

        # Even with disk_backend_env, we can override to disk explicitly
        with storage_context(backend_type="disk") as storage:
            assert isinstance(storage, DiskBackend)

    def test_context_manager_cleanup(self, disk_backend_env, sample_dataframe):
        """Test that context manager properly exits"""
        from backend.io import storage_context

        # Ensure no exceptions on exit
        with storage_context() as storage:
            storage.write_dataframe(sample_dataframe, "test.csv")

        # After exit, we should be able to enter a new context
        with storage_context() as storage:
            df = storage.read_dataframe("test.csv")
            assert df.shape == sample_dataframe.shape

    def test_can_perform_operations_within_context(self, disk_backend_env, sample_dataframe, sample_json):
        """Test performing various operations within the context"""
        from backend.io import storage_context

        with storage_context() as storage:
            # Write operations
            storage.makedirs("output")
            storage.write_dataframe(sample_dataframe, "output/data.csv")
            storage.write_json(sample_json, "output/metadata.json")
            storage.write_bytes(b"binary", "output/data.bin")

            # Read operations
            df = storage.read_dataframe("output/data.csv")
            meta = storage.read_json("output/metadata.json")
            binary = storage.read_bytes("output/data.bin")

            # Verify
            assert df.shape == sample_dataframe.shape
            assert meta == sample_json
            assert binary == b"binary"

            # Check existence
            assert storage.exists("output/data.csv")

            # List files
            files = storage.list_files("output", "*.csv")
            assert len(files) == 1


class TestGetStorage:
    """Tests for get_storage function"""

    def test_returns_storage_backend(self, disk_backend_env):
        """Test that get_storage returns a StorageBackend instance"""
        from backend.io.context import get_storage
        from backend.io.backends.base import StorageBackend

        storage = get_storage()
        assert isinstance(storage, StorageBackend)

    def test_returns_disk_backend_by_default(self, disk_backend_env):
        """Test that get_storage returns DiskBackend when STORAGE_BACKEND=disk"""
        from backend.io.context import get_storage

        storage = get_storage()
        assert isinstance(storage, DiskBackend)

    def test_backend_type_override(self, disk_backend_env):
        """Test that backend_type parameter overrides environment"""
        from backend.io.context import get_storage

        storage = get_storage(backend_type="disk")
        assert isinstance(storage, DiskBackend)

    def test_persistent_access(self, disk_backend_env, sample_dataframe):
        """Test that get_storage provides persistent access"""
        from backend.io.context import get_storage

        storage = get_storage()

        # Multiple operations with same instance
        storage.write_dataframe(sample_dataframe, "file1.csv")
        storage.write_dataframe(sample_dataframe, "file2.csv")

        files = storage.list_files(".", "*.csv")
        assert len(files) >= 2


class TestGetBackend:
    """Tests for get_backend factory function"""

    def test_returns_disk_backend(self, disk_backend_env):
        """Test get_backend returns DiskBackend for 'disk' type"""
        from backend.io.backends import get_backend

        backend = get_backend()
        assert isinstance(backend, DiskBackend)

    def test_explicit_disk_backend(self, clean_env, monkeypatch):
        """Test get_backend with explicit 'disk' parameter"""
        from backend.io.backends import get_backend

        monkeypatch.setenv("STORAGE_DISK_BASE_PATH", "data")
        backend = get_backend("disk")
        assert isinstance(backend, DiskBackend)

    def test_unknown_backend_raises(self, clean_env):
        """Test that unknown backend type raises ValueError"""
        from backend.io.backends import get_backend

        with pytest.raises(ValueError, match="Unknown storage backend"):
            get_backend("unknown")

    def test_minio_requires_config(self, clean_env, monkeypatch):
        """Test that minio backend requires configuration"""
        from backend.io.backends import get_backend

        monkeypatch.setenv("STORAGE_BACKEND", "minio")

        with pytest.raises(ValueError, match="MinIO backend selected but not fully configured"):
            get_backend()

    def test_postgres_requires_config(self, clean_env, monkeypatch):
        """Test that postgres backend requires configuration"""
        from backend.io.backends import get_backend

        monkeypatch.setenv("STORAGE_BACKEND", "postgres")

        with pytest.raises(ValueError, match="PostgreSQL backend selected but not fully configured"):
            get_backend()

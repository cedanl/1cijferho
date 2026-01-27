"""
Integration tests for MinIO storage backend

These tests require a running MinIO server. They will be skipped if MinIO is not available.
To run these tests, start MinIO with:
    docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"

Then set environment variables:
    export TEST_MINIO_ENDPOINT=localhost:9000
    export TEST_MINIO_ACCESS_KEY=minioadmin
    export TEST_MINIO_SECRET_KEY=minioadmin
    export TEST_MINIO_BUCKET=test-bucket
"""

import os

import polars as pl
import pytest


def minio_available():
    """Check if MinIO is available for testing"""
    try:
        from minio import Minio
        endpoint = os.getenv("TEST_MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("TEST_MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("TEST_MINIO_SECRET_KEY", "minioadmin")

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
        client.list_buckets()
        return True
    except Exception:
        return False


# Skip all tests in this module if MinIO is not available
pytestmark = pytest.mark.skipif(
    not minio_available(),
    reason="MinIO server not available. Set TEST_MINIO_* environment variables and start MinIO."
)


@pytest.fixture
def minio_backend(minio_env):
    """Create a MinIO backend for testing"""
    from backend.io.backends.minio import MinIOBackend
    from backend.io.config import MinIOConfig

    config = MinIOConfig()
    backend = MinIOBackend(config)  # Bucket is created in __init__

    yield backend

    # Cleanup: delete test objects after each test
    try:
        objects = list(backend.client.list_objects(backend.bucket, recursive=True))
        for obj in objects:
            backend.client.remove_object(backend.bucket, obj.object_name)
    except Exception:
        pass


class TestMinIOBackendInit:
    """Tests for MinIOBackend initialization"""

    def test_requires_configuration(self, clean_env, monkeypatch):
        """Test that MinIOBackend raises when not configured"""
        from backend.io.backends.minio import MinIOBackend

        monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
        # Missing other required vars

        with pytest.raises(ValueError, match="MinIO backend requires"):
            MinIOBackend()

    def test_creates_bucket_if_missing(self, minio_backend):
        """Test that MinIOBackend creates bucket if it doesn't exist"""
        # The bucket should exist after fixture setup
        buckets = [b.name for b in minio_backend.client.list_buckets()]
        assert minio_backend.config.bucket in buckets


class TestMinIOBackendDataFrameOperations:
    """Tests for DataFrame read/write on MinIO"""

    def test_write_and_read_csv(self, minio_backend, sample_dataframe):
        """Test writing and reading CSV files"""
        minio_backend.write_dataframe(sample_dataframe, "test/data.csv")
        df = minio_backend.read_dataframe("test/data.csv")

        assert df.shape == sample_dataframe.shape
        assert df["name"].to_list() == sample_dataframe["name"].to_list()

    def test_write_and_read_parquet(self, minio_backend, sample_dataframe):
        """Test writing and reading Parquet files"""
        minio_backend.write_dataframe(sample_dataframe, "test/data.parquet", format="parquet")
        df = minio_backend.read_dataframe("test/data.parquet", format="parquet")

        assert df.shape == sample_dataframe.shape


class TestMinIOBackendJsonOperations:
    """Tests for JSON read/write on MinIO"""

    def test_write_and_read_json(self, minio_backend, sample_json):
        """Test writing and reading JSON"""
        minio_backend.write_json(sample_json, "test/metadata.json")
        data = minio_backend.read_json("test/metadata.json")

        assert data == sample_json


class TestMinIOBackendBytesOperations:
    """Tests for binary read/write on MinIO"""

    def test_write_and_read_bytes(self, minio_backend, sample_bytes):
        """Test writing and reading binary data"""
        minio_backend.write_bytes(sample_bytes, "test/data.bin")
        data = minio_backend.read_bytes("test/data.bin")

        assert data == sample_bytes


class TestMinIOBackendExists:
    """Tests for exists() method on MinIO"""

    def test_exists_true(self, minio_backend, sample_bytes):
        """Test exists returns True for existing objects"""
        minio_backend.write_bytes(sample_bytes, "test/exists.bin")
        assert minio_backend.exists("test/exists.bin") is True

    def test_exists_false(self, minio_backend):
        """Test exists returns False for missing objects"""
        assert minio_backend.exists("nonexistent/path") is False


class TestMinIOBackendListFiles:
    """Tests for list_files() method on MinIO"""

    def test_list_csv_files(self, minio_backend, sample_dataframe):
        """Test listing CSV files"""
        minio_backend.write_dataframe(sample_dataframe, "data/file1.csv")
        minio_backend.write_dataframe(sample_dataframe, "data/file2.csv")
        minio_backend.write_dataframe(sample_dataframe, "data/file3.parquet", format="parquet")

        files = minio_backend.list_files("data", "*.csv")
        assert len(files) == 2
        assert all(".csv" in f for f in files)

    def test_list_empty_prefix(self, minio_backend):
        """Test listing files with no matches"""
        files = minio_backend.list_files("nonexistent", "*.csv")
        assert files == []


class TestMinIOBackendDelete:
    """Tests for delete() method on MinIO"""

    def test_delete_existing(self, minio_backend, sample_bytes):
        """Test deleting existing object"""
        minio_backend.write_bytes(sample_bytes, "test/delete.bin")
        assert minio_backend.exists("test/delete.bin")

        result = minio_backend.delete("test/delete.bin")
        assert result is True
        assert not minio_backend.exists("test/delete.bin")

    def test_delete_nonexistent(self, minio_backend):
        """Test deleting nonexistent object"""
        result = minio_backend.delete("nonexistent")
        assert result is False

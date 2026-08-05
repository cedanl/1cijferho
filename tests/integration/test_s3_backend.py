"""
Integration tests for the generic S3 (boto3) storage backend.

MinIO is S3-compatible, so these run the boto3-based S3Backend against the
same MinIO container used by the MinIO suite. They auto-skip when boto3 or
Docker/MinIO is unavailable.

Run with:
    uv run pytest tests/integration/ -v
"""

import polars as pl
import pytest

from eencijferho.io import get_backend
from eencijferho.io.backends.s3 import S3Backend
from eencijferho.io.decorators import with_storage


# ---------------------------------------------------------------------------
# Basic read/write operations
# ---------------------------------------------------------------------------


class TestS3ReadWrite:
    """Test fundamental read/write/exists/delete operations."""

    def test_write_and_read_bytes(self, s3_backend, minio_prefix):
        path = f"{minio_prefix}/hello.bin"
        s3_backend.write_bytes(b"hello world", path)
        assert s3_backend.read_bytes(path) == b"hello world"

    def test_write_and_read_text(self, s3_backend, minio_prefix):
        path = f"{minio_prefix}/unicode.txt"
        s3_backend.write_text("héllo wörld café", path)
        assert s3_backend.read_text(path) == "héllo wörld café"

    def test_write_and_read_text_latin1(self, s3_backend, minio_prefix):
        path = f"{minio_prefix}/latin1.txt"
        text = "Vóór het HO - één test"
        s3_backend.write_text(text, path, encoding="latin-1")
        assert s3_backend.read_text(path, encoding="latin-1") == text

    def test_write_and_read_json(self, s3_backend, minio_prefix):
        path = f"{minio_prefix}/data.json"
        data = {"key": "waarde", "lijst": [1, 2, 3], "nested": {"a": True}}
        s3_backend.write_json(data, path)
        assert s3_backend.read_json(path) == data

    def test_exists_true(self, s3_backend, minio_prefix):
        path = f"{minio_prefix}/exists.txt"
        s3_backend.write_bytes(b"data", path)
        assert s3_backend.exists(path) is True

    def test_exists_false(self, s3_backend, minio_prefix):
        assert s3_backend.exists(f"{minio_prefix}/nonexistent.txt") is False

    def test_delete(self, s3_backend, minio_prefix):
        path = f"{minio_prefix}/delete_me.txt"
        s3_backend.write_bytes(b"data", path)
        assert s3_backend.exists(path) is True
        s3_backend.delete(path)
        assert s3_backend.exists(path) is False

    def test_write_bytes_returns_s3_uri(self, s3_backend, minio_prefix):
        uri = s3_backend.write_bytes(b"data", f"{minio_prefix}/uri.bin")
        assert uri.startswith("s3://")
        assert uri.endswith(f"{minio_prefix}/uri.bin")


# ---------------------------------------------------------------------------
# DataFrame operations
# ---------------------------------------------------------------------------


class TestS3DataFrame:
    """Test DataFrame read/write in CSV and Parquet formats."""

    @pytest.fixture
    def sample_df(self):
        return pl.DataFrame({
            "naam": ["Jan", "Piet", "Katrijn"],
            "waarde": [1, 42, 7],
            "actief": [True, False, True],
        })

    def test_csv_roundtrip(self, s3_backend, minio_prefix, sample_df):
        path = f"{minio_prefix}/data.csv"
        s3_backend.write_dataframe(sample_df, path)
        result = s3_backend.read_dataframe(path)
        assert result.shape == sample_df.shape
        assert result.columns == sample_df.columns

    def test_parquet_roundtrip(self, s3_backend, minio_prefix, sample_df):
        path = f"{minio_prefix}/data.parquet"
        s3_backend.write_dataframe(sample_df, path)
        result = s3_backend.read_dataframe(path)
        assert result.equals(sample_df)

    def test_csv_semicolon_separator(self, s3_backend, minio_prefix):
        """Verify CSV uses semicolon separator (DUO convention)."""
        df = pl.DataFrame({"a": [1], "b": [2]})
        path = f"{minio_prefix}/semi.csv"
        s3_backend.write_dataframe(df, path)
        raw = s3_backend.read_text(path)
        assert ";" in raw
        assert "," not in raw.replace("1", "").replace("2", "")


# ---------------------------------------------------------------------------
# File listing
# ---------------------------------------------------------------------------


class TestS3ListFiles:
    """Test list_files with glob patterns."""

    def test_list_files_wildcard(self, s3_backend, minio_prefix):
        s3_backend.write_bytes(b"a", f"{minio_prefix}/list/file1.txt")
        s3_backend.write_bytes(b"b", f"{minio_prefix}/list/file2.txt")
        s3_backend.write_bytes(b"c", f"{minio_prefix}/list/file3.csv")
        txt_files = s3_backend.list_files(f"{minio_prefix}/list/*.txt")
        assert len(txt_files) == 2
        assert all(f.endswith(".txt") for f in txt_files)

    def test_list_files_nested(self, s3_backend, minio_prefix):
        s3_backend.write_bytes(b"a", f"{minio_prefix}/nested/sub/file.json")
        files = s3_backend.list_files(f"{minio_prefix}/nested/**/*.json")
        assert len(files) >= 1

    def test_list_files_empty_result(self, s3_backend, minio_prefix):
        files = s3_backend.list_files(f"{minio_prefix}/nonexistent/*.xyz")
        assert files == []


# ---------------------------------------------------------------------------
# @with_storage decorator with S3 env
# ---------------------------------------------------------------------------


class TestS3Decorators:
    """Test that @with_storage injects the S3 backend when env is set."""

    def test_with_storage_uses_s3(self, s3_env, minio_prefix):
        @with_storage
        def store_and_retrieve(storage, path, content):
            storage.write_text(content, path)
            return storage.read_text(path)

        result = store_and_retrieve(f"{minio_prefix}/decorator.txt", "decorator works!")
        assert result == "decorator works!"

    def test_get_backend_returns_s3(self, s3_env):
        backend = get_backend()
        assert isinstance(backend, S3Backend)

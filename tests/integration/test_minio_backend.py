"""
Integration tests for the MinIO storage backend.

Tests basic CRUD operations, DataFrame I/O, and file management against
a real MinIO instance running via docker-compose.

Run with:
    uv run pytest tests/integration/ -v
"""

import io

import polars as pl
import pytest
from openpyxl import Workbook

from eencijferho.core.converter import _load_metadata, process_chunk
from eencijferho.core.extractor import extract_tables_from_txt
from eencijferho.io import get_backend
from eencijferho.io.backends.minio import MinIOBackend
from eencijferho.io.decorators import with_storage
from eencijferho.utils.compressor import convert_csv_to_parquet


# ---------------------------------------------------------------------------
# Basic read/write operations
# ---------------------------------------------------------------------------


class TestMinIOReadWrite:
    """Test fundamental read/write/exists/delete operations."""

    def test_write_and_read_bytes(self, minio_backend, minio_prefix):
        path = f"{minio_prefix}/hello.bin"
        minio_backend.write_bytes(b"hello world", path)
        assert minio_backend.read_bytes(path) == b"hello world"

    def test_write_and_read_text(self, minio_backend, minio_prefix):
        path = f"{minio_prefix}/unicode.txt"
        minio_backend.write_text("héllo wörld café", path)
        assert minio_backend.read_text(path) == "héllo wörld café"

    def test_write_and_read_text_latin1(self, minio_backend, minio_prefix):
        path = f"{minio_prefix}/latin1.txt"
        text = "Vóór het HO - één test"
        minio_backend.write_text(text, path, encoding="latin-1")
        assert minio_backend.read_text(path, encoding="latin-1") == text

    def test_write_and_read_json(self, minio_backend, minio_prefix):
        path = f"{minio_prefix}/data.json"
        data = {"key": "waarde", "lijst": [1, 2, 3], "nested": {"a": True}}
        minio_backend.write_json(data, path)
        assert minio_backend.read_json(path) == data

    def test_exists_true(self, minio_backend, minio_prefix):
        path = f"{minio_prefix}/exists.txt"
        minio_backend.write_bytes(b"data", path)
        assert minio_backend.exists(path) is True

    def test_exists_false(self, minio_backend, minio_prefix):
        assert minio_backend.exists(f"{minio_prefix}/nonexistent.txt") is False

    def test_delete(self, minio_backend, minio_prefix):
        path = f"{minio_prefix}/delete_me.txt"
        minio_backend.write_bytes(b"data", path)
        assert minio_backend.exists(path) is True
        minio_backend.delete(path)
        assert minio_backend.exists(path) is False


# ---------------------------------------------------------------------------
# DataFrame operations
# ---------------------------------------------------------------------------


class TestMinIODataFrame:
    """Test DataFrame read/write in CSV and Parquet formats."""

    @pytest.fixture
    def sample_df(self):
        return pl.DataFrame({
            "naam": ["Jan", "Piet", "Katrijn"],
            "waarde": [1, 42, 7],
            "actief": [True, False, True],
        })

    def test_csv_roundtrip(self, minio_backend, minio_prefix, sample_df):
        path = f"{minio_prefix}/data.csv"
        minio_backend.write_dataframe(sample_df, path)
        result = minio_backend.read_dataframe(path)
        assert result.shape == sample_df.shape
        assert result.columns == sample_df.columns

    def test_parquet_roundtrip(self, minio_backend, minio_prefix, sample_df):
        path = f"{minio_prefix}/data.parquet"
        minio_backend.write_dataframe(sample_df, path)
        result = minio_backend.read_dataframe(path)
        assert result.equals(sample_df)

    def test_csv_semicolon_separator(self, minio_backend, minio_prefix):
        """Verify CSV uses semicolon separator (DUO convention)."""
        df = pl.DataFrame({"a": [1], "b": [2]})
        path = f"{minio_prefix}/semi.csv"
        minio_backend.write_dataframe(df, path)
        raw = minio_backend.read_text(path)
        assert ";" in raw
        assert "," not in raw.replace("1", "").replace("2", "")


# ---------------------------------------------------------------------------
# File listing
# ---------------------------------------------------------------------------


class TestMinIOListFiles:
    """Test list_files with glob patterns."""

    def test_list_files_wildcard(self, minio_backend, minio_prefix):
        minio_backend.write_bytes(b"a", f"{minio_prefix}/list/file1.txt")
        minio_backend.write_bytes(b"b", f"{minio_prefix}/list/file2.txt")
        minio_backend.write_bytes(b"c", f"{minio_prefix}/list/file3.csv")
        txt_files = minio_backend.list_files(f"{minio_prefix}/list/*.txt")
        assert len(txt_files) == 2
        assert all(f.endswith(".txt") for f in txt_files)

    def test_list_files_nested(self, minio_backend, minio_prefix):
        minio_backend.write_bytes(b"a", f"{minio_prefix}/nested/sub/file.json")
        files = minio_backend.list_files(f"{minio_prefix}/nested/**/*.json")
        assert len(files) >= 1

    def test_list_files_empty_result(self, minio_backend, minio_prefix):
        files = minio_backend.list_files(f"{minio_prefix}/nonexistent/*.xyz")
        assert files == []


# ---------------------------------------------------------------------------
# @with_storage decorator with MinIO env
# ---------------------------------------------------------------------------


class TestMinIODecorators:
    """Test that @with_storage injects MinIO backend when env is set."""

    def test_with_storage_uses_minio(self, minio_env, minio_prefix):
        @with_storage
        def store_and_retrieve(storage, path, content):
            storage.write_text(content, path)
            return storage.read_text(path)

        result = store_and_retrieve(f"{minio_prefix}/decorator.txt", "decorator works!")
        assert result == "decorator works!"

    def test_get_backend_returns_minio(self, minio_env):
        backend = get_backend()
        assert isinstance(backend, MinIOBackend)


# ---------------------------------------------------------------------------
# Workflow tests: exercise migrated modules against MinIO
# ---------------------------------------------------------------------------


class TestMinIOExtractorWorkflow:
    """Test extractor functions against MinIO."""

    def _upload_bestandsbeschrijving(self, backend, prefix, title="TestTabel"):
        """Upload a minimal DUO bestandsbeschrijving to MinIO."""
        content = (
            f"{title}\n"
            "================\n"
            "Startpositie  Naam\n"
            "1             VeldA\n"
            "\n"
        )
        path = f"{prefix}/Bestandsbeschrijving_test.txt"
        backend.write_text(content, path, encoding="latin-1")
        return path

    def test_extract_tables_from_txt(self, minio_env, minio_prefix):
        backend = get_backend()
        txt_path = self._upload_bestandsbeschrijving(backend, minio_prefix)
        json_dir = f"{minio_prefix}/json"

        result = extract_tables_from_txt(txt_path, json_dir)

        assert result is not None
        assert result.endswith(".json")
        data = backend.read_json(result)
        assert "tables" in data
        assert len(data["tables"]) == 1

    def test_extract_preserves_accented_title(self, minio_env, minio_prefix):
        backend = get_backend()
        txt_path = self._upload_bestandsbeschrijving(
            backend, minio_prefix, title="Vóór het HO"
        )
        json_dir = f"{minio_prefix}/json"

        result = extract_tables_from_txt(txt_path, json_dir)
        data = backend.read_json(result)
        titles = [t["table_title"] for t in data["tables"]]
        assert any("r het HO" in t for t in titles)


class TestMinIOConverterWorkflow:
    """Test converter functions against MinIO."""

    def test_process_chunk_is_pure(self):
        """process_chunk doesn't use storage — just verify it still works."""
        result = process_chunk(([(0, 5), (5, 10)], [b"abc  def  "]))
        assert result == ["abc;def"]

    def test_load_metadata_from_minio(self, minio_env, minio_prefix):
        """Upload an Excel metadata file to MinIO and load it."""
        backend = get_backend()

        # Create and upload a metadata Excel file using openpyxl
        wb = Workbook()
        ws = wb.active
        ws.append(["ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"])
        ws.append([1, "Veld1", 1, 5, ""])
        ws.append([2, "Veld2", 6, 3, ""])
        buf = io.BytesIO()
        wb.save(buf)
        backend.write_bytes(buf.getvalue(), f"{minio_prefix}/meta.xlsx")

        column_names, positions = _load_metadata(f"{minio_prefix}/meta.xlsx")

        assert column_names == ["Veld1", "Veld2"]
        assert positions == [(0, 5), (5, 8)]


class TestMinIOCompressorWorkflow:
    """Test CSV-to-Parquet compression against MinIO."""

    def test_csv_to_parquet(self, minio_env, minio_prefix):
        backend = get_backend()

        # Upload a CSV file (not starting with "dec" so it won't be skipped)
        df = pl.DataFrame({"naam": ["Jan", "Piet"], "waarde": [1, 2]})
        csv_path = f"{minio_prefix}/EV_test.csv"
        backend.write_dataframe(df, csv_path)

        convert_csv_to_parquet(minio_prefix)

        # Verify parquet was created
        parquet_path = f"{minio_prefix}/EV_test.parquet"
        assert backend.exists(parquet_path)
        result = backend.read_dataframe(parquet_path)
        assert result.shape == (2, 2)

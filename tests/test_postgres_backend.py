"""
Integration tests for PostgreSQL storage backend

These tests require a running PostgreSQL server. They will be skipped if PostgreSQL is not available.
To run these tests, start PostgreSQL with:
    docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres

Then set environment variables:
    export TEST_POSTGRES_HOST=localhost
    export TEST_POSTGRES_DATABASE=postgres
    export TEST_POSTGRES_USER=postgres
    export TEST_POSTGRES_PASSWORD=postgres
"""

import os

import polars as pl
import pytest


def postgres_available():
    """Check if PostgreSQL is available for testing"""
    try:
        import psycopg2

        # Connect to default postgres database to check availability
        conn = psycopg2.connect(
            host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
            port=int(os.getenv("TEST_POSTGRES_PORT", "5432")),
            database="postgres",  # Use default db, not test_db
            user=os.getenv("TEST_POSTGRES_USER", "postgres"),
            password=os.getenv("TEST_POSTGRES_PASSWORD", "postgres"),
        )
        conn.close()
        return True
    except Exception:
        return False


# Skip all tests in this module if PostgreSQL is not available
pytestmark = pytest.mark.skipif(
    not postgres_available(),
    reason="PostgreSQL server not available. Set TEST_POSTGRES_* environment variables and start PostgreSQL."
)


@pytest.fixture
def postgres_backend(postgres_env):
    """Create a PostgreSQL backend for testing"""
    import psycopg2

    # First create the test schema using a direct connection
    config_host = os.getenv("POSTGRES_HOST", "localhost")
    config_port = int(os.getenv("POSTGRES_PORT", "5432"))
    config_db = os.getenv("POSTGRES_DATABASE", "postgres")
    config_user = os.getenv("POSTGRES_USER", "postgres")
    config_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    schema_name = os.getenv("POSTGRES_SCHEMA", "test_schema")

    conn = psycopg2.connect(
        host=config_host,
        port=config_port,
        database=config_db,
        user=config_user,
        password=config_password,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.close()

    from backend.io.backends.postgres import PostgresBackend
    from backend.io.config import PostgresConfig

    config = PostgresConfig()
    backend = PostgresBackend(config)

    yield backend

    # Cleanup: drop test tables and data
    try:
        with backend.conn.cursor() as cur:
            # Drop tables created during tests
            cur.execute(f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{config.schema}'
                AND table_name NOT LIKE '\\_%%'
            """)
            for row in cur.fetchall():
                cur.execute(f"DROP TABLE IF EXISTS {config.schema}.{row[0]} CASCADE")

            # Clear special tables
            cur.execute(f"DELETE FROM {config.schema}.{backend.METADATA_TABLE}")
            cur.execute(f"DELETE FROM {config.schema}.{backend.BINARY_TABLE}")
    except Exception:
        pass
    finally:
        backend.close()


class TestPostgresBackendInit:
    """Tests for PostgresBackend initialization"""

    def test_requires_configuration(self, clean_env, monkeypatch):
        """Test that PostgresBackend raises when not configured"""
        from backend.io.backends.postgres import PostgresBackend

        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        # Missing other required vars

        with pytest.raises(ValueError, match="PostgreSQL backend requires"):
            PostgresBackend()

    def test_creates_special_tables(self, postgres_backend):
        """Test that PostgresBackend creates metadata and binary tables"""
        with postgres_backend.conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_name LIKE '\\_%%'
            """, (postgres_backend.config.schema,))
            tables = [row[0] for row in cur.fetchall()]

        assert postgres_backend.METADATA_TABLE in tables
        assert postgres_backend.BINARY_TABLE in tables


class TestPostgresPathToTableName:
    """Tests for path to table name conversion"""

    def test_simple_path(self, postgres_backend):
        """Test simple path conversion"""
        assert postgres_backend._path_to_table_name("data/test.csv") == "test"

    def test_numbered_directory(self, postgres_backend):
        """Test that numbered directories are handled"""
        assert postgres_backend._path_to_table_name("data/03-combined/file.csv") == "combined_file"

    def test_deep_path(self, postgres_backend):
        """Test deep path conversion"""
        assert postgres_backend._path_to_table_name("02-processed/EV2023.csv") == "processed_ev2023"

    def test_special_characters_removed(self, postgres_backend):
        """Test that special characters are replaced"""
        assert postgres_backend._path_to_table_name("data/file-name.csv") == "file_name"

    def test_starting_with_number(self, postgres_backend):
        """Test that table names starting with numbers are prefixed"""
        name = postgres_backend._path_to_table_name("123file.csv")
        assert name.startswith("t_")


class TestPostgresBackendDataFrameOperations:
    """Tests for DataFrame read/write on PostgreSQL"""

    def test_write_and_read_dataframe(self, postgres_backend, sample_dataframe):
        """Test writing and reading DataFrames"""
        postgres_backend.write_dataframe(sample_dataframe, "test/data.csv")
        df = postgres_backend.read_dataframe("test/data.csv")

        assert df.shape == sample_dataframe.shape
        assert sorted(df.columns) == sorted(sample_dataframe.columns)

    def test_format_is_ignored(self, postgres_backend, sample_dataframe):
        """Test that format parameter is ignored (always stores as table)"""
        # Write with different format - should all work the same
        postgres_backend.write_dataframe(sample_dataframe, "test/data.parquet", format="parquet")
        df = postgres_backend.read_dataframe("test/data.parquet", format="parquet")

        assert df.shape == sample_dataframe.shape


class TestPostgresBackendJsonOperations:
    """Tests for JSON read/write on PostgreSQL"""

    def test_write_and_read_json(self, postgres_backend, sample_json):
        """Test writing and reading JSON to metadata table"""
        postgres_backend.write_json(sample_json, "test/metadata.json")
        data = postgres_backend.read_json("test/metadata.json")

        assert data == sample_json

    def test_json_update(self, postgres_backend):
        """Test that writing to same key updates the value"""
        postgres_backend.write_json({"version": "1.0"}, "test/meta.json")
        postgres_backend.write_json({"version": "2.0"}, "test/meta.json")

        data = postgres_backend.read_json("test/meta.json")
        assert data["version"] == "2.0"

    def test_json_not_found_raises(self, postgres_backend):
        """Test that reading missing JSON raises FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            postgres_backend.read_json("nonexistent/path.json")


class TestPostgresBackendBytesOperations:
    """Tests for binary read/write on PostgreSQL"""

    def test_write_and_read_bytes(self, postgres_backend, sample_bytes):
        """Test writing and reading binary data"""
        postgres_backend.write_bytes(sample_bytes, "test/data.bin")
        data = postgres_backend.read_bytes("test/data.bin")

        assert data == sample_bytes

    def test_bytes_update(self, postgres_backend):
        """Test that writing to same key updates the value"""
        postgres_backend.write_bytes(b"first", "test/data.bin")
        postgres_backend.write_bytes(b"second", "test/data.bin")

        data = postgres_backend.read_bytes("test/data.bin")
        assert data == b"second"

    def test_bytes_not_found_raises(self, postgres_backend):
        """Test that reading missing bytes raises FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            postgres_backend.read_bytes("nonexistent/path.bin")


class TestPostgresBackendExists:
    """Tests for exists() method on PostgreSQL"""

    def test_exists_table(self, postgres_backend, sample_dataframe):
        """Test exists returns True for existing tables"""
        postgres_backend.write_dataframe(sample_dataframe, "test/exists.csv")
        assert postgres_backend.exists("test/exists.csv") is True

    def test_exists_json(self, postgres_backend, sample_json):
        """Test exists returns True for existing JSON keys"""
        postgres_backend.write_json(sample_json, "test/meta.json")
        assert postgres_backend.exists("test/meta.json") is True

    def test_exists_bytes(self, postgres_backend, sample_bytes):
        """Test exists returns True for existing binary keys"""
        postgres_backend.write_bytes(sample_bytes, "test/data.bin")
        assert postgres_backend.exists("test/data.bin") is True

    def test_exists_false(self, postgres_backend):
        """Test exists returns False for missing paths"""
        assert postgres_backend.exists("nonexistent/path") is False


class TestPostgresBackendDelete:
    """Tests for delete() method on PostgreSQL"""

    def test_delete_table(self, postgres_backend, sample_dataframe):
        """Test deleting a table"""
        postgres_backend.write_dataframe(sample_dataframe, "test/delete.csv")
        assert postgres_backend.exists("test/delete.csv")

        result = postgres_backend.delete("test/delete.csv")
        assert result is True
        assert not postgres_backend.exists("test/delete.csv")

    def test_delete_json(self, postgres_backend, sample_json):
        """Test deleting JSON data"""
        postgres_backend.write_json(sample_json, "test/meta.json")
        assert postgres_backend.exists("test/meta.json")

        result = postgres_backend.delete("test/meta.json")
        assert result is True
        assert not postgres_backend.exists("test/meta.json")

    def test_delete_nonexistent(self, postgres_backend):
        """Test deleting nonexistent path"""
        result = postgres_backend.delete("nonexistent")
        assert result is False


class TestPostgresBackendClose:
    """Tests for connection management"""

    def test_close_connection(self, postgres_backend):
        """Test that close() closes the connection"""
        # Connection should be open
        assert not postgres_backend._conn.closed

        postgres_backend.close()
        assert postgres_backend._conn is None

    def test_reconnects_after_close(self, postgres_backend, sample_json):
        """Test that operations work after close (reconnects)"""
        postgres_backend.close()

        # Should reconnect automatically
        postgres_backend.write_json(sample_json, "test/reconnect.json")
        data = postgres_backend.read_json("test/reconnect.json")
        assert data == sample_json

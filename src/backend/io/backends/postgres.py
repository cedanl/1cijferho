"""
PostgreSQL Storage Backend
Backend for storing DataFrames as database tables
"""

import io
import json
import re
from typing import Any

import polars as pl

from .base import StorageBackend
from ..config import PostgresConfig


class PostgresBackend(StorageBackend):
    """
    PostgreSQL storage backend

    Stores DataFrames as database tables. Paths are converted to table names
    using the following convention:
        "data/03-combined/enriched.csv" -> "combined_enriched"
        "data/02-processed/EV2023.csv" -> "processed_ev2023"

    Special tables for non-DataFrame data:
        _metadata_json: JSON key-value storage
        _binary_storage: Binary file storage

    Requires the 'psycopg2-binary' optional dependency.
    """

    # Special tables for non-DataFrame storage
    METADATA_TABLE = "_metadata_json"
    BINARY_TABLE = "_binary_storage"

    def __init__(self, config: PostgresConfig | None = None):
        """
        Initialize PostgreSQL backend

        Args:
            config: PostgreSQL configuration

        Raises:
            ImportError: If psycopg2 is not installed
            ValueError: If configuration is incomplete
        """
        try:
            import psycopg2
            import psycopg2.extras
            self._psycopg2 = psycopg2
        except ImportError:
            raise ImportError(
                "PostgreSQL backend requires the 'psycopg2-binary' package. "
                "Install it with: pip install 1cijferho[postgres]"
            )

        self.config = config or PostgresConfig()

        if not self.config.is_configured():
            raise ValueError(
                "PostgreSQL backend requires POSTGRES_HOST, POSTGRES_DATABASE, "
                "POSTGRES_USER, and POSTGRES_PASSWORD environment variables"
            )

        self._conn = None
        self._ensure_special_tables()

    @property
    def conn(self):
        """Get database connection, creating if needed"""
        if self._conn is None or self._conn.closed:
            self._conn = self._psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
            )
            self._conn.autocommit = True
        return self._conn

    def _ensure_special_tables(self) -> None:
        """Create special tables for JSON and binary storage if they don't exist"""
        with self.conn.cursor() as cur:
            # JSON metadata table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.config.schema}.{self.METADATA_TABLE} (
                    key VARCHAR(500) PRIMARY KEY,
                    value JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Binary storage table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.config.schema}.{self.BINARY_TABLE} (
                    key VARCHAR(500) PRIMARY KEY,
                    data BYTEA NOT NULL,
                    content_type VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def _path_to_table_name(self, path: str) -> str:
        """
        Convert a file path to a PostgreSQL table name

        Examples:
            "data/03-combined/enriched.csv" -> "combined_enriched"
            "data/02-processed/EV2023.csv" -> "processed_ev2023"
            "03-combined/test.parquet" -> "combined_test"

        Args:
            path: File path

        Returns:
            Valid PostgreSQL table name
        """
        # Remove file extension
        path_no_ext = re.sub(r"\.[^.]+$", "", path)

        # Split into parts
        parts = path_no_ext.replace("\\", "/").split("/")

        # Filter out common prefixes and numbers
        filtered_parts = []
        for part in parts:
            # Skip common root directories
            if part.lower() in ("data", ""):
                continue
            # Remove leading numbers and dashes (like "03-")
            clean_part = re.sub(r"^\d+-", "", part)
            if clean_part:
                filtered_parts.append(clean_part.lower())

        # Join with underscore
        table_name = "_".join(filtered_parts)

        # Ensure valid PostgreSQL identifier
        # Replace invalid chars with underscore
        table_name = re.sub(r"[^a-z0-9_]", "_", table_name)
        # Remove consecutive underscores
        table_name = re.sub(r"_+", "_", table_name)
        # Remove leading/trailing underscores
        table_name = table_name.strip("_")

        # Ensure it doesn't start with a number
        if table_name and table_name[0].isdigit():
            table_name = "t_" + table_name

        # Truncate to PostgreSQL's identifier limit
        return table_name[:63] or "unnamed_table"

    def _path_to_key(self, path: str) -> str:
        """Convert path to storage key for JSON/binary tables"""
        return path.replace("\\", "/").lower()

    def get_full_path(self, path: str) -> str:
        """Get table reference for a path"""
        table_name = self._path_to_table_name(path)
        return f"{self.config.schema}.{table_name}"

    def read_dataframe(
        self,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> pl.DataFrame:
        """
        Read DataFrame from PostgreSQL table

        The 'format' parameter is ignored as data is stored in tables.
        """
        table_name = self._path_to_table_name(path)
        full_table = f"{self.config.schema}.{table_name}"

        return pl.read_database_uri(
            query=f'SELECT * FROM {full_table}',
            uri=self.config.get_connection_string(),
        )

    def write_dataframe(
        self,
        df: pl.DataFrame,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> str:
        """
        Write DataFrame to PostgreSQL table

        Creates the table if it doesn't exist, replacing any existing data.
        """
        table_name = self._path_to_table_name(path)
        full_table = f"{self.config.schema}.{table_name}"

        # Write using Polars' database capabilities
        df.write_database(
            table_name=full_table,
            connection=self.config.get_connection_string(),
            if_table_exists="replace",
        )

        return full_table

    def read_json(self, path: str) -> dict:
        """Read JSON from metadata table"""
        key = self._path_to_key(path)

        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT value FROM {self.config.schema}.{self.METADATA_TABLE} WHERE key = %s",
                (key,)
            )
            row = cur.fetchone()

            if row is None:
                raise FileNotFoundError(f"JSON not found for key: {key}")

            return row[0]

    def write_json(self, data: dict, path: str) -> str:
        """Write JSON to metadata table"""
        key = self._path_to_key(path)
        json_data = json.dumps(data)

        with self.conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {self.config.schema}.{self.METADATA_TABLE} (key, value, updated_at)
                VALUES (%s, %s::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (key)
                DO UPDATE SET value = %s::jsonb, updated_at = CURRENT_TIMESTAMP
            """, (key, json_data, json_data))

        return f"{self.config.schema}.{self.METADATA_TABLE}[{key}]"

    def read_bytes(self, path: str) -> bytes:
        """Read binary data from storage table"""
        key = self._path_to_key(path)

        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM {self.config.schema}.{self.BINARY_TABLE} WHERE key = %s",
                (key,)
            )
            row = cur.fetchone()

            if row is None:
                raise FileNotFoundError(f"Binary data not found for key: {key}")

            return bytes(row[0])

    def write_bytes(self, data: bytes, path: str) -> str:
        """Write binary data to storage table"""
        key = self._path_to_key(path)

        # Determine content type from extension
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        content_types = {
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls": "application/vnd.ms-excel",
            "pdf": "application/pdf",
            "txt": "text/plain",
        }
        content_type = content_types.get(ext, "application/octet-stream")

        with self.conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {self.config.schema}.{self.BINARY_TABLE}
                    (key, data, content_type, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (key)
                DO UPDATE SET data = %s, content_type = %s, updated_at = CURRENT_TIMESTAMP
            """, (key, self._psycopg2.Binary(data), content_type,
                  self._psycopg2.Binary(data), content_type))

        return f"{self.config.schema}.{self.BINARY_TABLE}[{key}]"

    def exists(self, path: str) -> bool:
        """Check if table or key exists"""
        # First check if it's a table
        table_name = self._path_to_table_name(path)

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (self.config.schema, table_name))
            if cur.fetchone()[0]:
                return True

            # Check JSON metadata table
            key = self._path_to_key(path)
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM {self.config.schema}.{self.METADATA_TABLE} WHERE key = %s)",
                (key,)
            )
            if cur.fetchone()[0]:
                return True

            # Check binary storage table
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM {self.config.schema}.{self.BINARY_TABLE} WHERE key = %s)",
                (key,)
            )
            return cur.fetchone()[0]

    def list_files(self, path: str, pattern: str = "*") -> list[str]:
        """
        List tables/keys matching pattern

        For PostgreSQL, this lists tables whose names match the pattern
        after path-to-table conversion.
        """
        from fnmatch import fnmatch

        # Convert pattern to table name pattern
        table_prefix = self._path_to_table_name(path)
        # Convert glob pattern to SQL LIKE pattern base
        pattern_base = pattern.replace("*", "%").replace("?", "_")
        pattern_clean = pattern.replace("**/", "").replace("**", "*")

        results = []

        with self.conn.cursor() as cur:
            # List tables
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name LIKE %s
                AND table_name NOT LIKE '\\_%%'
            """, (self.config.schema, f"{table_prefix}%"))

            for row in cur.fetchall():
                table_name = row[0]
                # Convert table name back to path-like format
                pseudo_path = f"{path.rstrip('/')}/{table_name}.csv"
                if fnmatch(table_name, pattern_clean.replace("*", "*")):
                    results.append(pseudo_path)

        return sorted(results)

    def makedirs(self, path: str) -> None:
        """
        Create schema if needed

        In PostgreSQL, directories map to schemas.
        This is mostly a no-op as we use a single schema.
        """
        # Ensure schema exists
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema}")

    def delete(self, path: str) -> bool:
        """Delete table or key"""
        table_name = self._path_to_table_name(path)

        with self.conn.cursor() as cur:
            # Try to drop table
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (self.config.schema, table_name))

            if cur.fetchone()[0]:
                cur.execute(f"DROP TABLE IF EXISTS {self.config.schema}.{table_name}")
                return True

            # Try to delete from metadata
            key = self._path_to_key(path)
            cur.execute(
                f"DELETE FROM {self.config.schema}.{self.METADATA_TABLE} WHERE key = %s",
                (key,)
            )
            if cur.rowcount > 0:
                return True

            # Try to delete from binary storage
            cur.execute(
                f"DELETE FROM {self.config.schema}.{self.BINARY_TABLE} WHERE key = %s",
                (key,)
            )
            return cur.rowcount > 0

    def close(self) -> None:
        """Close database connection"""
        if hasattr(self, "_conn") and self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def __del__(self):
        """Clean up connection on deletion"""
        self.close()

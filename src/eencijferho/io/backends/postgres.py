"""PostgreSQL storage backend — stores DataFrames as tables, files as BLOBs."""

from __future__ import annotations

import re
from io import BytesIO

import polars as pl

from eencijferho.io.backends.base import StorageBackend


def _path_to_table(path: str) -> str:
    """Convert a file path to a valid PostgreSQL table name.

    Examples:
        "01-input/student_data.csv"  -> "input_student_data"
        "02-output/results.parquet"  -> "output_results"
    """
    name = re.sub(r"\.[^.]+$", "", path)
    name = re.sub(r"(\b|/)\d{2}-", "/", name)
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name[:63]


class PostgresBackend(StorageBackend):
    """Store DataFrames as PostgreSQL tables and binary data in a storage table."""

    BINARY_TABLE = "_binary_storage"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "cijferho",
        user: str = "postgres",
        password: str = "postgres",
    ):
        try:
            import psycopg2  # noqa: F401
        except ImportError:
            raise ImportError("Install 'psycopg2-binary' package: pip install psycopg2-binary")

        self._conn_params = dict(host=host, port=port, database=database, user=user, password=password)
        self._conn = None
        self._connstr = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self._ensure_binary_table()

    @property
    def conn(self):
        import psycopg2

        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self._conn_params)
            self._conn.autocommit = True
        return self._conn

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()

    JSON_TABLE = "_json_storage"

    def _ensure_binary_table(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.BINARY_TABLE} (
                    path TEXT PRIMARY KEY,
                    data BYTEA NOT NULL
                )
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.JSON_TABLE} (
                    path TEXT PRIMARY KEY,
                    data JSONB NOT NULL
                )
            """)

    def read_bytes(self, path: str) -> bytes:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT data FROM {self.BINARY_TABLE} WHERE path = %s", (path,))
            row = cur.fetchone()
            if row is None:
                raise FileNotFoundError(f"No binary data stored at: {path}")
            return bytes(row[0])

    def write_bytes(self, data: bytes, path: str) -> str:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.BINARY_TABLE} (path, data) VALUES (%s, %s)
                ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data
                """,
                (path, data),
            )
        return f"pg://{path}"

    def read_json(self, path: str) -> dict | list:
        import json as _json

        with self.conn.cursor() as cur:
            cur.execute(f"SELECT data FROM {self.JSON_TABLE} WHERE path = %s", (path,))
            row = cur.fetchone()
        if row is not None:
            data = row[0]
            return _json.loads(data) if isinstance(data, str) else data
        # Fallback to binary storage
        return _json.loads(self.read_bytes(path))

    def write_json(self, data: dict | list, path: str, **kwargs) -> str:
        import json as _json

        raw = _json.dumps(data, ensure_ascii=False)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.JSON_TABLE} (path, data) VALUES (%s, %s::jsonb)
                ON CONFLICT (path) DO UPDATE SET data = EXCLUDED.data
                """,
                (path, raw),
            )
        return f"pg://{path}"

    def read_dataframe(self, path: str, format: str | None = None, **kwargs) -> pl.DataFrame:
        table = _path_to_table(path)
        return pl.read_database(f"SELECT * FROM {table}", self._connstr)

    def write_dataframe(self, df: pl.DataFrame, path: str, format: str | None = None, **kwargs) -> str:
        table = _path_to_table(path)
        from sqlalchemy import create_engine

        engine = create_engine(self._connstr)
        pdf = df.to_pandas()
        pdf.to_sql(table, engine, if_exists="replace", index=False)
        engine.dispose()
        return f"pg://{table}"

    def list_files(self, pattern: str) -> list[str]:
        """List tables that match a pattern (converted to table name pattern)."""
        import fnmatch

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name NOT LIKE '\\_%'
            """)
            tables = [row[0] for row in cur.fetchall()]

        table_pattern = _path_to_table(pattern.replace("*", "WILDCARD")).replace("WILDCARD", "*")
        return [t for t in tables if fnmatch.fnmatch(t, table_pattern)]

    def exists(self, path: str) -> bool:
        table = _path_to_table(path)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                )
                """,
                (table,),
            )
            if cur.fetchone()[0]:
                return True

        with self.conn.cursor() as cur:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {self.BINARY_TABLE} WHERE path = %s)", (path,))
            return cur.fetchone()[0]

    def delete(self, path: str) -> None:
        table = _path_to_table(path)
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self.BINARY_TABLE} WHERE path = %s", (path,))
            cur.execute(f"DELETE FROM {self.JSON_TABLE} WHERE path = %s", (path,))
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)",
                (table,),
            )
            if cur.fetchone()[0]:
                cur.execute(f"DROP TABLE IF EXISTS {table}")

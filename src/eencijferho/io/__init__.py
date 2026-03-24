"""Storage abstraction layer for 1CijferHO.

Provides pluggable backends (disk, MinIO, PostgreSQL) selectable via
the STORAGE_BACKEND environment variable. Default is "disk".

Usage:
    from eencijferho.io import get_backend, storage_context

    # Option 1: context manager (auto-cleanup)
    with storage_context() as storage:
        df = storage.read_dataframe("01-input/data.csv")
        storage.write_dataframe(df, "02-output/result.parquet")

    # Option 2: direct
    storage = get_backend()
    df = storage.read_dataframe("01-input/data.csv")

    # Option 3: decorators
    from eencijferho.io.decorators import reads_from, writes_to

    @reads_from("01-input/data.csv")
    def process(df):
        return df.filter(...)

    @writes_to("02-output/result.parquet")
    def produce():
        return some_dataframe
"""

from contextlib import contextmanager

from eencijferho.io.config import StorageConfig
from eencijferho.io.backends.base import StorageBackend
from eencijferho.io.backends.disk import DiskBackend

__all__ = [
    "StorageBackend",
    "StorageConfig",
    "get_backend",
    "storage_context",
]


def get_backend(backend_type: str | None = None) -> StorageBackend:
    """Factory: return a configured storage backend.

    Args:
        backend_type: "disk", "minio", or "postgres". If None, reads
                      from STORAGE_BACKEND env var (default: "disk").
    """
    config = StorageConfig.from_env()
    backend = backend_type or config.backend

    if backend == "disk":
        return DiskBackend(base_path=config.disk_base_path)
    elif backend == "minio":
        from eencijferho.io.backends.minio import MinIOBackend

        return MinIOBackend(
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            bucket=config.minio_bucket,
            secure=config.minio_secure,
        )
    elif backend == "postgres":
        from eencijferho.io.backends.postgres import PostgresBackend

        return PostgresBackend(
            host=config.postgres_host,
            port=config.postgres_port,
            database=config.postgres_database,
            user=config.postgres_user,
            password=config.postgres_password,
        )
    else:
        raise ValueError(f"Unknown storage backend: {backend!r}")


@contextmanager
def storage_context(backend_type: str | None = None):
    """Context manager that yields a backend and cleans up on exit."""
    backend = get_backend(backend_type)
    try:
        yield backend
    finally:
        if hasattr(backend, "close"):
            backend.close()

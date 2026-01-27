"""
Storage Context Manager
Provides a context manager for easy access to storage backends
"""

from contextlib import contextmanager
from typing import Iterator

from .backends import StorageBackend, get_backend


@contextmanager
def storage_context(backend_type: str | None = None) -> Iterator[StorageBackend]:
    """
    Context manager for storage operations

    Provides access to the configured storage backend within a context.
    Useful for complex patterns requiring multiple storage operations.

    Usage:
        with storage_context() as storage:
            files = storage.list_files("03-combined", "*.csv")
            for f in files:
                df = storage.read_dataframe(f)
                # process...
                storage.write_dataframe(df, f"output/{f}")

        # Override backend type:
        with storage_context("minio") as storage:
            ...

    Args:
        backend_type: Override storage backend type (defaults to STORAGE_BACKEND env var)

    Yields:
        Configured storage backend instance
    """
    backend = get_backend(backend_type)
    try:
        yield backend
    finally:
        # Clean up if backend has close method (e.g., PostgreSQL)
        if hasattr(backend, "close"):
            backend.close()


def get_storage(backend_type: str | None = None) -> StorageBackend:
    """
    Get storage backend instance without context management

    Use this when you need persistent access to storage across multiple
    function calls. Remember to close the backend when done if needed.

    Usage:
        storage = get_storage()
        df = storage.read_dataframe("03-combined/data.csv")
        # ... more operations ...

    Args:
        backend_type: Override storage backend type

    Returns:
        Configured storage backend instance
    """
    return get_backend(backend_type)

"""
Storage Backends
Provides pluggable storage implementations for disk, MinIO, and PostgreSQL
"""

from .base import StorageBackend
from .disk import DiskBackend

# Lazy imports for optional backends
__all__ = ["StorageBackend", "DiskBackend", "get_backend"]


def get_backend(backend_type: str = None) -> StorageBackend:
    """
    Factory function to get the appropriate storage backend

    Args:
        backend_type: Override backend type (defaults to STORAGE_BACKEND env var)

    Returns:
        Configured storage backend instance
    """
    from ..config import get_config

    config = get_config()
    backend = backend_type or config.backend

    if backend == "disk":
        return DiskBackend(config.disk)
    elif backend == "minio":
        from .minio import MinIOBackend
        return MinIOBackend(config.minio)
    elif backend == "postgres":
        from .postgres import PostgresBackend
        return PostgresBackend(config.postgres)
    else:
        raise ValueError(f"Unknown storage backend: {backend}")

"""Environment-driven configuration for storage backends."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class StorageConfig:
    """All storage configuration, read from environment variables."""

    # Backend selection
    backend: str = "disk"

    # Disk
    disk_base_path: str = "data"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "1cijferho"
    minio_secure: bool = False

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "cijferho"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"

    @classmethod
    def from_env(cls) -> StorageConfig:
        """Create config from environment variables."""
        return cls(
            backend=os.getenv("STORAGE_BACKEND", "disk"),
            disk_base_path=os.getenv("STORAGE_DISK_BASE_PATH", "data"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            minio_bucket=os.getenv("MINIO_BUCKET", "1cijferho"),
            minio_secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_database=os.getenv("POSTGRES_DATABASE", "cijferho"),
            postgres_user=os.getenv("POSTGRES_USER", "postgres"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )

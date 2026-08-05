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
    disk_base_path: str = "."

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "1cijferho"
    minio_secure: bool = False

    # S3 (generic — SURF Ceph RADOS-Gateway, AWS, or any S3-compatible store)
    s3_endpoint: str = ""  # e.g. "https://object.surfsara.nl"; empty = AWS default
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "1cijferho"
    s3_region: str = "us-east-1"  # RADOS-GW accepts any; boto3 needs one set
    s3_secure: bool = True
    s3_path_style: bool = True  # RADOS-GW requires path-style; AWS tolerates it

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
            disk_base_path=os.getenv("STORAGE_DISK_BASE_PATH", "."),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            minio_bucket=os.getenv("MINIO_BUCKET", "1cijferho"),
            minio_secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            # S3 keys fall back to standard AWS env vars so IAM roles / AWS
            # credential files keep working; empty defers to boto3's chain.
            s3_endpoint=os.getenv("S3_ENDPOINT", ""),
            s3_access_key=os.getenv("S3_ACCESS_KEY", os.getenv("AWS_ACCESS_KEY_ID", "")),
            s3_secret_key=os.getenv("S3_SECRET_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "")),
            s3_bucket=os.getenv("S3_BUCKET", "1cijferho"),
            s3_region=os.getenv("S3_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
            s3_secure=os.getenv("S3_SECURE", "true").lower() == "true",
            s3_path_style=os.getenv("S3_PATH_STYLE", "true").lower() == "true",
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_database=os.getenv("POSTGRES_DATABASE", "cijferho"),
            postgres_user=os.getenv("POSTGRES_USER", "postgres"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )

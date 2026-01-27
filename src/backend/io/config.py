"""
Storage Configuration Module
Handles environment-based configuration for storage backends
"""

import os
from dataclasses import dataclass, field
from typing import Literal


StorageBackendType = Literal["disk", "minio", "postgres"]


@dataclass
class PathConfig:
    """Configuration for default I/O paths used by decorators"""
    default_input: str = field(default_factory=lambda: os.getenv("STORAGE_DEFAULT_INPUT", ""))
    default_output: str = field(default_factory=lambda: os.getenv("STORAGE_DEFAULT_OUTPUT", ""))

    # Pipeline stage directories (relative to base_path)
    metadata_dir: str = field(default_factory=lambda: os.getenv("STORAGE_METADATA_DIR", "00-metadata"))
    input_dir: str = field(default_factory=lambda: os.getenv("STORAGE_INPUT_DIR", "01-input"))
    processed_dir: str = field(default_factory=lambda: os.getenv("STORAGE_PROCESSED_DIR", "02-processed"))
    combined_dir: str = field(default_factory=lambda: os.getenv("STORAGE_COMBINED_DIR", "03-combined"))
    enriched_dir: str = field(default_factory=lambda: os.getenv("STORAGE_ENRICHED_DIR", "04-enriched"))
    reference_dir: str = field(default_factory=lambda: os.getenv("STORAGE_REFERENCE_DIR", "reference"))


@dataclass
class DiskConfig:
    """Configuration for disk-based storage"""
    base_path: str = field(default_factory=lambda: os.getenv("STORAGE_DISK_BASE_PATH", "data"))


@dataclass
class MinIOConfig:
    """Configuration for MinIO/S3 storage"""
    endpoint: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", ""))
    access_key: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", ""))
    secret_key: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", ""))
    bucket: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET", ""))
    secure: bool = field(default_factory=lambda: os.getenv("MINIO_SECURE", "true").lower() == "true")

    def is_configured(self) -> bool:
        """Check if MinIO is properly configured"""
        return bool(self.endpoint and self.access_key and self.secret_key and self.bucket)


@dataclass
class PostgresConfig:
    """Configuration for PostgreSQL storage"""
    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", ""))
    port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    database: str = field(default_factory=lambda: os.getenv("POSTGRES_DATABASE", ""))
    user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", ""))
    password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", ""))
    schema: str = field(default_factory=lambda: os.getenv("POSTGRES_SCHEMA", "public"))

    def is_configured(self) -> bool:
        """Check if PostgreSQL is properly configured"""
        return bool(self.host and self.database and self.user and self.password)

    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class StorageConfig:
    """Main storage configuration"""
    backend: StorageBackendType = field(
        default_factory=lambda: os.getenv("STORAGE_BACKEND", "disk")  # type: ignore
    )
    disk: DiskConfig = field(default_factory=DiskConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    paths: PathConfig = field(default_factory=PathConfig)

    def validate(self) -> None:
        """Validate configuration for selected backend"""
        if self.backend == "minio" and not self.minio.is_configured():
            raise ValueError(
                "MinIO backend selected but not fully configured. "
                "Required: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET"
            )
        if self.backend == "postgres" and not self.postgres.is_configured():
            raise ValueError(
                "PostgreSQL backend selected but not fully configured. "
                "Required: POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_PASSWORD"
            )


def get_config() -> StorageConfig:
    """Get storage configuration from environment"""
    config = StorageConfig()
    config.validate()
    return config

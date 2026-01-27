"""
Storage Abstraction Layer for 1CijferHO
Enables seamless switching between disk, MinIO, and PostgreSQL backends

Usage:
    # Using context manager (recommended for complex patterns)
    from backend.io import storage_context

    with storage_context() as storage:
        files = storage.list_files("03-combined", "*.csv")
        for f in files:
            df = storage.read_dataframe(f)
            # process...
            storage.write_dataframe(df, f"output/{f}")

    # Using decorators
    from backend.io import data_input, data_output, data_io

    @data_input(path="03-combined/*.csv")
    def analyze_data(df):
        return df.describe()

    @data_output(path="04-enriched/results.parquet")
    def create_summary(df):
        return df.group_by(...).agg(...)

    @data_io(input_path="03-combined/*.csv", output_path="04-enriched/enriched.csv")
    def enrich_dataframe(df):
        # transform...
        return df

Environment Variables:
    STORAGE_BACKEND: "disk" (default), "minio", or "postgres"
    STORAGE_DISK_BASE_PATH: Base path for disk storage (default: "data")
    STORAGE_DEFAULT_INPUT: Default input path for decorators
    STORAGE_DEFAULT_OUTPUT: Default output path for decorators

    Pipeline stage directories (relative to base path):
    STORAGE_METADATA_DIR: Metadata directory (default: "00-metadata")
    STORAGE_INPUT_DIR: Input directory (default: "01-input")
    STORAGE_PROCESSED_DIR: Processed directory (default: "02-processed")
    STORAGE_COMBINED_DIR: Combined directory (default: "03-combined")
    STORAGE_ENRICHED_DIR: Enriched directory (default: "04-enriched")
    STORAGE_REFERENCE_DIR: Reference directory (default: "reference")

    MINIO_ENDPOINT: MinIO server endpoint
    MINIO_ACCESS_KEY: MinIO access key
    MINIO_SECRET_KEY: MinIO secret key
    MINIO_BUCKET: Default bucket name
    MINIO_SECURE: Use HTTPS (default: "true")

    POSTGRES_HOST: PostgreSQL host
    POSTGRES_PORT: PostgreSQL port (default: 5432)
    POSTGRES_DATABASE: Database name
    POSTGRES_USER: Database user
    POSTGRES_PASSWORD: Database password
    POSTGRES_SCHEMA: Default schema (default: "public")
"""

from .config import StorageConfig, PathConfig, get_config
from .backends import StorageBackend, DiskBackend, get_backend
from .context import storage_context
from .decorators import data_input, data_output, data_io

__all__ = [
    # Config
    "StorageConfig",
    "PathConfig",
    "get_config",
    # Backends
    "StorageBackend",
    "DiskBackend",
    "get_backend",
    # Context manager
    "storage_context",
    # Decorators
    "data_input",
    "data_output",
    "data_io",
]

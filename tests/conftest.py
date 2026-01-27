"""
Pytest configuration and shared fixtures for storage abstraction tests
"""

import os
import tempfile
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing"""
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.3, 30.1],
    })


@pytest.fixture
def sample_json():
    """Create sample JSON data for testing"""
    return {
        "version": "1.0",
        "metadata": {
            "created": "2024-01-01",
            "author": "test"
        },
        "items": [1, 2, 3]
    }


@pytest.fixture
def sample_bytes():
    """Create sample binary data for testing"""
    return b"Hello, this is binary data for testing!"


@pytest.fixture
def clean_env(monkeypatch):
    """Fixture to ensure clean environment variables for testing"""
    # Clear storage-related env vars
    env_vars_to_clear = [
        "STORAGE_BACKEND",
        "STORAGE_DISK_BASE_PATH",
        "STORAGE_DEFAULT_INPUT",
        "STORAGE_DEFAULT_OUTPUT",
        "STORAGE_METADATA_DIR",
        "STORAGE_INPUT_DIR",
        "STORAGE_PROCESSED_DIR",
        "STORAGE_COMBINED_DIR",
        "STORAGE_ENRICHED_DIR",
        "STORAGE_REFERENCE_DIR",
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_BUCKET",
        "MINIO_SECURE",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DATABASE",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_SCHEMA",
    ]
    for var in env_vars_to_clear:
        monkeypatch.delenv(var, raising=False)
    yield


@pytest.fixture
def disk_backend_env(temp_dir, monkeypatch):
    """Configure environment for disk backend testing"""
    monkeypatch.setenv("STORAGE_BACKEND", "disk")
    monkeypatch.setenv("STORAGE_DISK_BASE_PATH", str(temp_dir))
    yield temp_dir


@pytest.fixture
def minio_env(monkeypatch):
    """Configure environment for MinIO backend testing (requires running MinIO)"""
    # Only set if MinIO is available (for integration tests)
    monkeypatch.setenv("STORAGE_BACKEND", "minio")
    monkeypatch.setenv("MINIO_ENDPOINT", os.getenv("TEST_MINIO_ENDPOINT", "localhost:9000"))
    monkeypatch.setenv("MINIO_ACCESS_KEY", os.getenv("TEST_MINIO_ACCESS_KEY", "minioadmin"))
    monkeypatch.setenv("MINIO_SECRET_KEY", os.getenv("TEST_MINIO_SECRET_KEY", "minioadmin"))
    monkeypatch.setenv("MINIO_BUCKET", os.getenv("TEST_MINIO_BUCKET", "test-bucket"))
    monkeypatch.setenv("MINIO_SECURE", "false")
    yield


@pytest.fixture
def postgres_env(monkeypatch):
    """Configure environment for PostgreSQL backend testing (requires running PostgreSQL)"""
    monkeypatch.setenv("STORAGE_BACKEND", "postgres")
    monkeypatch.setenv("POSTGRES_HOST", os.getenv("TEST_POSTGRES_HOST", "localhost"))
    monkeypatch.setenv("POSTGRES_PORT", os.getenv("TEST_POSTGRES_PORT", "5432"))
    monkeypatch.setenv("POSTGRES_DATABASE", os.getenv("TEST_POSTGRES_DATABASE", "postgres"))
    monkeypatch.setenv("POSTGRES_USER", os.getenv("TEST_POSTGRES_USER", "postgres"))
    monkeypatch.setenv("POSTGRES_PASSWORD", os.getenv("TEST_POSTGRES_PASSWORD", "postgres"))
    monkeypatch.setenv("POSTGRES_SCHEMA", "test_schema")
    yield

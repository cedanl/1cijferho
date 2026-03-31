"""
Fixtures for MinIO integration tests.

Start/stop MinIO via docker-compose and provide a configured backend.
Tests in this directory are skipped when Docker or MinIO is unavailable.
"""

import subprocess
import time
import uuid

import pytest


def _docker_available() -> bool:
    """Check if docker compose is reachable."""
    try:
        r = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True, timeout=5,
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _minio_healthy() -> bool:
    """Check if the minio container is healthy."""
    try:
        r = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", "minio"],
            capture_output=True, text=True, timeout=5,
        )
        return "healthy" in r.stdout
    except Exception:
        return False


def _wait_for_minio(timeout: int = 30) -> bool:
    """Wait until MinIO is healthy or timeout."""
    for _ in range(timeout):
        if _minio_healthy():
            return True
        time.sleep(1)
    return False


# ---------------------------------------------------------------------------
# Session-scoped: start MinIO once for all integration tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def minio_service():
    """Ensure MinIO is running via docker-compose. Tear down after session."""
    if not _docker_available():
        pytest.skip("Docker not available")

    already_running = _minio_healthy()

    if not already_running:
        result = subprocess.run(
            ["docker", "compose", "up", "-d", "minio", "minio-init"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            pytest.skip(f"Could not start MinIO: {result.stderr}")

    if not _wait_for_minio():
        pytest.skip("MinIO did not become healthy in time")

    yield

    # Only tear down if we started it
    if not already_running:
        subprocess.run(
            ["docker", "compose", "stop", "minio", "minio-init"],
            capture_output=True, timeout=30,
        )


# ---------------------------------------------------------------------------
# Test-scoped: fresh backend with isolated prefix per test
# ---------------------------------------------------------------------------

TEST_BUCKET = "1cijferho-test"


@pytest.fixture
def minio_backend(minio_service):
    """Return a MinIOBackend connected to the test MinIO instance.

    Uses a dedicated test bucket that is auto-created.
    """
    from eencijferho.io.backends.minio import MinIOBackend

    backend = MinIOBackend(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket=TEST_BUCKET,
        secure=False,
    )
    return backend


@pytest.fixture
def minio_prefix():
    """Return a unique prefix string to isolate test data within the bucket."""
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def minio_env(minio_service, monkeypatch):
    """Set env vars so get_backend() returns a MinIO backend for the test bucket."""
    monkeypatch.setenv("STORAGE_BACKEND", "minio")
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_BUCKET", TEST_BUCKET)
    monkeypatch.setenv("MINIO_SECURE", "false")

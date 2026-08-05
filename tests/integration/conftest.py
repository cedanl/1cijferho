"""
Fixtures for MinIO integration tests.

Start/stop MinIO via docker-compose and provide a configured backend.
Tests in this directory are skipped when Docker or MinIO is unavailable.
"""

import subprocess
import time
import uuid

import pytest

try:
    import minio as _minio  # noqa: F401
except ImportError:
    pytest.skip("minio package not installed", allow_module_level=True)


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
    """Check if MinIO is responding to HTTP health checks."""
    try:
        r = subprocess.run(
            ["curl", "-sf", "http://localhost:9000/minio/health/live"],
            capture_output=True, timeout=5,
        )
        return r.returncode == 0
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


# ---------------------------------------------------------------------------
# S3 backend — boto3 against either the local MinIO container (default) or a
# real S3 endpoint (e.g. SURF RADOS-Gateway) selected via S3_TEST_* env vars.
# ---------------------------------------------------------------------------

S3_TEST_BUCKET = "1cijferho-s3-test"


def _s3_target(request):
    """Resolve the S3 test target.

    If ``S3_TEST_ENDPOINT`` is set, tests run against that real endpoint
    (SURF RADOS-GW / AWS) using ``S3_TEST_*`` credentials — no local MinIO
    container is required. Otherwise they fall back to the docker-compose
    MinIO container (MinIO is S3-compatible), which is started on demand.
    """
    import os

    endpoint = os.getenv("S3_TEST_ENDPOINT")
    if endpoint:
        return {
            "endpoint": endpoint,
            "access_key": os.getenv("S3_TEST_ACCESS_KEY", ""),
            "secret_key": os.getenv("S3_TEST_SECRET_KEY", ""),
            "bucket": os.getenv("S3_TEST_BUCKET", S3_TEST_BUCKET),
            "region": os.getenv("S3_TEST_REGION", "us-east-1"),
            "secure": os.getenv("S3_TEST_SECURE", "true").lower() == "true",
            "path_style": os.getenv("S3_TEST_PATH_STYLE", "true").lower() == "true",
        }

    # No real endpoint — use the local MinIO container.
    request.getfixturevalue("minio_service")
    return {
        "endpoint": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket": S3_TEST_BUCKET,
        "region": "us-east-1",
        "secure": False,
        "path_style": True,
    }


@pytest.fixture
def s3_backend(request):
    """Return an S3Backend (boto3) connected to the resolved S3 target.

    Target is the local MinIO container by default, or a real endpoint when
    ``S3_TEST_ENDPOINT`` is set. Uses a dedicated bucket, auto-created.
    """
    pytest.importorskip("boto3")
    from eencijferho.io.backends.s3 import S3Backend

    return S3Backend(**_s3_target(request))


@pytest.fixture
def s3_env(request, monkeypatch):
    """Set env vars so get_backend() returns an S3 backend for the test target."""
    pytest.importorskip("boto3")
    target = _s3_target(request)
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("S3_ENDPOINT", target["endpoint"])
    monkeypatch.setenv("S3_ACCESS_KEY", target["access_key"])
    monkeypatch.setenv("S3_SECRET_KEY", target["secret_key"])
    monkeypatch.setenv("S3_BUCKET", target["bucket"])
    monkeypatch.setenv("S3_REGION", target["region"])
    monkeypatch.setenv("S3_SECURE", "true" if target["secure"] else "false")
    monkeypatch.setenv("S3_PATH_STYLE", "true" if target["path_style"] else "false")


# ---------------------------------------------------------------------------
# PostgreSQL — used by the personal-data store (needs MinIO + Postgres together)
# ---------------------------------------------------------------------------

def _postgres_available() -> bool:
    """Check if a Postgres instance is reachable with the test credentials."""
    try:
        import psycopg2
    except ImportError:
        return False
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, dbname="cijferho",
            user="postgres", password="postgres", connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture
def personal_data_env(minio_service, monkeypatch):
    """Point both MinIO and Postgres backends at the local test instances.

    Skips when Postgres is unavailable — the personal-data store splits records
    across both, so it needs a live Postgres in addition to MinIO.
    """
    if not _postgres_available():
        pytest.skip("PostgreSQL not available")
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_BUCKET", TEST_BUCKET)
    monkeypatch.setenv("MINIO_SECURE", "false")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DATABASE", "cijferho")
    monkeypatch.setenv("POSTGRES_USER", "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", "postgres")

"""
Tests for storage configuration module
"""

import pytest


class TestPathConfig:
    """Tests for PathConfig class"""

    def test_default_values(self, clean_env):
        """Test that PathConfig uses correct defaults"""
        from backend.io.config import PathConfig

        config = PathConfig()

        assert config.default_input == ""
        assert config.default_output == ""
        assert config.metadata_dir == "00-metadata"
        assert config.input_dir == "01-input"
        assert config.processed_dir == "02-processed"
        assert config.combined_dir == "03-combined"
        assert config.enriched_dir == "04-enriched"
        assert config.reference_dir == "reference"

    def test_env_overrides(self, clean_env, monkeypatch):
        """Test that PathConfig reads from environment variables"""
        from backend.io.config import PathConfig

        monkeypatch.setenv("STORAGE_DEFAULT_INPUT", "custom-input")
        monkeypatch.setenv("STORAGE_DEFAULT_OUTPUT", "custom-output")
        monkeypatch.setenv("STORAGE_METADATA_DIR", "custom-metadata")
        monkeypatch.setenv("STORAGE_INPUT_DIR", "custom-input-dir")

        config = PathConfig()

        assert config.default_input == "custom-input"
        assert config.default_output == "custom-output"
        assert config.metadata_dir == "custom-metadata"
        assert config.input_dir == "custom-input-dir"


class TestDiskConfig:
    """Tests for DiskConfig class"""

    def test_default_base_path(self, clean_env):
        """Test that DiskConfig uses 'data' as default base path"""
        from backend.io.config import DiskConfig

        config = DiskConfig()
        assert config.base_path == "data"

    def test_env_override(self, clean_env, monkeypatch):
        """Test that DiskConfig reads STORAGE_DISK_BASE_PATH"""
        from backend.io.config import DiskConfig

        monkeypatch.setenv("STORAGE_DISK_BASE_PATH", "/custom/path")
        config = DiskConfig()
        assert config.base_path == "/custom/path"


class TestMinIOConfig:
    """Tests for MinIOConfig class"""

    def test_default_values(self, clean_env):
        """Test that MinIOConfig has empty defaults"""
        from backend.io.config import MinIOConfig

        config = MinIOConfig()

        assert config.endpoint == ""
        assert config.access_key == ""
        assert config.secret_key == ""
        assert config.bucket == ""
        assert config.secure is True

    def test_is_configured_false_when_missing(self, clean_env):
        """Test is_configured returns False when required fields missing"""
        from backend.io.config import MinIOConfig

        config = MinIOConfig()
        assert config.is_configured() is False

    def test_is_configured_true_when_complete(self, clean_env, monkeypatch):
        """Test is_configured returns True when all required fields present"""
        from backend.io.config import MinIOConfig

        monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "admin")
        monkeypatch.setenv("MINIO_SECRET_KEY", "secret")
        monkeypatch.setenv("MINIO_BUCKET", "mybucket")

        config = MinIOConfig()
        assert config.is_configured() is True

    def test_secure_false(self, clean_env, monkeypatch):
        """Test secure flag can be set to false"""
        from backend.io.config import MinIOConfig

        monkeypatch.setenv("MINIO_SECURE", "false")
        config = MinIOConfig()
        assert config.secure is False


class TestPostgresConfig:
    """Tests for PostgresConfig class"""

    def test_default_values(self, clean_env):
        """Test that PostgresConfig has correct defaults"""
        from backend.io.config import PostgresConfig

        config = PostgresConfig()

        assert config.host == ""
        assert config.port == 5432
        assert config.database == ""
        assert config.user == ""
        assert config.password == ""
        assert config.schema == "public"

    def test_is_configured_false_when_missing(self, clean_env):
        """Test is_configured returns False when required fields missing"""
        from backend.io.config import PostgresConfig

        config = PostgresConfig()
        assert config.is_configured() is False

    def test_is_configured_true_when_complete(self, clean_env, monkeypatch):
        """Test is_configured returns True when all required fields present"""
        from backend.io.config import PostgresConfig

        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_DATABASE", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "admin")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

        config = PostgresConfig()
        assert config.is_configured() is True

    def test_connection_string(self, clean_env, monkeypatch):
        """Test connection string generation"""
        from backend.io.config import PostgresConfig

        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_DATABASE", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "admin")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

        config = PostgresConfig()
        assert config.get_connection_string() == "postgresql://admin:secret@localhost:5433/mydb"


class TestStorageConfig:
    """Tests for StorageConfig class"""

    def test_default_backend_is_disk(self, clean_env):
        """Test that default backend is disk"""
        from backend.io.config import StorageConfig

        config = StorageConfig()
        assert config.backend == "disk"

    def test_backend_from_env(self, clean_env, monkeypatch):
        """Test backend can be set via environment"""
        from backend.io.config import StorageConfig

        monkeypatch.setenv("STORAGE_BACKEND", "minio")
        config = StorageConfig()
        assert config.backend == "minio"

    def test_validate_disk_always_valid(self, clean_env):
        """Test that disk backend validation always passes"""
        from backend.io.config import StorageConfig

        config = StorageConfig()
        config.validate()  # Should not raise

    def test_validate_minio_raises_when_not_configured(self, clean_env, monkeypatch):
        """Test that MinIO backend validation fails when not configured"""
        from backend.io.config import StorageConfig

        monkeypatch.setenv("STORAGE_BACKEND", "minio")
        config = StorageConfig()

        with pytest.raises(ValueError, match="MinIO backend selected but not fully configured"):
            config.validate()

    def test_validate_postgres_raises_when_not_configured(self, clean_env, monkeypatch):
        """Test that PostgreSQL backend validation fails when not configured"""
        from backend.io.config import StorageConfig

        monkeypatch.setenv("STORAGE_BACKEND", "postgres")
        config = StorageConfig()

        with pytest.raises(ValueError, match="PostgreSQL backend selected but not fully configured"):
            config.validate()


class TestGetConfig:
    """Tests for get_config function"""

    def test_returns_storage_config(self, clean_env):
        """Test that get_config returns a StorageConfig instance"""
        from backend.io.config import get_config, StorageConfig

        config = get_config()
        assert isinstance(config, StorageConfig)

    def test_validates_config(self, clean_env, monkeypatch):
        """Test that get_config validates the configuration"""
        from backend.io.config import get_config

        monkeypatch.setenv("STORAGE_BACKEND", "minio")

        with pytest.raises(ValueError):
            get_config()

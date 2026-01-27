"""
MinIO/S3 Storage Backend
Backend for S3-compatible object storage using MinIO client
"""

import io
import json
from fnmatch import fnmatch
from typing import Any

import polars as pl

from .base import StorageBackend
from ..config import MinIOConfig


class MinIOBackend(StorageBackend):
    """
    MinIO/S3-compatible storage backend

    Uses the MinIO Python client for all storage operations.
    Requires the 'minio' optional dependency.
    """

    def __init__(self, config: MinIOConfig | None = None):
        """
        Initialize MinIO backend

        Args:
            config: MinIO configuration (uses defaults if not provided)

        Raises:
            ImportError: If minio package is not installed
            ValueError: If configuration is incomplete
        """
        try:
            from minio import Minio
        except ImportError:
            raise ImportError(
                "MinIO backend requires the 'minio' package. "
                "Install it with: pip install 1cijferho[minio]"
            )

        self.config = config or MinIOConfig()

        if not self.config.is_configured():
            raise ValueError(
                "MinIO backend requires MINIO_ENDPOINT, MINIO_ACCESS_KEY, "
                "MINIO_SECRET_KEY, and MINIO_BUCKET environment variables"
            )

        self.client = Minio(
            self.config.endpoint,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            secure=self.config.secure,
        )
        self.bucket = self.config.bucket

        # Ensure bucket exists
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def _normalize_path(self, path: str) -> str:
        """
        Normalize path for S3 (no leading slash)

        Args:
            path: Input path

        Returns:
            Normalized path
        """
        return path.lstrip("/")

    def get_full_path(self, path: str) -> str:
        """Get full S3 URI for a path"""
        return f"s3://{self.bucket}/{self._normalize_path(path)}"

    def read_dataframe(
        self,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> pl.DataFrame:
        """Read DataFrame from MinIO"""
        obj_path = self._normalize_path(path)
        response = self.client.get_object(self.bucket, obj_path)

        try:
            data = response.read()
        finally:
            response.close()
            response.release_conn()

        buffer = io.BytesIO(data)

        if format == "csv":
            return pl.read_csv(buffer, **kwargs)
        elif format == "parquet":
            return pl.read_parquet(buffer, **kwargs)
        elif format == "json":
            return pl.read_json(buffer, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def write_dataframe(
        self,
        df: pl.DataFrame,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> str:
        """Write DataFrame to MinIO"""
        obj_path = self._normalize_path(path)
        buffer = io.BytesIO()

        if format == "csv":
            df.write_csv(buffer, **kwargs)
            content_type = "text/csv"
        elif format == "parquet":
            df.write_parquet(buffer, **kwargs)
            content_type = "application/octet-stream"
        elif format == "json":
            df.write_json(buffer, **kwargs)
            content_type = "application/json"
        else:
            raise ValueError(f"Unsupported format: {format}")

        buffer.seek(0)
        data = buffer.getvalue()

        self.client.put_object(
            self.bucket,
            obj_path,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

        return self.get_full_path(path)

    def read_json(self, path: str) -> dict:
        """Read JSON from MinIO"""
        obj_path = self._normalize_path(path)
        response = self.client.get_object(self.bucket, obj_path)

        try:
            data = response.read()
        finally:
            response.close()
            response.release_conn()

        return json.loads(data.decode("utf-8"))

    def write_json(self, data: dict, path: str) -> str:
        """Write JSON to MinIO"""
        obj_path = self._normalize_path(path)
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")

        self.client.put_object(
            self.bucket,
            obj_path,
            io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json",
        )

        return self.get_full_path(path)

    def read_bytes(self, path: str) -> bytes:
        """Read raw bytes from MinIO"""
        obj_path = self._normalize_path(path)
        response = self.client.get_object(self.bucket, obj_path)

        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def write_bytes(self, data: bytes, path: str) -> str:
        """Write raw bytes to MinIO"""
        obj_path = self._normalize_path(path)

        self.client.put_object(
            self.bucket,
            obj_path,
            io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )

        return self.get_full_path(path)

    def exists(self, path: str) -> bool:
        """Check if object exists in MinIO"""
        obj_path = self._normalize_path(path)

        try:
            self.client.stat_object(self.bucket, obj_path)
            return True
        except Exception:
            # Check if it's a "directory" (prefix with objects)
            objects = list(self.client.list_objects(
                self.bucket,
                prefix=obj_path.rstrip("/") + "/",
                recursive=False,
            ))
            return len(objects) > 0

    def list_files(self, path: str, pattern: str = "*") -> list[str]:
        """
        List files matching pattern in MinIO

        Args:
            path: Prefix path to list
            pattern: Glob pattern to filter results (applied to filename only)

        Returns:
            List of matching object paths
        """
        prefix = self._normalize_path(path)
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        recursive = "**" in pattern or "/" in pattern

        objects = self.client.list_objects(
            self.bucket,
            prefix=prefix,
            recursive=recursive,
        )

        results = []
        for obj in objects:
            if obj.is_dir:
                continue

            # Get filename for pattern matching
            obj_name = obj.object_name
            filename = obj_name.split("/")[-1]

            # Apply pattern filter
            if fnmatch(filename, pattern.replace("**/", "").replace("**", "*")):
                results.append(obj_name)

        return sorted(results)

    def makedirs(self, path: str) -> None:
        """
        Create directory marker in MinIO

        In S3/MinIO, directories don't really exist - they're just prefixes.
        This is a no-op but included for interface consistency.
        """
        # S3 doesn't have real directories, so this is essentially a no-op
        pass

    def delete(self, path: str) -> bool:
        """Delete object from MinIO"""
        obj_path = self._normalize_path(path)

        # Check if object exists first (S3 remove_object doesn't error on missing objects)
        try:
            self.client.stat_object(self.bucket, obj_path)
        except Exception:
            return False

        try:
            self.client.remove_object(self.bucket, obj_path)
            return True
        except Exception:
            return False

"""MinIO (S3-compatible) storage backend."""

from __future__ import annotations

from io import BytesIO

import polars as pl

from eencijferho.io.backends.base import StorageBackend


class MinIOBackend(StorageBackend):
    """Read/write data via MinIO S3-compatible object storage."""

    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket: str = "1cijferho",
        secure: bool = False,
    ):
        try:
            from minio import Minio
        except ImportError:
            raise ImportError("Install 'minio' package: pip install minio")

        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self.bucket = bucket
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    @staticmethod
    def _normalize_key(path: str) -> str:
        """Strip leading slashes for S3 keys."""
        return path.lstrip("/")

    def read_bytes(self, path: str) -> bytes:
        key = self._normalize_key(path)
        response = self.client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def write_bytes(self, data: bytes, path: str) -> str:
        key = self._normalize_key(path)
        buf = BytesIO(data)
        self.client.put_object(self.bucket, key, buf, length=len(data))
        return f"s3://{self.bucket}/{key}"

    def read_dataframe(self, path: str, format: str | None = None, **kwargs) -> pl.DataFrame:
        fmt = format or self.detect_format(path)
        data = self.read_bytes(path)
        buf = BytesIO(data)

        if fmt == "csv":
            kwargs.setdefault("separator", ";")
            return pl.read_csv(buf, **kwargs)
        elif fmt == "parquet":
            return pl.read_parquet(buf, **kwargs)
        elif fmt == "excel":
            return pl.read_excel(buf, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

    def write_dataframe(self, df: pl.DataFrame, path: str, format: str | None = None, **kwargs) -> str:
        fmt = format or self.detect_format(path)
        buf = BytesIO()

        if fmt == "csv":
            kwargs.setdefault("separator", ";")
            df.write_csv(buf, **kwargs)
        elif fmt == "parquet":
            df.write_parquet(buf, **kwargs)
        elif fmt == "excel":
            df.write_excel(buf, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        raw = buf.getvalue()
        key = self._normalize_key(path)
        return self.write_bytes(raw, key)

    def list_files(self, pattern: str) -> list[str]:
        """List objects matching a glob pattern (prefix + fnmatch filter)."""
        import fnmatch

        prefix = pattern.split("*")[0] if "*" in pattern else pattern
        prefix = self._normalize_key(prefix)

        objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        all_keys = [obj.object_name for obj in objects]

        normalized_pattern = self._normalize_key(pattern)
        return [k for k in all_keys if fnmatch.fnmatch(k, normalized_pattern)]

    def exists(self, path: str) -> bool:
        key = self._normalize_key(path)
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except Exception:
            return False

    def delete(self, path: str) -> None:
        key = self._normalize_key(path)
        self.client.remove_object(self.bucket, key)

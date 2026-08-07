"""MinIO (S3-compatible) storage backend."""

from __future__ import annotations

from eencijferho.io.backends.objectstore import ObjectStoreBackend


class MinIOBackend(ObjectStoreBackend):
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

    def read_bytes(self, path: str) -> bytes:
        key = self._normalize_key(path)
        response = self.client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def write_bytes(self, data: bytes, path: str) -> str:
        from io import BytesIO

        key = self._normalize_key(path)
        buf = BytesIO(data)
        self.client.put_object(self.bucket, key, buf, length=len(data))
        return f"s3://{self.bucket}/{key}"

    def _list_keys(self, prefix: str) -> list[str]:
        objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]

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

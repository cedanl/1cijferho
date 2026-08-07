"""Generic S3 storage backend (SURF Ceph RADOS-Gateway, AWS, or any S3-compatible store).

Uses boto3 rather than the ``minio`` client so non-MinIO endpoints work
cleanly: RADOS-Gateway needs path-style addressing and an explicit region /
SigV4, which boto3 handles via botocore config.
"""

from __future__ import annotations

from eencijferho.io.backends.objectstore import ObjectStoreBackend


class S3Backend(ObjectStoreBackend):
    """Read/write data via a generic S3-compatible object store."""

    def __init__(
        self,
        endpoint: str = "",
        access_key: str = "",
        secret_key: str = "",
        bucket: str = "1cijferho",
        region: str = "us-east-1",
        secure: bool = True,
        path_style: bool = True,
        connect_timeout: int = 10,
        max_attempts: int = 2,
    ):
        try:
            import boto3
            from botocore.config import Config
        except ImportError:
            raise ImportError("Install 'boto3' package: pip install boto3")

        # Cap connect time + retries so an unreachable endpoint (e.g. a
        # VPN/firewall-gated store hit from a runner without network access)
        # fails in seconds rather than hanging through boto3's long default
        # retry loop. Read timeout is left at boto's default so large object
        # transfers aren't cut short.
        config = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if path_style else "auto"},
            connect_timeout=connect_timeout,
            retries={"max_attempts": max_attempts, "mode": "standard"},
        )

        # Empty keys defer to boto3's default credential chain (env vars,
        # AWS credential files, IAM roles). A scheme is only prepended when an
        # endpoint is given; AWS uses the SDK default (endpoint_url=None).
        # `secure` controls TLS: production (SURF/AWS) uses secure=True → https;
        # secure=False is only for local dev stores (e.g. a MinIO container).
        endpoint_url = None
        if endpoint:
            if "://" in endpoint:
                endpoint_url = endpoint  # caller supplied an explicit scheme
            else:
                scheme = "https" if secure else "http"  # NOSONAR: http only when secure=False (local dev)
                endpoint_url = f"{scheme}://{endpoint}"

        client_kwargs = {
            "endpoint_url": endpoint_url,
            "region_name": region,
            "config": config,
        }
        if access_key and secret_key:
            client_kwargs["aws_access_key_id"] = access_key
            client_kwargs["aws_secret_access_key"] = secret_key

        self.client = boto3.client("s3", **client_kwargs)
        self.bucket = bucket
        self._ensure_bucket()

    def _ensure_bucket(self):
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)

    def read_bytes(self, path: str) -> bytes:
        key = self._normalize_key(path)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def write_bytes(self, data: bytes, path: str) -> str:
        key = self._normalize_key(path)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"

    def _list_keys(self, prefix: str) -> list[str]:
        all_keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            all_keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return all_keys

    def exists(self, path: str) -> bool:
        from botocore.exceptions import ClientError

        key = self._normalize_key(path)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def delete(self, path: str) -> None:
        key = self._normalize_key(path)
        self.client.delete_object(Bucket=self.bucket, Key=key)

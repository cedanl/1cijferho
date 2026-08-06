"""Generic S3 storage backend (SURF Ceph RADOS-Gateway, AWS, or any S3-compatible store).

Uses boto3 rather than the ``minio`` client so non-MinIO endpoints work
cleanly: RADOS-Gateway needs path-style addressing and an explicit region /
SigV4, which boto3 handles via botocore config.
"""

from __future__ import annotations

from io import BytesIO

import polars as pl

from eencijferho.io.backends.base import StorageBackend


class S3Backend(StorageBackend):
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
        endpoint_url = None
        if endpoint:
            if endpoint.startswith(("http://", "https://")):
                endpoint_url = endpoint
            else:
                endpoint_url = f"{'https' if secure else 'http'}://{endpoint}"

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

    @staticmethod
    def _normalize_key(path: str) -> str:
        """Strip leading slashes for S3 keys."""
        return path.lstrip("/")

    def read_bytes(self, path: str) -> bytes:
        key = self._normalize_key(path)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def write_bytes(self, data: bytes, path: str) -> str:
        key = self._normalize_key(path)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
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
        """List objects matching a glob pattern (prefix + fnmatch/pathlib filter).

        Supports ``**`` for recursive directory matching (zero or more segments),
        consistent with pathlib.Path.glob() used by the disk backend.
        """
        import fnmatch

        prefix = pattern.split("*")[0] if "*" in pattern else pattern
        prefix = self._normalize_key(prefix)

        all_keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            all_keys.extend(obj["Key"] for obj in page.get("Contents", []))

        normalized_pattern = self._normalize_key(pattern)

        if "**" in normalized_pattern:
            # fnmatch doesn't handle ** (zero-or-more directories) correctly.
            # Expand ** to match both "dir/**/file" and "dir/file" cases by
            # also testing the pattern with ** replaced by a single *.
            flat_pattern = normalized_pattern.replace("/**/", "/")
            return [
                k for k in all_keys
                if fnmatch.fnmatch(k, normalized_pattern)
                or fnmatch.fnmatch(k, flat_pattern)
            ]

        return [k for k in all_keys if fnmatch.fnmatch(k, normalized_pattern)]

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

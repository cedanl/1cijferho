"""Shared logic for S3-compatible object-store backends (MinIO, generic S3).

Both the ``minio`` client and ``boto3`` speak the same object model — keys in a
bucket — and differ only in how bytes are transferred and how objects are
listed. This mixin holds everything that is identical between them: key
normalisation, DataFrame (de)serialisation via an in-memory buffer, and glob
matching over a flat key list. Concrete backends implement the small set of
client-specific primitives below.
"""

from __future__ import annotations

import fnmatch
from io import BytesIO

import polars as pl

from eencijferho.io.backends.base import StorageBackend


class ObjectStoreBackend(StorageBackend):
    """Bucket/key storage. Subclasses provide the client-specific transfer.

    Required of subclasses:
      - ``self.bucket`` set in ``__init__``
      - ``read_bytes`` / ``write_bytes`` (the raw transfer)
      - ``_list_keys(prefix)`` yielding every object key under ``prefix``
    """

    bucket: str

    @staticmethod
    def _normalize_key(path: str) -> str:
        """Strip leading slashes so paths become clean S3 keys."""
        return path.lstrip("/")

    def _list_keys(self, prefix: str) -> list[str]:
        """Return all object keys under ``prefix``. Implemented per client."""
        raise NotImplementedError

    def read_dataframe(self, path: str, format: str | None = None, **kwargs) -> pl.DataFrame:
        fmt = format or self.detect_format(path)
        buf = BytesIO(self.read_bytes(path))

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

        return self.write_bytes(buf.getvalue(), self._normalize_key(path))

    def list_files(self, pattern: str) -> list[str]:
        """List objects matching a glob pattern (prefix + fnmatch filter).

        Supports ``**`` for recursive directory matching (zero or more segments),
        consistent with pathlib.Path.glob() used by the disk backend.
        """
        prefix = pattern.split("*")[0] if "*" in pattern else pattern
        all_keys = self._list_keys(self._normalize_key(prefix))

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

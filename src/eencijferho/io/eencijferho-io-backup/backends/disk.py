"""Local filesystem storage backend."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from eencijferho.io.backends.base import StorageBackend


class DiskBackend(StorageBackend):
    """Read/write files on the local filesystem."""

    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)

    def _resolve(self, path: str) -> Path:
        """Resolve a path: absolute paths pass through, relative paths are joined with base_path."""
        p = Path(path)
        if p.is_absolute():
            return p
        return self.base_path / p

    def read_bytes(self, path: str) -> bytes:
        return self._resolve(path).read_bytes()

    def write_bytes(self, data: bytes, path: str) -> str:
        resolved = self._resolve(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_bytes(data)
        return str(resolved)

    def read_dataframe(self, path: str, format: str | None = None, **kwargs) -> pl.DataFrame:
        resolved = self._resolve(path)
        fmt = format or self.detect_format(str(resolved))

        if fmt == "csv":
            kwargs.setdefault("separator", ";")
            kwargs.setdefault("encoding", "utf8")
            return pl.read_csv(resolved, **kwargs)
        elif fmt == "parquet":
            return pl.read_parquet(resolved, **kwargs)
        elif fmt == "excel":
            return pl.read_excel(resolved, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

    def write_dataframe(self, df: pl.DataFrame, path: str, format: str | None = None, **kwargs) -> str:
        resolved = self._resolve(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        fmt = format or self.detect_format(str(resolved))

        if fmt == "csv":
            kwargs.setdefault("separator", ";")
            df.write_csv(resolved, **kwargs)
        elif fmt == "parquet":
            df.write_parquet(resolved, **kwargs)
        elif fmt == "excel":
            df.write_excel(resolved, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {fmt}")
        return str(resolved)

    def list_files(self, pattern: str) -> list[str]:
        matches = sorted(self.base_path.glob(pattern))
        return [str(m.relative_to(self.base_path)) for m in matches if m.is_file()]

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()

"""
Disk Storage Backend
Default backend using local filesystem operations
"""

import json
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import polars as pl

from .base import StorageBackend
from ..config import DiskConfig


class DiskBackend(StorageBackend):
    """
    Disk-based storage backend

    Uses the local filesystem for all storage operations.
    This is the default backend and matches the original behavior of the pipeline.
    """

    def __init__(self, config: DiskConfig | None = None):
        """
        Initialize disk backend

        Args:
            config: Disk configuration (uses defaults if not provided)
        """
        self.config = config or DiskConfig()
        self.base_path = Path(self.config.base_path)

    def _resolve_path(self, path: str) -> Path:
        """
        Resolve a relative path to absolute path

        Args:
            path: Relative path

        Returns:
            Absolute path
        """
        p = Path(path)
        if p.is_absolute():
            return p
        return self.base_path / path

    def get_full_path(self, path: str) -> str:
        """Get full path for a relative path"""
        return str(self._resolve_path(path))

    def read_dataframe(
        self,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> pl.DataFrame:
        """Read DataFrame from disk"""
        full_path = self._resolve_path(path)

        if format == "csv":
            return pl.read_csv(full_path, **kwargs)
        elif format == "parquet":
            return pl.read_parquet(full_path, **kwargs)
        elif format == "json":
            return pl.read_json(full_path, **kwargs)
        elif format == "excel":
            return pl.read_excel(full_path, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def write_dataframe(
        self,
        df: pl.DataFrame,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> str:
        """Write DataFrame to disk"""
        full_path = self._resolve_path(path)

        # Ensure parent directory exists
        full_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "csv":
            df.write_csv(full_path, **kwargs)
        elif format == "parquet":
            df.write_parquet(full_path, **kwargs)
        elif format == "json":
            df.write_json(full_path, **kwargs)
        elif format == "excel":
            df.write_excel(full_path, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {format}")

        return str(full_path)

    def read_json(self, path: str) -> dict:
        """Read JSON from disk"""
        full_path = self._resolve_path(path)
        with open(full_path) as f:
            return json.load(f)

    def write_json(self, data: dict, path: str) -> str:
        """Write JSON to disk"""
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return str(full_path)

    def read_bytes(self, path: str) -> bytes:
        """Read raw bytes from disk"""
        full_path = self._resolve_path(path)
        return full_path.read_bytes()

    def write_bytes(self, data: bytes, path: str) -> str:
        """Write raw bytes to disk"""
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)
        return str(full_path)

    def exists(self, path: str) -> bool:
        """Check if path exists on disk"""
        full_path = self._resolve_path(path)
        return full_path.exists()

    def list_files(self, path: str, pattern: str = "*") -> list[str]:
        """
        List files matching pattern in directory

        Supports glob patterns like "*.csv", "**/*.parquet"
        """
        full_path = self._resolve_path(path)

        if not full_path.exists():
            return []

        if not full_path.is_dir():
            # If path is a file that matches pattern, return it
            if fnmatch(full_path.name, pattern):
                return [str(full_path)]
            return []

        # Use glob for pattern matching
        if "**" in pattern:
            matches = list(full_path.glob(pattern))
        else:
            matches = list(full_path.glob(pattern))

        # Return paths relative to base_path for consistency
        result = []
        for match in matches:
            if match.is_file():
                try:
                    rel_path = match.relative_to(self.base_path)
                    result.append(str(rel_path))
                except ValueError:
                    # Path is not relative to base_path, return absolute
                    result.append(str(match))

        return sorted(result)

    def makedirs(self, path: str) -> None:
        """Create directory and parents"""
        full_path = self._resolve_path(path)
        full_path.mkdir(parents=True, exist_ok=True)

    def delete(self, path: str) -> bool:
        """Delete file from disk"""
        full_path = self._resolve_path(path)
        if full_path.exists():
            full_path.unlink()
            return True
        return False

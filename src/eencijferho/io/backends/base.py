"""Abstract base class for storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


class StorageBackend(ABC):
    """Interface that all storage backends must implement."""

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Read raw bytes from a path."""

    @abstractmethod
    def write_bytes(self, data: bytes, path: str) -> str:
        """Write raw bytes to a path. Returns the resolved path/URI."""

    @abstractmethod
    def read_dataframe(self, path: str, format: str | None = None, **kwargs) -> pl.DataFrame:
        """Read a DataFrame from a path.

        Args:
            path: File path or identifier.
            format: "csv", "parquet", or "excel". Auto-detected from extension if None.
            **kwargs: Passed to the underlying reader (e.g. separator for CSV).
        """

    @abstractmethod
    def write_dataframe(self, df: pl.DataFrame, path: str, format: str | None = None, **kwargs) -> str:
        """Write a DataFrame to a path.

        Returns the resolved path/URI where data was written.
        """

    @abstractmethod
    def list_files(self, pattern: str) -> list[str]:
        """List files matching a glob pattern."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a path exists."""

    def delete(self, path: str) -> None:
        """Delete a file at path. Default raises NotImplementedError."""
        raise NotImplementedError(f"{type(self).__name__} does not support delete")

    def read_json(self, path: str) -> dict | list:
        """Read a JSON file. Default: read_bytes + json.loads."""
        import json

        return json.loads(self.read_bytes(path))

    def write_json(self, data: dict | list, path: str, **kwargs) -> str:
        """Write a JSON file. Default: json.dumps + write_bytes."""
        import json

        kwargs.setdefault("indent", 2)
        kwargs.setdefault("ensure_ascii", False)
        raw = json.dumps(data, **kwargs).encode("utf-8")
        return self.write_bytes(raw, path)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read a text file with a given encoding."""
        return self.read_bytes(path).decode(encoding)

    def write_text(self, text: str, path: str, encoding: str = "utf-8") -> str:
        """Write a text file with a given encoding."""
        return self.write_bytes(text.encode(encoding), path)

    def detect_format(self, path: str) -> str:
        """Infer format from file extension."""
        path_lower = path.lower()
        if path_lower.endswith(".parquet"):
            return "parquet"
        elif path_lower.endswith((".xlsx", ".xls")):
            return "excel"
        elif path_lower.endswith(".csv"):
            return "csv"
        elif path_lower.endswith(".json"):
            return "json"
        raise ValueError(f"Cannot detect format from path: {path}")

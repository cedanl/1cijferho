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

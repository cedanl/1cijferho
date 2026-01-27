"""
Abstract Storage Backend Interface
Defines the contract for all storage implementations
"""

from abc import ABC, abstractmethod
from typing import Any

import polars as pl


class StorageBackend(ABC):
    """
    Abstract base class for storage backends

    All storage backends must implement these methods to provide
    a consistent interface for reading and writing data.
    """

    @abstractmethod
    def read_dataframe(
        self,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> pl.DataFrame:
        """
        Read a DataFrame from storage

        Args:
            path: Path to the file (relative to base path)
            format: File format ("csv", "parquet", "json")
            **kwargs: Additional arguments passed to the reader

        Returns:
            Polars DataFrame
        """
        pass

    @abstractmethod
    def write_dataframe(
        self,
        df: pl.DataFrame,
        path: str,
        format: str = "csv",
        **kwargs: Any
    ) -> str:
        """
        Write a DataFrame to storage

        Args:
            df: Polars DataFrame to write
            path: Destination path (relative to base path)
            format: File format ("csv", "parquet", "json")
            **kwargs: Additional arguments passed to the writer

        Returns:
            Full path where data was written
        """
        pass

    @abstractmethod
    def read_json(self, path: str) -> dict:
        """
        Read JSON data from storage

        Args:
            path: Path to the JSON file

        Returns:
            Parsed JSON as dictionary
        """
        pass

    @abstractmethod
    def write_json(self, data: dict, path: str) -> str:
        """
        Write JSON data to storage

        Args:
            data: Dictionary to write as JSON
            path: Destination path

        Returns:
            Full path where data was written
        """
        pass

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """
        Read raw bytes from storage

        Args:
            path: Path to the file

        Returns:
            File contents as bytes
        """
        pass

    @abstractmethod
    def write_bytes(self, data: bytes, path: str) -> str:
        """
        Write raw bytes to storage

        Args:
            data: Bytes to write
            path: Destination path

        Returns:
            Full path where data was written
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if a path exists in storage

        Args:
            path: Path to check

        Returns:
            True if path exists, False otherwise
        """
        pass

    @abstractmethod
    def list_files(self, path: str, pattern: str = "*") -> list[str]:
        """
        List files in a directory matching a pattern

        Args:
            path: Directory path to list
            pattern: Glob pattern to match (e.g., "*.csv")

        Returns:
            List of matching file paths
        """
        pass

    @abstractmethod
    def makedirs(self, path: str) -> None:
        """
        Create directories if they don't exist

        Args:
            path: Directory path to create
        """
        pass

    @abstractmethod
    def delete(self, path: str) -> bool:
        """
        Delete a file from storage

        Args:
            path: Path to delete

        Returns:
            True if deleted, False if not found
        """
        pass

    def get_full_path(self, path: str) -> str:
        """
        Get the full path for a given relative path

        Default implementation returns the path unchanged.
        Backends may override to prepend base paths.

        Args:
            path: Relative path

        Returns:
            Full path
        """
        return path

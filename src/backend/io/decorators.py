"""
Storage Decorators
Decorators for declarative data I/O in pipeline functions

Environment Variables:
    STORAGE_DEFAULT_INPUT: Default input path for @data_input and @data_io
    STORAGE_DEFAULT_OUTPUT: Default output path for @data_output and @data_io
"""

import os
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import polars as pl

from .backends import get_backend
from .config import get_config


def _get_default_input() -> str:
    """Get default input path from environment"""
    return os.getenv("STORAGE_DEFAULT_INPUT", "")


def _get_default_output() -> str:
    """Get default output path from environment"""
    return os.getenv("STORAGE_DEFAULT_OUTPUT", "")


def _infer_format(path: str) -> str:
    """Infer file format from path extension"""
    ext = Path(path).suffix.lower().lstrip(".")
    return {
        "csv": "csv",
        "parquet": "parquet",
        "pq": "parquet",
        "json": "json",
        "xlsx": "excel",
        "xls": "excel",
    }.get(ext, "csv")


def _expand_glob_pattern(storage, path: str) -> list[str]:
    """Expand glob patterns in path to list of files"""
    if "*" in path:
        # Split into directory and pattern
        parts = path.rsplit("/", 1)
        if len(parts) == 2:
            directory, pattern = parts
        else:
            directory, pattern = ".", parts[0]

        return storage.list_files(directory, pattern)
    return [path]


def data_input(
    path: str | None = None,
    format: str | None = None,
    concat: bool = True,
    **read_kwargs: Any
) -> Callable:
    """
    Decorator that reads data and passes it to the decorated function

    The decorated function receives the loaded DataFrame as its first argument.

    Usage:
        @data_input(path="03-combined/*.csv")
        def analyze_data(df):
            return df.describe()

        @data_input(path="03-combined/data.parquet", format="parquet")
        def process_data(df):
            return df.filter(...)

        # Using environment variable default (STORAGE_DEFAULT_INPUT)
        @data_input()
        def process_default(df):
            return df

    Args:
        path: Path to read from (supports glob patterns like "*.csv").
              If None, uses STORAGE_DEFAULT_INPUT environment variable.
        format: File format (auto-detected from extension if not specified)
        concat: If True and path is a glob pattern, concatenate all matched files
        **read_kwargs: Additional arguments passed to the reader

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Resolve path at call time (not decoration time) for env var flexibility
            resolved_path = path if path is not None else _get_default_input()
            if not resolved_path:
                raise ValueError(
                    "No input path specified. Either pass 'path' argument or set "
                    "STORAGE_DEFAULT_INPUT environment variable."
                )

            storage = get_backend()
            file_format = format or _infer_format(resolved_path)

            files = _expand_glob_pattern(storage, resolved_path)

            if not files:
                raise FileNotFoundError(f"No files found matching: {resolved_path}")

            if len(files) == 1:
                df = storage.read_dataframe(files[0], format=file_format, **read_kwargs)
            elif concat:
                dfs = [
                    storage.read_dataframe(f, format=file_format, **read_kwargs)
                    for f in files
                ]
                df = pl.concat(dfs)
            else:
                # Return list of dataframes
                df = [
                    storage.read_dataframe(f, format=file_format, **read_kwargs)
                    for f in files
                ]

            return func(df, *args, **kwargs)

        return wrapper
    return decorator


def data_output(
    path: str | None = None,
    format: str | None = None,
    **write_kwargs: Any
) -> Callable:
    """
    Decorator that writes the function's return value to storage

    The decorated function should return a DataFrame (or dict for JSON).

    Usage:
        @data_output(path="04-enriched/results.parquet")
        def create_summary(df):
            return df.group_by("year").agg(...)

        @data_output(path="04-enriched/metadata.json")
        def create_metadata():
            return {"version": "1.0", "columns": [...]}

        # Using environment variable default (STORAGE_DEFAULT_OUTPUT)
        @data_output()
        def create_default_output():
            return pl.DataFrame(...)

    Args:
        path: Path to write to. If None, uses STORAGE_DEFAULT_OUTPUT environment variable.
        format: File format (auto-detected from extension if not specified)
        **write_kwargs: Additional arguments passed to the writer

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Resolve path at call time
            resolved_path = path if path is not None else _get_default_output()
            if not resolved_path:
                raise ValueError(
                    "No output path specified. Either pass 'path' argument or set "
                    "STORAGE_DEFAULT_OUTPUT environment variable."
                )

            result = func(*args, **kwargs)

            storage = get_backend()
            file_format = format or _infer_format(resolved_path)

            if isinstance(result, pl.DataFrame):
                storage.write_dataframe(result, resolved_path, format=file_format, **write_kwargs)
            elif isinstance(result, dict):
                storage.write_json(result, resolved_path)
            elif isinstance(result, bytes):
                storage.write_bytes(result, resolved_path)
            else:
                raise TypeError(
                    f"Unsupported return type for data_output: {type(result)}. "
                    "Expected DataFrame, dict, or bytes."
                )

            return result

        return wrapper
    return decorator


def data_io(
    input_path: str | None = None,
    output_path: str | None = None,
    input_format: str | None = None,
    output_format: str | None = None,
    concat: bool = True,
    **kwargs: Any
) -> Callable:
    """
    Decorator that reads input, calls function, and writes output

    Combines @data_input and @data_output into a single decorator.
    The decorated function receives a DataFrame and should return a DataFrame.

    Usage:
        @data_io(
            input_path="03-combined/*.csv",
            output_path="04-enriched/enriched.csv"
        )
        def enrich_dataframe(df):
            df = add_uitwonend(df)
            df = add_indicatie_voltijd(df)
            return df

        # Using environment variable defaults
        @data_io()
        def process_with_defaults(df):
            return df

    Args:
        input_path: Path to read from (supports glob patterns).
                   If None, uses STORAGE_DEFAULT_INPUT environment variable.
        output_path: Path to write to.
                    If None, uses STORAGE_DEFAULT_OUTPUT environment variable.
        input_format: Input file format (auto-detected if not specified)
        output_format: Output file format (auto-detected if not specified)
        concat: If True and input_path is glob, concatenate matched files
        **kwargs: Split into read_* and write_* prefixed kwargs

    Returns:
        Decorated function
    """
    # Split kwargs into read and write kwargs
    read_kwargs = {k[5:]: v for k, v in kwargs.items() if k.startswith("read_")}
    write_kwargs = {k[6:]: v for k, v in kwargs.items() if k.startswith("write_")}

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **func_kwargs):
            # Resolve paths at call time
            resolved_input = input_path if input_path is not None else _get_default_input()
            resolved_output = output_path if output_path is not None else _get_default_output()

            if not resolved_input:
                raise ValueError(
                    "No input path specified. Either pass 'input_path' argument or set "
                    "STORAGE_DEFAULT_INPUT environment variable."
                )
            if not resolved_output:
                raise ValueError(
                    "No output path specified. Either pass 'output_path' argument or set "
                    "STORAGE_DEFAULT_OUTPUT environment variable."
                )

            storage = get_backend()

            # Read input
            in_format = input_format or _infer_format(resolved_input)
            files = _expand_glob_pattern(storage, resolved_input)

            if not files:
                raise FileNotFoundError(f"No files found matching: {resolved_input}")

            if len(files) == 1:
                df = storage.read_dataframe(files[0], format=in_format, **read_kwargs)
            elif concat:
                dfs = [
                    storage.read_dataframe(f, format=in_format, **read_kwargs)
                    for f in files
                ]
                df = pl.concat(dfs)
            else:
                raise ValueError(
                    "data_io requires single file or concat=True for glob patterns"
                )

            # Call function
            result = func(df, *args, **func_kwargs)

            # Write output
            out_format = output_format or _infer_format(resolved_output)

            if isinstance(result, pl.DataFrame):
                storage.write_dataframe(result, resolved_output, format=out_format, **write_kwargs)
            elif isinstance(result, dict):
                storage.write_json(result, resolved_output)
            else:
                raise TypeError(
                    f"data_io function must return DataFrame or dict, got {type(result)}"
                )

            return result

        return wrapper
    return decorator

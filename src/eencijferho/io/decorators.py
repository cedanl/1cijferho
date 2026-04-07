"""Decorators for declarative I/O.

Usage:
    from eencijferho.io.decorators import reads_from, writes_to, with_storage

    # Static path — decorator reads/writes for you
    @reads_from("data/01-input/students.csv")
    def analyze(df):
        return df.describe()

    @writes_to("data/02-output/result.parquet")
    def produce():
        return some_dataframe

    # Dynamic paths — decorator injects the storage backend
    @with_storage
    def process(storage, input_path, output_path):
        df = storage.read_dataframe(input_path)
        result = df.filter(...)
        storage.write_dataframe(result, output_path)
"""

from __future__ import annotations

import functools
from collections.abc import Callable


def reads_from(path: str, format: str | None = None, **read_kwargs) -> Callable:
    """Decorator that reads data from storage and passes it as the first argument.

    The decorated function receives a DataFrame (for csv/parquet/excel),
    a dict/list (for json), or bytes (for other formats).
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            from eencijferho.io import get_backend

            backend = get_backend()
            fmt = format or backend.detect_format(path)

            if fmt == "json":
                data = backend.read_json(path)
            elif fmt in ("csv", "parquet", "excel"):
                data = backend.read_dataframe(path, format=fmt, **read_kwargs)
            else:
                data = backend.read_bytes(path)

            return func(data, *args, **kwargs)

        return wrapper

    return decorator


def writes_to(path: str, format: str | None = None, **write_kwargs) -> Callable:
    """Decorator that writes the function's return value to storage.

    Supports DataFrame, dict/list (JSON), bytes, and str return types.
    Returns the storage path/URI.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            from eencijferho.io import get_backend

            result = func(*args, **kwargs)
            if result is None:
                return None

            backend = get_backend()

            import polars as pl

            if isinstance(result, pl.DataFrame):
                return backend.write_dataframe(result, path, format=format, **write_kwargs)
            elif isinstance(result, (dict, list)):
                return backend.write_json(result, path)
            elif isinstance(result, bytes):
                return backend.write_bytes(result, path)
            elif isinstance(result, str):
                return backend.write_text(result, path)
            else:
                raise TypeError(f"Cannot write {type(result).__name__}")

        return wrapper

    return decorator


def with_storage(func: Callable) -> Callable:
    """Decorator that injects a storage backend as the first argument.

    Use this for functions with dynamic or multiple I/O operations::

        @with_storage
        def process(storage, input_dir, output_dir):
            for path in storage.list_files(f"{input_dir}/*.csv"):
                df = storage.read_dataframe(path)
                storage.write_dataframe(df, f"{output_dir}/{path}")
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        from eencijferho.io import get_backend

        backend = get_backend()
        return func(backend, *args, **kwargs)

    return wrapper

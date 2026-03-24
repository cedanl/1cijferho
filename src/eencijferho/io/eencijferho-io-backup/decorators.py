"""Simple decorators for declarative I/O.

Usage:
    from eencijferho.io.decorators import reads_from, writes_to

    @reads_from("01-input/students.csv")
    def analyze(df):
        return df.describe()

    @writes_to("02-output/result.parquet")
    def produce():
        return some_dataframe
"""

from __future__ import annotations

import functools
from typing import Callable


def reads_from(path: str, format: str | None = None, **read_kwargs) -> Callable:
    """Decorator that reads data from storage and passes it as the first argument.

    The decorated function receives a DataFrame (for csv/parquet/excel)
    or bytes (for other formats) as its first positional argument.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            from eencijferho.io import get_backend

            backend = get_backend()
            fmt = format or backend.detect_format(path)

            if fmt in ("csv", "parquet", "excel"):
                data = backend.read_dataframe(path, format=fmt, **read_kwargs)
            else:
                data = backend.read_bytes(path)

            return func(data, *args, **kwargs)

        return wrapper

    return decorator


def writes_to(path: str, format: str | None = None, **write_kwargs) -> Callable:
    """Decorator that writes the function's return value to storage.

    If the function returns a DataFrame, it is written using write_dataframe.
    If it returns bytes, it is written using write_bytes.
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
            elif isinstance(result, bytes):
                return backend.write_bytes(result, path)
            else:
                raise TypeError(f"Cannot write {type(result).__name__} — expected DataFrame or bytes")

        return wrapper

    return decorator

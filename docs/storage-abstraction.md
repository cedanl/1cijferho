# Storage Abstraction Layer

The `eencijferho.io` module provides a pluggable storage layer that lets you swap between local disk, MinIO (S3-compatible), and PostgreSQL — without changing any processing code.

## Quick Start

```python
from eencijferho.io import get_backend, storage_context

# Context manager (recommended — auto-cleans up connections)
with storage_context() as storage:
    df = storage.read_dataframe("01-input/data.csv")
    storage.write_dataframe(df, "02-output/result.parquet")

# Or get a backend directly
storage = get_backend()
```

The backend is selected via the `STORAGE_BACKEND` environment variable (default: `"disk"`).

## Backends

### Disk (default)

Local filesystem. Relative paths are resolved against a configurable base path.

```bash
export STORAGE_BACKEND=disk
export STORAGE_DISK_BASE_PATH=data  # default
```

### MinIO

S3-compatible object storage. Requires the `minio` optional dependency.

```bash
pip install eencijferho[minio]

export STORAGE_BACKEND=minio
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=1cijferho
export MINIO_SECURE=false
```

### PostgreSQL

Stores DataFrames as database tables and binary files in a `_binary_storage` table. Requires the `postgres` optional dependency.

```bash
pip install eencijferho[postgres]

export STORAGE_BACKEND=postgres
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=cijferho
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

File paths are converted to table names automatically:
- `01-input/student_data.csv` → `input_student_data`
- `02-output/results.parquet` → `output_results`

## Backend Interface

All backends implement the same interface (`StorageBackend`):

| Method | Description |
|---|---|
| `read_bytes(path)` | Read raw bytes |
| `write_bytes(data, path)` | Write raw bytes, returns path/URI |
| `read_dataframe(path, format=None, **kwargs)` | Read a Polars DataFrame |
| `write_dataframe(df, path, format=None, **kwargs)` | Write a Polars DataFrame, returns path/URI |
| `list_files(pattern)` | List files matching a glob pattern |
| `exists(path)` | Check if a path exists |

Format is auto-detected from the file extension (`.csv`, `.parquet`, `.xlsx`). CSV defaults to semicolon separator and UTF-8 encoding.

## Decorators

Two decorators provide a declarative way to wire up I/O:

### `@reads_from(path)`

Reads data from storage and passes it as the first argument to the decorated function.

```python
from eencijferho.io.decorators import reads_from

@reads_from("01-input/students.csv")
def analyze(df):
    # df is a Polars DataFrame, loaded automatically
    return df.filter(pl.col("score") > 80)
```

### `@writes_to(path)`

Writes the function's return value (DataFrame or bytes) to storage.

```python
from eencijferho.io.decorators import writes_to

@writes_to("02-output/summary.parquet")
def summarize():
    return pl.DataFrame({"metric": ["mean"], "value": [42.0]})

path = summarize()  # returns the written path/URI
```

Both decorators accept an optional `format` parameter and pass extra kwargs to the underlying read/write call.

## Docker Setup

The included `docker-compose.yml` runs the app with all three backends available:

```bash
# Default (disk)
docker compose up

# Use MinIO
STORAGE_BACKEND=minio docker compose up

# Use PostgreSQL
STORAGE_BACKEND=postgres docker compose up
```

Services:
- **app** — the Streamlit application (port 8000)
- **minio** — S3-compatible storage (API: 9000, console: 9001)
- **minio-init** — creates the default bucket on startup
- **postgres** — PostgreSQL 16 (port 5432)

## Architecture

```
eencijferho/io/
├── __init__.py          # get_backend() factory + storage_context()
├── config.py            # StorageConfig dataclass (all from env vars)
├── decorators.py        # @reads_from, @writes_to
└── backends/
    ├── base.py          # StorageBackend ABC
    ├── disk.py          # Local filesystem
    ├── minio.py         # S3-compatible (lazy import)
    └── postgres.py      # PostgreSQL tables (lazy import)
```

MinIO and PostgreSQL backends use lazy imports — their dependencies are only required when the backend is actually selected. The disk backend has no extra dependencies.

# Storage Abstraction Layer

The `eencijferho.io` module provides a pluggable storage layer that lets you swap between local disk, MinIO (S3-compatible), and PostgreSQL — without changing any processing code. All core modules use the `@with_storage` decorator, so switching backends is a single environment variable change.

## Table of Contents

- [Quick Start](#quick-start)
- [Backends](#backends)
- [Backend Interface](#backend-interface)
- [Decorators](#decorators)
- [Local Development with Docker](#local-development-with-docker)
- [Remote MinIO Setup](#remote-minio-setup)
- [Integration Testing](#integration-testing)
- [Architecture](#architecture)
- [Migrated Modules](#migrated-modules)

## Quick Start

By default, everything runs on the local filesystem — no setup needed:

```python
from eencijferho.io import get_backend, storage_context

# Context manager (recommended — auto-cleans up connections)
with storage_context() as storage:
    df = storage.read_dataframe("01-input/data.csv")
    storage.write_dataframe(df, "02-output/result.parquet")

# Or get a backend directly
storage = get_backend()
```

To switch to MinIO, set one environment variable:

```bash
export STORAGE_BACKEND=minio
# That's it — all @with_storage-decorated functions now use MinIO
```

## Backends

### Disk (default)

Local filesystem. Relative paths are resolved against a configurable base path.

```bash
export STORAGE_BACKEND=disk
export STORAGE_DISK_BASE_PATH=data   # default
```

### MinIO

S3-compatible object storage. Requires the `minio` optional dependency.

```bash
uv sync --extra minio
# or: pip install eencijferho[minio]

export STORAGE_BACKEND=minio
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=1cijferho
export MINIO_SECURE=false
```

### PostgreSQL

Stores DataFrames as database tables, JSON in a native JSONB column, and binary files in a `_binary_storage` table. Requires the `postgres` optional dependency.

```bash
uv sync --extra postgres
# or: pip install eencijferho[postgres]

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

All backends implement `StorageBackend`:

| Method | Description |
|---|---|
| `read_bytes(path)` | Read raw bytes |
| `write_bytes(data, path)` | Write raw bytes, returns path/URI |
| `read_dataframe(path, format?, **kwargs)` | Read a Polars DataFrame |
| `write_dataframe(df, path, format?, **kwargs)` | Write a Polars DataFrame |
| `list_files(pattern)` | List files matching a glob pattern |
| `exists(path)` | Check if a path exists |
| `delete(path)` | Delete a file |
| `read_json(path)` | Read and parse a JSON file |
| `write_json(data, path)` | Write a dict/list as JSON |
| `read_text(path, encoding?)` | Read a text file (default UTF-8) |
| `write_text(text, path, encoding?)` | Write a text file (default UTF-8) |

Format is auto-detected from the file extension (`.csv`, `.parquet`, `.xlsx`, `.json`). CSV defaults to semicolon separator (`;`) and UTF-8 encoding, matching the DUO convention.

## Decorators

Three decorators in `eencijferho.io.decorators`:

### `@with_storage` — the main decorator

Injects a storage backend as the first argument. Used by all migrated modules for dynamic or multi-file I/O.

```python
from eencijferho.io.decorators import with_storage

@with_storage
def process_files(storage, input_dir):
    for path in storage.list_files(f"{input_dir}/*.csv"):
        df = storage.read_dataframe(path)
        storage.write_dataframe(df, path.replace(".csv", ".parquet"))
```

Callers don't pass `storage` — the decorator handles it:

```python
process_files("data/01-input")  # storage is injected automatically
```

### `@reads_from(path)` — static reads

Reads data from a fixed path and passes it as the first argument.

```python
from eencijferho.io.decorators import reads_from

@reads_from("01-input/config.json")
def get_setting(data):
    return data["setting_name"]
```

### `@writes_to(path)` — static writes

Writes the function's return value to a fixed path.

```python
from eencijferho.io.decorators import writes_to

@writes_to("02-output/summary.parquet")
def summarize():
    return pl.DataFrame({"metric": ["mean"], "value": [42.0]})
```

## Local Development with Docker

The included `docker-compose.yml` provides MinIO and PostgreSQL for local development.

### Start MinIO only

```bash
docker compose up -d minio minio-init
```

This starts:
- **MinIO** — S3-compatible storage (API: `localhost:9000`, web console: `localhost:9001`)
- **minio-init** — creates the default `1cijferho` bucket automatically

Open the MinIO console at http://localhost:9001 (login: `minioadmin` / `minioadmin`) to browse uploaded objects.

### Start everything

```bash
docker compose up -d
```

Starts MinIO, PostgreSQL, and the Streamlit app.

### Run the app with MinIO

```bash
# Option 1: via docker-compose environment
STORAGE_BACKEND=minio docker compose up

# Option 2: locally against the dockerized MinIO
export STORAGE_BACKEND=minio
export MINIO_ENDPOINT=localhost:9000
uv run streamlit run src/main.py
```

### Upload existing data to MinIO

To copy your local `data/` folder into MinIO, use the MinIO client (`mc`):

```bash
# Install mc (macOS)
brew install minio/stable/mc

# Configure alias
mc alias set local http://localhost:9000 minioadmin minioadmin

# Upload data
mc cp --recursive data/ local/1cijferho/
```

Or use Python:

```python
from eencijferho.io import get_backend
import os

disk = get_backend("disk")
minio = get_backend("minio")

for path in disk.list_files("**/*"):
    data = disk.read_bytes(path)
    minio.write_bytes(data, path)
    print(f"Uploaded: {path}")
```

### Tear down

```bash
docker compose down           # stop containers, keep data
docker compose down -v        # stop containers AND delete volumes (fresh start)
```

## Remote MinIO Setup

To connect to a remote MinIO (or any S3-compatible) server in production:

### 1. Provision the server

MinIO can be deployed as a single binary, a Docker container, or a Kubernetes operator. See [min.io/docs](https://min.io/docs/minio/linux/index.html).

Example with Docker on a remote server:

```bash
docker run -d \
  --name minio \
  -p 9000:9000 -p 9001:9001 \
  -v /mnt/data:/data \
  -e MINIO_ROOT_USER=your-access-key \
  -e MINIO_ROOT_PASSWORD=your-secret-key \
  minio/minio server /data --console-address ":9001"
```

### 2. Create the bucket

Using the MinIO client:

```bash
mc alias set remote https://minio.example.com your-access-key your-secret-key
mc mb remote/1cijferho
```

Or the bucket is auto-created when the `MinIOBackend` connects (via `_ensure_bucket()`).

### 3. Configure the application

```bash
export STORAGE_BACKEND=minio
export MINIO_ENDPOINT=minio.example.com:9000   # or :443 for HTTPS
export MINIO_ACCESS_KEY=your-access-key
export MINIO_SECRET_KEY=your-secret-key
export MINIO_BUCKET=1cijferho
export MINIO_SECURE=true                        # true for HTTPS
```

### 4. Upload initial data

```bash
mc cp --recursive data/ remote/1cijferho/
```

### 5. Run the application

```bash
uv run streamlit run src/main.py
# or
uv run eencijferho pipeline --input 01-input --output 02-output
```

All paths in the code are relative (e.g., `01-input/file.asc`) and the MinIO backend uses them as S3 object keys within the configured bucket.

### Using AWS S3 instead of MinIO

MinIO is S3-compatible, so the same backend works with AWS S3. Just point to the AWS endpoint:

```bash
export STORAGE_BACKEND=minio
export MINIO_ENDPOINT=s3.eu-west-1.amazonaws.com
export MINIO_ACCESS_KEY=AKIA...
export MINIO_SECRET_KEY=...
export MINIO_BUCKET=your-bucket-name
export MINIO_SECURE=true
```

### Security notes

- Never commit access keys to version control. Use `.env` files (gitignored) or a secrets manager.
- For production, use `MINIO_SECURE=true` (TLS/HTTPS).
- Consider using IAM roles or MinIO's built-in policy system for fine-grained access control.

## Integration Testing

The project includes integration tests that run against a real MinIO instance.

### Run integration tests

```bash
# Start MinIO
docker compose up -d minio minio-init

# Run the integration suite (20 tests)
uv run pytest tests/integration/ -v

# Run ALL tests (unit + integration)
uv run pytest tests/ -v
```

### What's tested

| Category | Tests | Description |
|---|---|---|
| Basic CRUD | 7 | bytes, text, latin-1, JSON, exists, delete |
| DataFrame I/O | 3 | CSV roundtrip, Parquet roundtrip, separator check |
| File listing | 3 | wildcards, nested patterns, empty results |
| Decorators | 2 | `@with_storage` injection, `get_backend()` returns MinIO |
| Extractor workflow | 2 | extract tables, preserve accented characters |
| Converter workflow | 2 | pure chunk processing, metadata loading from MinIO |
| Compressor workflow | 1 | CSV → Parquet conversion in MinIO |

### Auto-skip behavior

Integration tests automatically skip when:
- Docker is not installed or not running
- MinIO container is not healthy

This means `uv run pytest tests/` always works — integration tests are silently skipped if MinIO isn't available.

## Architecture

```
eencijferho/io/
├── __init__.py          # get_backend() factory + storage_context()
├── config.py            # StorageConfig dataclass (all from env vars)
├── decorators.py        # @reads_from, @writes_to, @with_storage
└── backends/
    ├── base.py          # StorageBackend ABC + convenience methods
    ├── disk.py          # Local filesystem
    ├── minio.py         # S3-compatible (lazy import)
    └── postgres.py      # PostgreSQL tables + JSONB (lazy import)
```

MinIO and PostgreSQL backends use lazy imports — their dependencies are only loaded when the backend is actually selected. The disk backend has no extra dependencies.

## Migrated Modules

All core processing modules use `@with_storage` for I/O:

| Module | What uses storage |
|---|---|
| `core/converter.py` | Reading metadata (Excel), reading input files, writing logs |
| `core/decoder.py` | Loading variable mappings (JSON), loading Dec tables (CSV) |
| `core/decoder_info.py` | Reading decode metadata (JSON) |
| `core/extractor.py` | Reading .txt files, writing JSON, file listing, log writing |
| `core/parse_metadata.py` | Reading metadata text files |
| `core/pipeline.py` | File discovery, CSV read/write for decode/enrich steps |
| `utils/compressor.py` | CSV → Parquet conversion |
| `utils/converter_match.py` | File listing, row counting, log writing |
| `utils/extractor_validation.py` | Reading Excel metadata, log writing |
| `cli.py` | Decode, enrich, and validate-output commands |

**Intentionally disk-only** (not migrated):
- `_write_table_excel` — pandas ExcelWriter needs a file path or buffer; used only during metadata extraction
- `_run_parallel` / `_run_serial` — multiprocessing workers need direct disk access for writing CSV chunks
- `converter_validation.py`, `encryptor.py`, `converter_headers.py` — leaf utilities that could be migrated later if needed

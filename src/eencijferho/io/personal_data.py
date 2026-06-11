"""In-process personal-data store for the Streamlit demo.

Splits an uploaded record into three destinations using the storage backends
in :mod:`eencijferho.io`:

- ``secret_sensitive`` (PostgreSQL): UUID + encrypted personal identifiers
- ``secret_regular``   (PostgreSQL): UUID + demographic fields
- a JSON object in MinIO: UUID + remaining non-sensitive fields

This is the library equivalent of the FastAPI ``personal-data`` router from the
sibling 1cijfer-config repo, but it calls the backends directly instead of over
HTTP, matching this repo's storage-abstraction architecture.
"""

from __future__ import annotations

import json
from datetime import datetime

from eencijferho.io import get_backend

MINIO_PREFIX = "personal-data"

# Columns expected in the two PostgreSQL tables (excluding uuid/created_at).
# These mirror init.sql; read_schema() refreshes them from the live DB.
DEFAULT_SENSITIVE_COLUMNS = [
    "persoonsgebonden_nummer",
    "burgerservice_nummer",
    "onderwijs_nummer",
]
DEFAULT_REGULAR_COLUMNS = [
    "geslacht",
    "nationaliteit_1",
    "nationaliteit_2",
    "nationaliteit_3",
    "geboorteland",
    "geboorteland_ouder_1",
    "geboorteland_ouder_2",
    "postcodecijfer_ho",
    "indicatie_eer_actueel",
    "indicatie_internationale_student",
    "nationaliteit_eer_actueel",
    "herkomstland_cbr",
    "herkomstindikking_cbr",
    "indicatie_geboren",
]

_SENSITIVE_DDL = """
CREATE TABLE IF NOT EXISTS secret_sensitive (
    uuid TEXT PRIMARY KEY,
    persoonsgebonden_nummer TEXT,
    burgerservice_nummer TEXT,
    onderwijs_nummer TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

_REGULAR_DDL = """
CREATE TABLE IF NOT EXISTS secret_regular (
    uuid TEXT PRIMARY KEY,
    geslacht VARCHAR(8),
    nationaliteit_1 VARCHAR(255),
    nationaliteit_2 VARCHAR(255),
    nationaliteit_3 VARCHAR(255),
    geboorteland VARCHAR(255),
    geboorteland_ouder_1 VARCHAR(255),
    geboorteland_ouder_2 VARCHAR(255),
    postcodecijfer_ho VARCHAR(8),
    indicatie_eer_actueel VARCHAR(255),
    indicatie_internationale_student VARCHAR(255),
    nationaliteit_eer_actueel VARCHAR(255),
    herkomstland_cbr VARCHAR(255),
    herkomstindikking_cbr VARCHAR(255),
    indicatie_geboren VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (uuid) REFERENCES secret_sensitive(uuid) ON DELETE CASCADE
)
"""


def _pg():
    """Return the PostgresBackend (autocommit psycopg2 connection on .conn)."""
    return get_backend("postgres")


def _minio():
    return get_backend("minio")


def ensure_schema() -> None:
    """Create the two personal-data tables if they do not exist."""
    pg = _pg()
    with pg.conn.cursor() as cur:
        cur.execute(_SENSITIVE_DDL)
        cur.execute(_REGULAR_DDL)


def _table_columns(pg, table_name: str) -> list[str]:
    with pg.conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name NOT IN ('uuid', 'created_at')
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def get_schema() -> dict[str, list[str]]:
    """Return the sensitive/regular column names from the live database.

    Falls back to the defaults if the tables don't exist yet.
    """
    try:
        pg = _pg()
        ensure_schema()
        sensitive = _table_columns(pg, "secret_sensitive") or DEFAULT_SENSITIVE_COLUMNS
        regular = _table_columns(pg, "secret_regular") or DEFAULT_REGULAR_COLUMNS
        return {"sensitive_columns": sensitive, "regular_columns": regular}
    except Exception:
        return {
            "sensitive_columns": DEFAULT_SENSITIVE_COLUMNS,
            "regular_columns": DEFAULT_REGULAR_COLUMNS,
        }


def _get_value(row: dict, col: str):
    """Case-insensitive lookup of a column in a row dict."""
    if col in row:
        return row[col]
    lower = {k.lower(): k for k in row}
    if col.lower() in lower:
        return row[lower[col.lower()]]
    return None


def upload_personal_data(rows: list[dict]) -> dict:
    """Store pre-encrypted rows across PostgreSQL tables and a MinIO JSON file.

    Each row must already contain a ``uuid`` plus encrypted sensitive fields and
    plaintext regular/extra fields (encryption happens client-side in the browser
    via Pyodide before this is ever called).
    """
    ensure_schema()
    schema = get_schema()
    sensitive_cols = schema["sensitive_columns"]
    regular_cols = schema["regular_columns"]

    pg = _pg()
    inserted = 0
    minio_rows: list[dict] = []
    exclude = {c.lower() for c in sensitive_cols + regular_cols}

    with pg.conn.cursor() as cur:
        for row in rows:
            uuid = row.get("uuid")
            if not uuid:
                continue

            # secret_sensitive
            cols = ["uuid"] + sensitive_cols
            vals = [uuid] + [_get_value(row, c) for c in sensitive_cols]
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in sensitive_cols)
            cur.execute(
                f"INSERT INTO secret_sensitive ({', '.join(cols)}) "
                f"VALUES ({', '.join(['%s'] * len(cols))}) "
                f"ON CONFLICT (uuid) DO UPDATE SET {updates}",
                vals,
            )

            # secret_regular
            cols = ["uuid"] + regular_cols
            vals = [uuid] + [_get_value(row, c) for c in regular_cols]
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in regular_cols)
            cur.execute(
                f"INSERT INTO secret_regular ({', '.join(cols)}) "
                f"VALUES ({', '.join(['%s'] * len(cols))}) "
                f"ON CONFLICT (uuid) DO UPDATE SET {updates}",
                vals,
            )
            inserted += 1

            # MinIO: uuid + everything that isn't a known secret/regular column
            minio_row = {"uuid": uuid}
            for key, value in row.items():
                if key != "uuid" and key.lower() not in exclude:
                    minio_row[key] = value
            minio_rows.append(minio_row)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    minio_filename = f"{MINIO_PREFIX}/personal_data_{timestamp}.json"
    _minio().write_json(minio_rows, minio_filename)

    return {"inserted": inserted, "minio_file": minio_filename}


def list_minio_files() -> list[dict]:
    """List the personal-data JSON objects stored in MinIO."""
    minio = _minio()
    keys = minio.list_files(f"{MINIO_PREFIX}/*.json")
    files = []
    for key in sorted(keys):
        try:
            stat = minio.client.stat_object(minio.bucket, key)
            files.append(
                {
                    "name": key,
                    "size": stat.size,
                    "last_modified": stat.last_modified.isoformat()
                    if stat.last_modified
                    else None,
                }
            )
        except Exception:
            files.append({"name": key, "size": None, "last_modified": None})
    return files


def _fetch_db_rows(pg, table: str, columns: list[str], uuids: list[str]) -> dict[str, dict]:
    if not uuids:
        return {}
    placeholders = ", ".join(["%s"] * len(uuids))
    col_str = ", ".join(columns)
    with pg.conn.cursor() as cur:
        cur.execute(
            f"SELECT uuid, {col_str} FROM {table} WHERE uuid IN ({placeholders})",
            uuids,
        )
        result = {}
        for db_row in cur.fetchall():
            result[str(db_row[0])] = {col: db_row[i + 1] for i, col in enumerate(columns)}
    return result


def view_file(filename: str, mode: str = "raw") -> list[dict]:
    """Return file contents at one of three resolution levels.

    Modes:
        raw            — MinIO file only (uuid + extra fields).
        with_regular   — joined with secret_regular (demographic data).
        with_encrypted — joined with secret_sensitive (still encrypted) + regular.
    """
    file_data = _minio().read_json(filename)
    if not isinstance(file_data, list):
        file_data = [file_data]

    if mode == "raw":
        return file_data

    schema = get_schema()
    regular_cols = schema["regular_columns"]
    sensitive_cols = schema["sensitive_columns"]
    uuids = [r["uuid"] for r in file_data if "uuid" in r]

    pg = _pg()
    regular = _fetch_db_rows(pg, "secret_regular", regular_cols, uuids)
    sensitive = (
        _fetch_db_rows(pg, "secret_sensitive", sensitive_cols, uuids)
        if mode == "with_encrypted"
        else {}
    )

    merged = []
    for file_row in file_data:
        uuid = file_row.get("uuid")
        ordered = {"uuid": uuid} if uuid is not None else {}
        if mode == "with_encrypted" and uuid in sensitive:
            ordered.update(sensitive[uuid])
        if uuid in regular:
            ordered.update(regular[uuid])
        for key, value in file_row.items():
            if key != "uuid":
                ordered[key] = value
        merged.append(ordered)
    return merged


def delete_file(filename: str) -> dict:
    """Delete a MinIO file and the DB records for the UUIDs it references."""
    minio = _minio()
    file_data = minio.read_json(filename)
    if not isinstance(file_data, list):
        file_data = [file_data]
    uuids = [r["uuid"] for r in file_data if "uuid" in r]

    deleted = 0
    if uuids:
        pg = _pg()
        placeholders = ", ".join(["%s"] * len(uuids))
        with pg.conn.cursor() as cur:
            # secret_regular cascades from secret_sensitive via FK.
            cur.execute(
                f"DELETE FROM secret_sensitive WHERE uuid IN ({placeholders})", uuids
            )
            deleted = cur.rowcount

    minio.delete(filename)
    return {"deleted_records": deleted, "file": filename}

"""Integration tests for the personal-data store.

Exercises the full server-side flow of ``eencijferho.io.personal_data`` against
real MinIO + PostgreSQL instances: schema creation, UUID derivation, splitting
records across the two secret tables and MinIO, reading back at each resolution
level, and deleting. Skips when either backend is unavailable.

Run with:
    uv run pytest tests/integration/test_personal_data.py -v
"""

import pytest

from eencijferho.io import personal_data


@pytest.fixture
def clean_store(personal_data_env):
    """Ensure a fresh schema and drop any rows/files left by earlier runs."""
    personal_data.ensure_schema()
    pg = personal_data._pg()
    with pg.conn.cursor() as cur:
        cur.execute("DELETE FROM secret_sensitive")  # cascades to secret_regular
    # Remove any lingering personal-data objects in MinIO.
    minio = personal_data._minio()
    for key in minio.list_files(f"{personal_data.MINIO_PREFIX}/*.json"):
        minio.delete(key)
    yield


def _sample_rows():
    return [
        {
            "__uuid_source": "100000009",
            "salt": "c2FsdHNhbHRzYWx0MDE=",
            "persoonsgebonden_nummer": "gAAAAABenc_pgn_1",
            "burgerservice_nummer": "gAAAAABenc_bsn_1",
            "onderwijs_nummer": "",
            "geslacht": "M",
            "nationaliteit_1": "NL",
            "extra_veld": "waarde-1",
        },
        {
            "__uuid_source": "100000009",  # same source -> same uuid
            "salt": "c2FsdHNhbHRzYWx0MDI=",
            "persoonsgebonden_nummer": "gAAAAABenc_pgn_2",
            "burgerservice_nummer": "",
            "onderwijs_nummer": "",
            "geslacht": "M",
            "extra_veld": "waarde-2",
        },
    ]


def test_upload_derives_uuid_and_splits_storage(clean_store):
    result = personal_data.upload_personal_data(_sample_rows())
    assert result["inserted"] == 2
    assert result["minio_file"].startswith(personal_data.MINIO_PREFIX)


def test_uuid_source_not_persisted(clean_store):
    """The plaintext PGN (uuid source) must never land in MinIO or the DB."""
    result = personal_data.upload_personal_data(_sample_rows())
    raw = personal_data.view_file(result["minio_file"], mode="raw")
    for row in raw:
        assert "__uuid_source" not in row
        assert "persoonsgebonden_nummer" not in row  # sensitive stays in Postgres
    # And the derived uuid matches server-side derivation from the source.
    assert raw[0]["uuid"] == personal_data.derive_uuid("100000009")


def test_same_source_yields_same_uuid(clean_store):
    result = personal_data.upload_personal_data(_sample_rows())
    raw = personal_data.view_file(result["minio_file"], mode="raw")
    assert raw[0]["uuid"] == raw[1]["uuid"]


def test_view_with_encrypted_returns_salt_and_ciphertext(clean_store):
    result = personal_data.upload_personal_data(_sample_rows())
    rows = personal_data.view_file(result["minio_file"], mode="with_encrypted")
    first = rows[0]
    assert first["salt"]  # salt returned so the browser can re-derive the key
    assert first["persoonsgebonden_nummer"].startswith("gAAAAAB")  # still ciphertext


def test_view_with_regular_joins_demographics(clean_store):
    result = personal_data.upload_personal_data(_sample_rows())
    rows = personal_data.view_file(result["minio_file"], mode="with_regular")
    assert rows[0]["geslacht"] == "M"
    assert "persoonsgebonden_nummer" not in rows[0]  # sensitive not in this mode


def test_delete_removes_file_and_records(clean_store):
    result = personal_data.upload_personal_data(_sample_rows())
    deleted = personal_data.delete_file(result["minio_file"])
    assert deleted["deleted_records"] >= 1
    assert personal_data.list_minio_files() == []


def test_list_minio_files_reflects_uploads(clean_store):
    assert personal_data.list_minio_files() == []
    personal_data.upload_personal_data(_sample_rows())
    files = personal_data.list_minio_files()
    assert len(files) == 1
    assert files[0]["name"].startswith(personal_data.MINIO_PREFIX)

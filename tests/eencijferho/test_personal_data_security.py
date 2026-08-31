"""Beveiligingsregressietests voor de personal-data demo.

Deze tests borgen de fixes uit de code-review op PR #147:

- Server-side UUID-afleiding (de HMAC-sleutel mag nooit naar de browser).
- Geen plaintext-identifier in de opslag-payload.
- XSS-fix in de client-side ontsleutel-render (textContent i.p.v. innerHTML).
- Geen misleidende "verlaat uw browser nooit"-belofte meer.

De JS-tests zijn *regressiewachters*: ze parsen de asset-bestanden en
controleren op (on)veilige patronen. Ze draaien in de bestaande Python-CI
zonder browser of Node-toolchain — ze bewijzen geen browsergedrag, maar
vangen het opnieuw introduceren van de kwetsbare patronen af.
"""

from pathlib import Path

import pytest

from eencijferho.io import personal_data

_JS_DIR = Path(__file__).resolve().parents[2] / "src" / "frontend" / "Files" / "js"


def _read(name: str) -> str:
    return (_JS_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Server-side UUID-afleiding (#1)
# ---------------------------------------------------------------------------

def test_derive_uuid_is_deterministic():
    assert personal_data.derive_uuid("123456789") == personal_data.derive_uuid("123456789")


def test_derive_uuid_is_source_sensitive():
    assert personal_data.derive_uuid("123456789") != personal_data.derive_uuid("987654321")


def test_derive_uuid_has_uuid_shape():
    uuid = personal_data.derive_uuid("123456789")
    parts = uuid.split("-")
    assert [len(p) for p in parts] == [8, 4, 4, 4, 12]


def test_derive_uuid_depends_on_key(monkeypatch):
    monkeypatch.setenv(personal_data.ENCRYPT_KEY_ENV, "a" * 64)
    with_key_a = personal_data.derive_uuid("123456789")
    monkeypatch.setenv(personal_data.ENCRYPT_KEY_ENV, "b" * 64)
    with_key_b = personal_data.derive_uuid("123456789")
    assert with_key_a != with_key_b


def test_is_demo_key_reflects_env(monkeypatch):
    monkeypatch.delenv(personal_data.ENCRYPT_KEY_ENV, raising=False)
    assert personal_data.is_demo_key() is True
    monkeypatch.setenv(personal_data.ENCRYPT_KEY_ENV, "x" * 64)
    assert personal_data.is_demo_key() is False


def test_uuid_key_never_appears_in_upload_js():
    """The browser payload must not carry the HMAC key or derive UUIDs itself."""
    js = _read("upload_personal.js")
    assert "uuid_key" not in js
    assert "generate_uuid" not in js
    # The browser sends a value to derive from; the server does the HMAC.
    assert "__uuid_source" in js


# ---------------------------------------------------------------------------
# XSS-regressiewachter voor de ontsleutel-render (#2)
# ---------------------------------------------------------------------------

def test_decrypt_render_uses_textcontent():
    html = _read("decrypt_personal.html")
    assert "textContent" in html
    assert "createElement" in html


def test_decrypt_render_has_no_unsafe_cell_concatenation():
    """The vulnerable '<td>' + value + '</td>' innerHTML pattern must be gone."""
    html = _read("decrypt_personal.html")
    assert "'<td>'" not in html
    assert "'<th>'" not in html
    # No innerHTML assignment of a built-up html string in the render path.
    assert "innerHTML = html" not in html


# ---------------------------------------------------------------------------
# Geen misleidende privacy-belofte (#3)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "page",
    [
        "Upload_Personal_Data.py",
        "Browse_Personal_Data.py",
        "Encrypt_Upload.py",
    ],
)
def test_pages_drop_never_leaves_browser_claim(page):
    frontend = Path(__file__).resolve().parents[2] / "src" / "frontend" / "Files"
    text = (frontend / page).read_text(encoding="utf-8")
    assert "verlaat uw browser nooit" not in text
    assert "nooit naar de server" not in text

import base64
import json
from datetime import datetime
from pathlib import Path

import polars as pl
import streamlit as st
from streamlit_js import st_js_blocking

from eencijferho.io import get_backend

_JS_DIR = Path(__file__).parent / "js"


def _load_js(name: str, payload_b64: str) -> str:
    template = (_JS_DIR / name).read_text(encoding="utf-8")
    bootstrap = (_JS_DIR / "_pyodide_bootstrap.js").read_text(encoding="utf-8")
    return template.replace("// __PYODIDE_BOOTSTRAP__", bootstrap).replace(
        "__PAYLOAD_B64__", payload_b64
    )

# -----------------------------------------------------------------------------
# Page Header
# -----------------------------------------------------------------------------
st.title("Versleutelen & uploaden")
st.markdown("""
<div class="page-intro">
    Upload een CSV-bestand en versleutel een gevoelige kolom (zoals BSN) <strong>in uw
    browser</strong>. Met één knop wordt de data versleuteld én opgeslagen in MinIO en
    PostgreSQL. De opgeslagen data bevat de gekozen kolom alleen in versleutelde vorm.
</div>
""", unsafe_allow_html=True)

st.markdown("""
**Stappen:**
1. Upload een CSV-bestand
2. Kies de kolom die versleuteld moet worden
3. Voer een wachtwoord in
4. Klik op **Versleutelen & opslaan** — versleuteling gebeurt in uw browser,
   daarna wordt het resultaat opgeslagen (downloaden blijft optioneel)
""")

st.markdown("---")

# -----------------------------------------------------------------------------
# Step 1 — Upload CSV
# -----------------------------------------------------------------------------
st.subheader("Stap 1 · CSV-bestand uploaden")

uploaded_file = st.file_uploader("Kies een CSV-bestand", type=["csv"])

if uploaded_file is None:
    st.info("Upload een CSV-bestand om te beginnen.")
    st.stop()

try:
    df = pl.read_csv(uploaded_file, infer_schema_length=0)
except Exception as e:
    st.error(f"Fout bij lezen van CSV: {e}")
    st.stop()

st.success(f"Bestand geladen: {uploaded_file.name}")
st.write(f"**Rijen:** {len(df)} · **Kolommen:** {len(df.columns)}")
with st.expander("Data bekijken"):
    st.dataframe(df.head(10))

st.markdown("---")

# -----------------------------------------------------------------------------
# Step 2 — Select column
# -----------------------------------------------------------------------------
st.subheader("Stap 2 · Kolom kiezen om te versleutelen")

column_to_encrypt = st.selectbox(
    "Selecteer de kolom met gevoelige data (bijv. BSN)",
    options=df.columns,
    index=0,
)
with st.expander(f"Voorbeeldwaarden van '{column_to_encrypt}'"):
    st.write(df[column_to_encrypt].head(10))

st.markdown("---")

# -----------------------------------------------------------------------------
# Step 3 — Encrypt in browser + store, with one button
# -----------------------------------------------------------------------------
st.subheader("Stap 3 · Versleutelen & opslaan")

password = st.text_input(
    "Voer een versleutelwachtwoord in",
    type="password",
    help="Dit wachtwoord versleutelt de gekozen kolom in uw browser.",
)

if not password:
    st.info("Voer een wachtwoord in om versleuteling in te schakelen.")
    st.stop()

st.warning("Onthoud dit wachtwoord — u heeft het nodig om de data later te ontsleutelen.")

st.caption(
    "De eerste keer duurt het laden van Pyodide in uw browser 10-20 seconden. "
    "De versleuteling zelf gebeurt volledig lokaal."
)


def _build_encrypt_js(csv_b64: str, password: str, column: str) -> str:
    """Load the encrypt JS and inject all inputs as a single base64 JSON blob."""
    payload = json.dumps({"csv_b64": csv_b64, "password": password, "column": column})
    payload_b64 = base64.b64encode(payload.encode()).decode()
    return _load_js("encrypt_upload.js", payload_b64)


# st_js_blocking calls st.stop() until the browser component returns a result on
# a later rerun, so the click must be latched in session_state — gating the work
# directly on st.button() would drop the async result on the next rerun.
if st.button("Versleutelen & opslaan", type="primary"):
    st.session_state["encrypt_upload_running"] = True

if st.session_state.get("encrypt_upload_running"):
    csv_base64 = base64.b64encode(df.write_csv().encode()).decode()
    js = _build_encrypt_js(csv_base64, password, column_to_encrypt)

    with st.spinner("Versleutelen in uw browser (Pyodide)..."):
        raw = st_js_blocking(js, key="encrypt_upload_js")

    # Result is in; clear the latch so a rerun doesn't reprocess.
    st.session_state["encrypt_upload_running"] = False

    try:
        result = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        st.error("Versleuteling in de browser is mislukt of niet voltooid. Probeer opnieuw.")
        st.stop()

    encrypted_csv = result["csv"]
    encrypted_count = result["encrypted_count"]
    encrypted_bytes = encrypted_csv.encode()

    st.success(f"{encrypted_count} waarden versleuteld in kolom '{column_to_encrypt}'.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_path = f"encrypted-uploads/{timestamp}_{uploaded_file.name}"

    try:
        with st.spinner("Opslaan in MinIO..."):
            minio_uri = get_backend("minio").write_bytes(encrypted_bytes, object_path)
        with st.spinner("Opslaan in PostgreSQL..."):
            pg_uri = get_backend("postgres").write_bytes(encrypted_bytes, object_path)
    except Exception as e:
        st.error(f"Fout bij opslaan: {e}")
        st.info("Controleer of de MinIO- en PostgreSQL-containers draaien (`docker compose up`).")
        st.stop()

    st.success("Versleuteld bestand opgeslagen in MinIO én PostgreSQL.")
    st.json({"minio": minio_uri, "postgres": pg_uri, "bytes": len(encrypted_bytes)})

    st.download_button(
        "Versleuteld CSV downloaden (optioneel)",
        data=encrypted_bytes,
        file_name=f"encrypted_{uploaded_file.name}",
        mime="text/csv",
    )
    st.balloons()

# -----------------------------------------------------------------------------
# Info
# -----------------------------------------------------------------------------
st.markdown("---")
with st.expander("Hoe werkt browser-versleuteling?"):
    st.markdown("""
    **Pyodide** draait Python in uw browser via WebAssembly. De versleuteling van de
    gekozen kolom gebeurt **lokaal**, zodat de opgeslagen data die kolom alleen in
    versleutelde vorm bevat.

    1. CSV wordt in uw browser geladen
    2. Per rij wordt een willekeurige salt gegenereerd; het wachtwoord leidt
       daarmee een sleutel af (PBKDF2, 100.000 iteraties)
    3. De gekozen kolom wordt versleuteld met Fernet (AES-128-CBC); de salt komt
       in een extra `salt`-kolom zodat ontsleuteling later mogelijk blijft
    4. Alleen het **versleutelde** resultaat gaat naar de server
    5. Het wordt opgeslagen in MinIO én PostgreSQL — downloaden is optioneel
    """)

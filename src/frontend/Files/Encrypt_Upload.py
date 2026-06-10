import base64
import json
from datetime import datetime

import pandas as pd
import streamlit as st
from streamlit_js import st_js_blocking

from eencijferho.io import get_backend

# -----------------------------------------------------------------------------
# Page Header
# -----------------------------------------------------------------------------
st.title("Versleutelen & uploaden")
st.markdown("""
<div class="page-intro">
    Upload een CSV-bestand en versleutel een gevoelige kolom (zoals BSN) <strong>in uw
    browser</strong>. Met één knop wordt de data versleuteld én opgeslagen in MinIO en
    PostgreSQL. Onversleutelde data verlaat uw browser nooit.
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
    df = pd.read_csv(uploaded_file)
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
    options=df.columns.tolist(),
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
    """JS that loads Pyodide, encrypts the chosen column in-browser, returns the CSV."""
    payload = json.dumps({"csv_b64": csv_b64, "password": password, "column": column})
    payload_b64 = base64.b64encode(payload.encode()).decode()
    return f"""
const payload = JSON.parse(atob("{payload_b64}"));
if (!window.__pyodidePromise) {{
    window.__pyodidePromise = (async () => {{
        const {{ loadPyodide }} = await import("https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.mjs");
        const py = await loadPyodide();
        await py.loadPackage(["micropip"]);
        await py.runPythonAsync(`
            import micropip
            await micropip.install('cryptography')
        `);
        return py;
    }})();
}}
const py = await window.__pyodidePromise;
py.globals.set("csv_data_input", atob(payload.csv_b64));
py.globals.set("password_input", payload.password);
py.globals.set("column_name_input", payload.column);
const result = await py.runPythonAsync(`
import io, csv, base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

def derive_key(password, salt=b'eencijfer_salt_2025'):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

cipher = Fernet(derive_key(password_input))
rows = list(csv.DictReader(io.StringIO(csv_data_input)))
encrypted_count = 0
for row in rows:
    if column_name_input in row and row[column_name_input]:
        row[column_name_input] = cipher.encrypt(row[column_name_input].encode()).decode()
        encrypted_count += 1
output = io.StringIO()
if rows:
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
import json as _json
_json.dumps({{"encrypted_count": encrypted_count, "csv": output.getvalue()}})
`);
return result;
"""


if st.button("Versleutelen & opslaan", type="primary"):
    csv_base64 = base64.b64encode(df.to_csv(index=False).encode()).decode()
    js = _build_encrypt_js(csv_base64, password, column_to_encrypt)

    with st.spinner("Versleutelen in uw browser (Pyodide)..."):
        raw = st_js_blocking(js, key="encrypt_upload_js")

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
    **Pyodide** draait Python in uw browser via WebAssembly. Alle versleuteling
    gebeurt **lokaal**; onversleutelde gevoelige data verlaat uw browser nooit.

    1. CSV wordt in uw browser geladen
    2. Het wachtwoord leidt een sleutel af (PBKDF2, 100.000 iteraties)
    3. De gekozen kolom wordt versleuteld met Fernet (AES-128-CBC)
    4. Alleen het **versleutelde** resultaat gaat naar de server
    5. Het wordt opgeslagen in MinIO én PostgreSQL — downloaden is optioneel
    """)

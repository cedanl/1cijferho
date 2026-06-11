import base64
import json

import pandas as pd
import streamlit as st
from streamlit_js import st_js_blocking

from eencijferho.io import personal_data

# -----------------------------------------------------------------------------
# Schema (column names) from the database, with defaults as fallback
# -----------------------------------------------------------------------------
@st.cache_data(ttl=300)
def _schema() -> tuple[list[str], list[str]]:
    s = personal_data.get_schema()
    return s["sensitive_columns"], s["regular_columns"]


SENSITIVE_FIELDS, REGULAR_FIELDS = _schema()

# -----------------------------------------------------------------------------
# Page Header
# -----------------------------------------------------------------------------
st.title("Persoonsgegevens uploaden")
st.markdown("""
<div class="page-intro">
    Upload een CSV met persoonsgegevens. Zeer gevoelige velden worden automatisch
    <strong>in uw browser</strong> versleuteld. Met één knop wordt de data verwerkt én
    opgesplitst over PostgreSQL (gevoelig + regulier) en MinIO (overige velden).
</div>
""", unsafe_allow_html=True)

st.markdown("""
**Stappen:**
1. Upload een CSV met persoonsgegevens
2. Het systeem detecteert gevoelige kolommen automatisch
3. Voer een wachtwoord in
4. Klik op **Verwerken & opslaan** — versleuteling + UUID-afleiding gebeuren in uw
   browser, daarna wordt de data opgeslagen (downloaden blijft optioneel)
""")

st.markdown("---")

# -----------------------------------------------------------------------------
# Step 1 — Upload CSV
# -----------------------------------------------------------------------------
st.subheader("Stap 1 · CSV-bestand uploaden")

uploaded_file = st.file_uploader("Kies een CSV met persoonsgegevens", type=["csv"])
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


def _find_matching(schema_cols: list[str], df_cols) -> list[str]:
    lower = {c.lower(): c for c in df_cols}
    return [lower[c.lower()] for c in schema_cols if c.lower() in lower]


detected_sensitive = _find_matching(SENSITIVE_FIELDS, df.columns)
detected_regular = _find_matching(REGULAR_FIELDS, df.columns)

st.markdown("---")
st.subheader("Stap 2 · Gedetecteerde kolommen")
col1, col2 = st.columns(2)
with col1:
    st.write("**Zeer gevoelig (wordt versleuteld):**")
    if detected_sensitive:
        for c in detected_sensitive:
            st.write(f"🔒 {c}")
    else:
        st.warning("Geen zeer gevoelige kolommen gedetecteerd")
with col2:
    st.write("**Regulier (opgeslagen zoals het is):**")
    if detected_regular:
        for c in detected_regular:
            st.write(f"📋 {c}")
    else:
        st.info("Geen reguliere kolommen gedetecteerd")

# Persoonsgebonden_nummer is required for UUID generation (case-insensitive).
pgn_col = next((c for c in df.columns if c.lower() == "persoonsgebonden_nummer"), None)
if pgn_col is None:
    st.error("Verplichte kolom ontbreekt: Persoonsgebonden_nummer (nodig voor UUID).")
    st.stop()
if not detected_sensitive:
    st.error("Geen zeer gevoelige kolommen gedetecteerd. Kan niet doorgaan.")
    st.stop()

st.markdown("---")

# -----------------------------------------------------------------------------
# Step 3 — Encrypt + process in browser + store, with one button
# -----------------------------------------------------------------------------
st.subheader("Stap 3 · Verwerken & opslaan")

password = st.text_input(
    "Voer een versleutelwachtwoord in",
    type="password",
    help="Dit wachtwoord versleutelt de zeer gevoelige kolommen in uw browser.",
)
if not password:
    st.info("Voer een wachtwoord in om versleuteling in te schakelen.")
    st.stop()

st.warning("Onthoud dit wachtwoord — u heeft het nodig om de data later te ontsleutelen.")

st.caption(
    "De eerste keer duurt het laden van Pyodide in uw browser 10-20 seconden. "
    "Versleuteling en UUID-afleiding gebeuren volledig lokaal."
)


def _build_process_js(csv_b64: str, password: str,
                      sensitive: list[str], regular: list[str]) -> str:
    """JS that loads Pyodide, encrypts sensitive cols + derives UUID, returns rows JSON."""
    payload = json.dumps({
        "csv_b64": csv_b64,
        "password": password,
        "sensitive": sensitive,
        "regular": regular,
    })
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
py.globals.set("sensitive_columns_input", payload.sensitive);
py.globals.set("regular_columns_input", payload.regular);
const result = await py.runPythonAsync(`
import io, csv, hashlib, base64, json
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

def derive_key(password, salt=b'eencijfer_salt_2025'):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

def generate_uuid(pgn):
    h = hashlib.md5(str(pgn).encode()).hexdigest()
    return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]

cipher = Fernet(derive_key(password_input))
sensitive_columns = list(sensitive_columns_input)
regular_columns = list(regular_columns_input)
rows = list(csv.DictReader(io.StringIO(csv_data_input)))

processed_rows = []
encrypted_count = 0
for i, row in enumerate(rows):
    pgn_value = None
    for col in row:
        if col.lower() == 'persoonsgebonden_nummer':
            pgn_value = row[col]
            break
    uuid = generate_uuid(pgn_value) if pgn_value else hashlib.md5(str(i).encode()).hexdigest()
    processed_row = {{'uuid': uuid}}
    for col in sensitive_columns:
        if col in row and row[col]:
            processed_row[col.lower()] = cipher.encrypt(str(row[col]).encode()).decode()
            encrypted_count += 1
        else:
            processed_row[col.lower()] = ''
    for col in regular_columns:
        processed_row[col.lower()] = row.get(col, '')
    for col in row:
        if col not in sensitive_columns and col not in regular_columns:
            processed_row[col] = row[col]
    processed_rows.append(processed_row)

json.dumps({{"encrypted_count": encrypted_count, "rows": processed_rows}})
`);
return result;
"""


if st.button("Verwerken & opslaan", type="primary"):
    csv_base64 = base64.b64encode(df.to_csv(index=False).encode()).decode()
    js = _build_process_js(csv_base64, password, detected_sensitive, detected_regular)

    with st.spinner("Verwerken in uw browser (Pyodide)..."):
        raw = st_js_blocking(js, key="upload_personal_js")

    try:
        result = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        st.error("Verwerking in de browser is mislukt of niet voltooid. Probeer opnieuw.")
        st.stop()

    rows = result["rows"]
    encrypted_count = result["encrypted_count"]
    st.success(f"{len(rows)} rijen verwerkt, {encrypted_count} waarden versleuteld.")

    try:
        with st.spinner("Opslaan in database en object-opslag..."):
            store_result = personal_data.upload_personal_data(rows)
    except Exception as e:
        st.error(f"Fout bij opslaan: {e}")
        st.info("Controleer of de PostgreSQL- en MinIO-containers draaien (`docker compose up`).")
        st.stop()

    st.success(f"Opgeslagen: {store_result['inserted']} records.")
    st.json(store_result)

    st.download_button(
        "Verwerkt JSON downloaden (optioneel)",
        data=json.dumps(rows, indent=2).encode(),
        file_name="processed_personal_data.json",
        mime="application/json",
    )
    st.balloons()

# -----------------------------------------------------------------------------
# Info
# -----------------------------------------------------------------------------
st.markdown("---")
with st.expander("Hoe werkt dit?"):
    st.markdown(f"""
    **Dataflow:**
    1. CSV wordt in uw browser geladen
    2. Gevoelige kolommen worden gedetecteerd: {', '.join(SENSITIVE_FIELDS)}
    3. Het wachtwoord leidt een sleutel af (PBKDF2, 100.000 iteraties)
    4. UUID wordt afgeleid van Persoonsgebonden_nummer (MD5)
    5. Zeer gevoelige kolommen worden versleuteld (Fernet, AES-128-CBC)
    6. Alleen het **verwerkte** resultaat gaat naar de server

    **Opslag:**
    - `secret_sensitive` (PostgreSQL): UUID + versleutelde identifiers
    - `secret_regular` (PostgreSQL): UUID + demografische velden
    - MinIO: UUID + overige niet-gevoelige velden, als JSON-object
    """)

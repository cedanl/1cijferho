import base64
import json
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from eencijferho.io import personal_data

_JS_DIR = Path(__file__).parent / "js"


def _load_html(name: str, payload_b64: str) -> str:
    template = (_JS_DIR / name).read_text(encoding="utf-8")
    return template.replace("__PAYLOAD_B64__", payload_b64)

# -----------------------------------------------------------------------------
# Page Header
# -----------------------------------------------------------------------------
st.title("Persoonsgegevens bekijken")
st.markdown("""
<div class="page-intro">
    Bekijk bestanden uit MinIO op drie detailniveaus. Ontsleuteling van gevoelige
    velden gebeurt <strong>in uw browser</strong> — het wachtwoord verlaat uw browser nooit.
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# -----------------------------------------------------------------------------
# Step 1 — Select file
# -----------------------------------------------------------------------------
st.subheader("Stap 1 · Bestand kiezen")

try:
    files = personal_data.list_minio_files()
except Exception as e:
    st.error(f"Verbindingsfout met MinIO: {e}")
    st.info("Controleer of de MinIO-container draait (`docker compose up`).")
    st.stop()

if not files:
    st.info("Geen bestanden gevonden in MinIO. Upload eerst data via 'Persoonsgegevens uploaden'.")
    st.stop()

labels = [f"{f['name']} ({f['size']} bytes, {f['last_modified']})" for f in files]
idx = st.selectbox("Kies een bestand", range(len(files)), format_func=lambda i: labels[i])
selected_file = files[idx]["name"]

col1, col2 = st.columns([3, 1])
with col1:
    st.success(f"Geselecteerd: **{selected_file}**")
with col2:
    if st.button("Bestand verwijderen", type="secondary",
                 help="Verwijdert dit bestand én de bijbehorende databaserecords"):
        try:
            result = personal_data.delete_file(selected_file)
            st.success(f"Verwijderd: {result['deleted_records']} databaserecords + bestand.")
            st.info("Ververs de pagina om de bijgewerkte lijst te zien.")
            st.stop()
        except Exception as e:
            st.error(f"Fout bij verwijderen: {e}")

st.markdown("---")

# -----------------------------------------------------------------------------
# Step 2 — Select viewing mode
# -----------------------------------------------------------------------------
st.subheader("Stap 2 · Weergavemodus kiezen")

view_mode = st.radio(
    "Hoe wilt u dit bestand bekijken?",
    [
        "1. Ruwe inhoud (alleen UUID + overige velden)",
        "2. Met reguliere data (join met secret_regular)",
        "3. Met ontsleutelde data (wachtwoord vereist)",
    ],
)

st.markdown("---")
st.subheader("Stap 3 · Data bekijken")

# -----------------------------------------------------------------------------
# Mode 1 & 2 — server-side joins, rendered directly
# -----------------------------------------------------------------------------
if view_mode.startswith("1"):
    try:
        data = personal_data.view_file(selected_file, mode="raw")
        df = pd.DataFrame(data)
        st.write(f"**{len(df)} records** (UUID + overige velden, geen gevoelige data)")
        st.dataframe(df, use_container_width=True)
        st.download_button("Download als CSV", df.to_csv(index=False),
                           file_name="raw.csv", mime="text/csv")
    except Exception as e:
        st.error(f"Fout bij laden: {e}")

elif view_mode.startswith("2"):
    try:
        data = personal_data.view_file(selected_file, mode="with_regular")
        df = pd.DataFrame(data)
        st.write(f"**{len(df)} records** (samengevoegd met demografische data uit secret_regular)")
        st.dataframe(df, use_container_width=True)
        st.download_button("Download als CSV", df.to_csv(index=False),
                           file_name="with_regular.csv", mime="text/csv")
    except Exception as e:
        st.error(f"Fout bij laden: {e}")

# -----------------------------------------------------------------------------
# Mode 3 — client-side decryption with Pyodide
# -----------------------------------------------------------------------------
else:
    st.write("Dit ontsleutelt gevoelige identifiers (Persoonsgebonden_nummer, Burgerservice_nummer, Onderwijs_nummer).")
    password = st.text_input("Voer het ontsleutelwachtwoord in", type="password",
                             help="Het wachtwoord dat bij versleuteling is gebruikt.")
    if not password:
        st.info("Voer het ontsleutelwachtwoord in.")
        st.stop()

    st.warning("Ontsleuteling gebeurt in uw browser. Het wachtwoord wordt nooit naar de server gestuurd.")

    try:
        sensitive_fields = personal_data.get_schema()["sensitive_columns"]
        data = personal_data.view_file(selected_file, mode="with_encrypted")
    except Exception as e:
        st.error(f"Fout bij laden: {e}")
        st.stop()

    payload = json.dumps({
        "data_b64": base64.b64encode(json.dumps(data).encode()).decode(),
        "password": password,
        "sensitive_fields": sensitive_fields,
    })
    payload_b64 = base64.b64encode(payload.encode()).decode()
    decryption_html = _load_html("decrypt_personal.html", payload_b64)
    components.html(decryption_html, height=600, scrolling=True)

# -----------------------------------------------------------------------------
# Info
# -----------------------------------------------------------------------------
st.markdown("---")
with st.expander("Over de weergavemodi"):
    st.markdown("""
    **1. Ruwe inhoud** — precies wat in het MinIO-bestand staat (UUID + overige velden).

    **2. Met reguliere data** — join met `secret_regular` op UUID (demografische velden).

    **3. Met ontsleutelde data** — join met `secret_sensitive` (versleuteld), daarna
    client-side ontsleuteld met Pyodide. Het wachtwoord verlaat uw browser nooit.
    """)

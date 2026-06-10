import base64
import json

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from eencijferho.io import personal_data

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

    data_base64 = base64.b64encode(json.dumps(data).encode()).decode()
    password_escaped = password.replace("'", "\\'").replace('"', '\\"')
    sensitive_fields_json = json.dumps(sensitive_fields)

    decryption_html = f"""
<!DOCTYPE html>
<html>
<head>
    <script src="https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.js"></script>
    <style>
        body {{ font-family: 'Source Sans Pro', sans-serif; padding: 20px; }}
        .status {{ padding: 15px; margin: 10px 0; border-radius: 5px; font-size: 14px; }}
        .loading {{ background-color: #fff3cd; color: #856404; border: 1px solid #ffeeba; }}
        .success {{ background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }}
        .error {{ background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        #preview {{ max-height: 420px; overflow: auto; margin-top: 10px; }}
    </style>
</head>
<body>
    <div id="status" class="status loading">Pyodide initialiseren...</div>
    <div id="preview"></div>

    <script type="text/javascript">
        let pyodide;

        async function initPyodide() {{
            try {{
                document.getElementById('status').innerHTML = 'Pyodide laden (10-20 seconden)...';
                pyodide = await loadPyodide();
                document.getElementById('status').innerHTML = 'cryptography installeren...';
                await pyodide.loadPackage(['micropip']);
                await pyodide.runPythonAsync(`
                    import micropip
                    await micropip.install('cryptography')
                `);
                decryptData();
            }} catch (error) {{
                document.getElementById('status').innerHTML = 'Fout bij laden Pyodide: ' + error;
                document.getElementById('status').className = 'status error';
                console.error(error);
            }}
        }}

        async function decryptData() {{
            try {{
                document.getElementById('status').innerHTML = 'Data ontsleutelen in browser...';
                document.getElementById('status').className = 'status loading';

                pyodide.globals.set('data_input', atob('{data_base64}'));
                pyodide.globals.set('password_input', `{password_escaped}`);
                pyodide.globals.set('sensitive_fields_input', {sensitive_fields_json});

                const result = await pyodide.runPythonAsync(`
import json, base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

def derive_key(password, salt=b'eencijfer_salt_2025'):
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt,
                     iterations=100000, backend=default_backend())
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

cipher = Fernet(derive_key(password_input))
data = json.loads(data_input)
sensitive_fields = sensitive_fields_input
decrypted_count = 0
for row in data:
    for field in sensitive_fields:
        if field in row and row[field]:
            try:
                row[field] = cipher.decrypt(row[field].encode()).decode()
                decrypted_count += 1
            except Exception as e:
                row[field] = "DECRYPT_ERROR"
str(decrypted_count) + "||" + json.dumps(data)
                `);

                const parts = result.split('||', 2);
                const decrypted = JSON.parse(parts[1]);
                let html = '<strong>Ontsleuteld: ' + parts[0] + ' waarden.</strong><br><br>';
                if (decrypted.length > 0) {{
                    html += '<table><thead><tr>';
                    for (let key in decrypted[0]) html += '<th>' + key + '</th>';
                    html += '</tr></thead><tbody>';
                    for (let i = 0; i < Math.min(decrypted.length, 20); i++) {{
                        html += '<tr>';
                        for (let key in decrypted[i]) html += '<td>' + (decrypted[i][key] ?? '') + '</td>';
                        html += '</tr>';
                    }}
                    html += '</tbody></table>';
                    if (decrypted.length > 20)
                        html += '<br><em>Eerste 20 van ' + decrypted.length + ' records.</em>';
                }}
                document.getElementById('preview').innerHTML = html;
                document.getElementById('status').innerHTML = 'Klaar! ' + parts[0] + ' waarden ontsleuteld.';
                document.getElementById('status').className = 'status success';
            }} catch (error) {{
                document.getElementById('status').innerHTML =
                    'Ontsleutelfout: ' + error + ' — controleer het wachtwoord.';
                document.getElementById('status').className = 'status error';
                console.error(error);
            }}
        }}

        initPyodide();
    </script>
</body>
</html>
"""
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

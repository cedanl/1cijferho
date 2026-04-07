import streamlit as st
import os
import glob
from config import get_demo_mode, get_input_dir, get_decoder_input_dir

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def categorize_files() -> tuple[bool, dict[str, list[str]], int]:
    """Check if the input directory exists and categorize files found"""
    input_dir = get_input_dir()

    # Create directory if it doesn't exist
    os.makedirs(input_dir, exist_ok=True)

    if not os.path.exists(input_dir):
        return False, {}, 0

    # Get all files in the directory (any extension, excluding .zip files)
    all_files_paths = glob.glob(os.path.join(input_dir, "*"))
    # Also include decoder files from root input dir when in DEMO_MODE
    if get_demo_mode():
        all_files_paths += glob.glob(os.path.join(get_decoder_input_dir(), "Dec_*.asc"))

    # Filter out directories and .zip files, keep only regular files
    all_files = []
    seen = set()
    for file_path in all_files_paths:
        if os.path.isfile(file_path) and not file_path.lower().endswith('.zip'):
            basename = os.path.basename(file_path)
            if basename not in seen:
                seen.add(basename)
                all_files.append(basename)

    # Categorize files
    categorized_files = {
        "bestandsbeschrijvingen": [],
        "decodeer_files": [],
        "main_files": []
    }

    for filename in all_files:
        filename_lower = filename.lower()

        if filename_lower.endswith('.txt') and 'bestandsbeschrijving' in filename_lower:
            categorized_files["bestandsbeschrijvingen"].append(filename)
        elif filename.startswith('Dec_'):
            categorized_files["decodeer_files"].append(filename)
        elif (filename.startswith('EV') or
              filename.startswith('VAKHAVW') or
              filename.startswith('Croho') or
              filename.startswith('Croho_vest')):
            categorized_files["main_files"].append(filename)

    for category in categorized_files:
        categorized_files[category].sort()

    total_files = len(all_files)
    files_found = total_files > 0

    return files_found, categorized_files, total_files


# -----------------------------------------------------------------------------
# Page Header
# -----------------------------------------------------------------------------
st.title("Bestanden uploaden")

if get_demo_mode():
    col_info, col_switch = st.columns([3, 1])
    with col_info:
        st.info(f"**Demo modus** — bestanden worden geladen uit `{get_input_dir()}`.")
    with col_switch:
        if st.button("Eigen data gebruiken", type="secondary", use_container_width=True):
            st.switch_page("frontend/Overview/Home.py")
else:
    st.markdown(f"""
<div class="page-intro">
    Plaats uw DUO-bestanden in <code>{get_input_dir()}</code> en klik op 'Bestanden controleren'.
    Zet bestanden direct in de map — niet in submappen.
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Action Buttons
# -----------------------------------------------------------------------------
files_found, categorized_files, total_files = categorize_files()

col1, col2 = st.columns(2)
with col1:
    if st.button("Bestanden controleren", type="secondary", use_container_width=True):
        st.rerun()
with col2:
    if st.button("Ga door naar stap 1 →", type="primary", disabled=not files_found, use_container_width=True):
        st.switch_page("frontend/Modules/Extract_Metadata.py")

# -----------------------------------------------------------------------------
# Example Directory Structure (collapsed by default)
# -----------------------------------------------------------------------------
with st.expander("Welke bestanden heb ik nodig?"):
    st.markdown(f"""
Uw `{get_input_dir()}` map moet drie soorten bestanden bevatten:

| Type | Naampatroon | Voorbeeld |
|---|---|---|
| Bestandsbeschrijvingen | `Bestandsbeschrijving_*.txt` | `Bestandsbeschrijving_EV_2023.txt` |
| Decodeerbestanden | `Dec_*.asc` | `Dec_landcode.asc` |
| Hoofdbestanden | `EV_*.asc`, `VAKHAVW_*.asc` | `EV2023.asc` |
""")
    if os.path.exists("src/assets/example_files.png"):
        st.image("src/assets/example_files.png")

# -----------------------------------------------------------------------------
# File Detection Results
# -----------------------------------------------------------------------------
if not files_found:
    st.error(f"**Geen bestanden gevonden** in `{get_input_dir()}`. Kopieer uw uitgepakte DUO-bestanden naar deze map en klik op 'Bestanden controleren'.")
else:
    st.success(f"✅ **{total_files} bestanden gevonden** in `{get_input_dir()}`")

    col1, col2, col3 = st.columns(3)

    def _file_column(label: str, files: list[str]) -> None:
        count = len(files)
        status = "✅" if count > 0 else "—"
        st.markdown(f"**{label}** &nbsp; {status} &nbsp; `{count}`")
        if count > 0:
            with st.expander(f"Bekijk {count} bestanden"):
                for filename in files:
                    st.write(f"• `{filename}`")
        else:
            st.caption("Geen bestanden gevonden")

    with col1:
        _file_column("Bestandsbeschrijvingen", categorized_files["bestandsbeschrijvingen"])
    with col2:
        _file_column("Decodeerbestanden", categorized_files["decodeer_files"])
    with col3:
        _file_column("Hoofdbestanden", categorized_files["main_files"])

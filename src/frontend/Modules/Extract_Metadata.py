import streamlit as st
import os
import glob
import eencijferho.core.extractor as ex
import io
import contextlib
from config import get_input_dir, get_metadata_dir


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def clear_existing_files() -> list[str]:
    """Clear existing metadata files before starting new extraction"""
    metadata_dir = get_metadata_dir()
    json_dir = os.path.join(metadata_dir, "json")

    files_cleared = []

    if os.path.exists(metadata_dir):
        for file_path in glob.glob(os.path.join(metadata_dir, "*.xlsx")):
            try:
                os.remove(file_path)
                files_cleared.append(os.path.basename(file_path))
            except Exception as e:
                print(f"Warning: Could not remove {file_path}: {e}")

    if os.path.exists(json_dir):
        for file_path in glob.glob(os.path.join(json_dir, "*.json")):
            try:
                os.remove(file_path)
                files_cleared.append(f"json/{os.path.basename(file_path)}")
            except Exception as e:
                print(f"Warning: Could not remove {file_path}: {e}")

    return files_cleared


def get_bestandsbeschrijvingen() -> list[str]:
    """Get all bestandsbeschrijving files from the input directory"""
    input_dir = get_input_dir()
    if not os.path.exists(input_dir):
        return []

    return sorted([
        os.path.basename(f)
        for f in glob.glob(os.path.join(input_dir, "*.txt"))
        if "bestandsbeschrijving" in os.path.basename(f).lower()
    ])


# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------
st.markdown('<span class="step-badge">Stap 1 van 3</span>', unsafe_allow_html=True)
st.title("Metadata extraheren")
st.markdown("""
<div class="page-intro">
    Lees de veldposities uit uw bestandsbeschrijvingen. Dit maakt de "kaart" waarmee de vaste-breedte bestanden correct worden gesplitst.
</div>
""", unsafe_allow_html=True)

with st.expander("Wat doet deze stap precies?"):
    st.markdown("""
- Veldposities extraheren uit de `.txt`-bestandsbeschrijvingen
- Resultaten opslaan als JSON en Excel in `data/00-metadata/`
- Variabelenoverzicht aanmaken voor de conversiestap
""")

with st.expander("Heb ik al geëxtraheerde bestanden?"):
    st.markdown(f"""
Als u al gestructureerde bestandsbeschrijvingen heeft ontvangen, plaatst u ze direct in `data/00-metadata/`
en slaat u deze stap over. Voer de extractie **niet** uit — bestaande bestanden worden anders overschreven.

Zorg dat de bestandsnamen overeenkomen, bijv. `Dec_land_naar_herkomstindikking.asc.xlsx`
met de kolommen `Startpositie`, `Aantal posities` en `Opmerking`.
""")

# -----------------------------------------------------------------------------
# Status + Actions
# -----------------------------------------------------------------------------
bestandsbeschrijvingen = get_bestandsbeschrijvingen()

if not bestandsbeschrijvingen:
    st.error("**Geen bestandsbeschrijvingen gevonden.** Controleer of de juiste DUO-bestanden in de invoermap staan en ga terug naar de vorige stap.")
else:
    st.success(f"✅ **{len(bestandsbeschrijvingen)} bestandsbeschrijving(en) gevonden**")

    # Warn about existing files — before the action button
    _metadata_dir_pre = get_metadata_dir()
    if os.path.exists(_metadata_dir_pre) and os.listdir(_metadata_dir_pre):
        st.warning("⚠️ Er zijn al eerder geëxtraheerde bestanden aanwezig. Een nieuwe extractie overschrijft deze.")

    # Check if extraction already completed
    metadata_dir = get_metadata_dir()
    logs_dir = os.path.join(metadata_dir, "logs")
    extraction_complete = False
    if os.path.exists(metadata_dir) and os.listdir(metadata_dir) and os.path.exists(logs_dir):
        xlsx_processing_log_files = glob.glob(os.path.join(logs_dir, "*xlsx_processing_log_latest.json"))
        extraction_complete = len(xlsx_processing_log_files) > 0

    col1, col2 = st.columns(2)
    with col1:
        extract_clicked = st.button("Extraheren starten", type="primary", use_container_width=True)
    with col2:
        validate_clicked = st.button(
            "Ga door naar stap 2 →",
            type="secondary",
            disabled=not extraction_complete,
            use_container_width=True,
        )

    if validate_clicked:
        st.switch_page("frontend/Modules/Validate_Metadata.py")

    # -----------------------------------------------------------------------------
    # Extraction Logic
    # -----------------------------------------------------------------------------
    if extract_clicked:
        st.session_state.extract_console_log = ""

        with st.spinner("Bezig met extraheren..."):
            try:
                st.session_state.extract_console_log += "Extractie gestart...\n"
                st.session_state.extract_console_log += "Bestaande metadata-bestanden verwijderen...\n"
                cleared_files = clear_existing_files()
                if cleared_files:
                    st.session_state.extract_console_log += f"✅ {len(cleared_files)} bestaande bestanden verwijderd{': ' + ', '.join(cleared_files[:3]) + ('...' if len(cleared_files) > 3 else '')}\n"
                else:
                    st.session_state.extract_console_log += "✅ Geen bestaande bestanden te verwijderen\n"

                st.session_state.extract_console_log += "Bestandsbeschrijvingen verwerken...\n"
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    metadata_dir = get_metadata_dir()
                    json_dir = os.path.join(metadata_dir, "json")
                    extracted_files = ex.process_txt_folder(get_input_dir(), json_output_folder=json_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                dec_matches = [f for f in (extracted_files or []) if "Dec-bestanden" in f]
                if dec_matches:
                    st.session_state.dec_metadata_json = dec_matches[0]
                st.session_state.extract_console_log += "✅ Bestandsbeschrijvingen verwerkt\n"

                st.session_state.extract_console_log += "Variabelenoverzicht aanmaken...\n"
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex.write_variable_metadata(input_dir=get_input_dir(), json_folder=json_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                st.session_state.extract_console_log += "✅ Variabelenoverzicht aangemaakt\n"

                st.session_state.extract_console_log += "Excel-bestanden aanmaken...\n"
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex.process_json_folder(json_input_folder=json_dir, excel_output_folder=metadata_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                st.session_state.extract_console_log += "✅ Excel-bestanden aangemaakt\n"
                st.session_state.extract_console_log += "✅ Extractie succesvol afgerond\n"

                st.success("✅ **Extractie voltooid.** Ga door naar stap 2 om de metadata te valideren.")
                st.rerun()

            except Exception as e:
                st.session_state.extract_console_log += f"❌ Fout: {str(e)}\n"
                st.error("❌ **Extractie mislukt.** Bekijk het console log hieronder voor details.")
                with st.expander("Technische foutdetails"):
                    st.code(str(e))

    # Console Log
    _log_has_content = "extract_console_log" in st.session_state and bool(st.session_state.extract_console_log)
    with st.expander("Console Log", expanded=_log_has_content):
        if _log_has_content:
            st.code(st.session_state.extract_console_log, language=None)
        else:
            st.caption("Nog geen extractieproces gestart.")

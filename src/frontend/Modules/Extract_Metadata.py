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

    # Clear .xlsx files from metadata directory
    if os.path.exists(metadata_dir):
        xlsx_files = glob.glob(os.path.join(metadata_dir, "*.xlsx"))
        for file_path in xlsx_files:
            try:
                os.remove(file_path)
                files_cleared.append(os.path.basename(file_path))
            except Exception as e:
                print(f"Warning: Could not remove {file_path}: {e}")

    # Clear JSON files from json subfolder
    if os.path.exists(json_dir):
        json_files = glob.glob(os.path.join(json_dir, "*.json"))
        for file_path in json_files:
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

    txt_files = glob.glob(os.path.join(input_dir, "*.txt"))
    bestandsbeschrijvingen = []

    for file_path in txt_files:
        filename = os.path.basename(file_path)
        if "bestandsbeschrijving" in filename.lower():
            bestandsbeschrijvingen.append(filename)

    return sorted(bestandsbeschrijvingen)


# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("🔍 Metadata extraheren")

# Intro text
st.write("""
**Stap 1: Structuur van data extraheren**

We lezen nu uw Bestandsbeschrijving-bestanden om te bepalen waar elk veld staat in uw hoofd- en dec-bestanden. Dit vormt de "kaart" die nodig is om uw vaste-breedte data correct te splitsen.

Wat gebeurt er:
- Veldposities extraheren uit uw .txt-bestanden
- Omzetten naar JSON-formaat, daarna Excel
- Opslaan als bestandsbeschrijvingen klaar voor validatie
""")

with st.expander("🚨 Bestaande Bestandsbeschrijvingen"):
    st.markdown("""
    Heeft u al gestructureerde Bestandsbeschrijving-bestanden ontvangen? Plaats deze dan in `data/00-metadata/`
    en ga door naar stap 2. Voer de extractie niet uit, want bestaande bestanden worden overschreven.
                
    Zorg ervoor dat de bestandsnamen overeenkomen met de inputbestanden, bijvoorbeeld `Dec_land_naar_herkomstindikking.asc.xlsx` 
    met de kolommen `Startpositie`, `Aantal posities` en `Opmerking`.
    """)

# Get files and display status
bestandsbeschrijvingen = get_bestandsbeschrijvingen()

if not bestandsbeschrijvingen:
    st.error("🚨 **Geen bestandsbeschrijvingen gevonden.** Zorg dat u de juiste DUO-bestanden in de invoermap hebt geplaatst en ververs de pagina.")
else:
    st.success(
        f"✅ **{len(bestandsbeschrijvingen)} Bestandsbeschrijving-bestanden gevonden**"
    )
    st.info("💡 Klopt het aantal bestanden niet? Controleer of alle DUO-bestanden in de invoermap staan.")

    # Side-by-side buttons with equal width
    col1, col2 = st.columns(2)

    with col1:
        extract_clicked = st.button(
            "🔍 Start met extraheren", type="primary", use_container_width=True
        )

    with col2:
        # Check if metadata exists to enable/disable the validate button
        metadata_dir = get_metadata_dir()
        logs_dir = os.path.join(metadata_dir, "logs")
        extraction_complete = False

        if (
            os.path.exists(metadata_dir)
            and os.listdir(metadata_dir)
            and os.path.exists(logs_dir)
        ):
            # Check for the required extraction log file
            xlsx_processing_log_files = glob.glob(
                os.path.join(logs_dir, "*xlsx_processing_log_latest.json")
            )
            extraction_complete = len(xlsx_processing_log_files) > 0

        validate_clicked = st.button(
            "➡️ Ga door naar stap 2",
            type="secondary",
            disabled=not extraction_complete,
            use_container_width=True,
        )

    # Handle validate button click
    if validate_clicked:
        st.switch_page("frontend/Modules/Validate_Metadata.py")

    # Handle extraction logic
    if extract_clicked:
        # Reset console log at the start of each extraction
        st.session_state.extract_console_log = ""

        with st.spinner("Bezig met extraheren..."):
            try:
                st.session_state.extract_console_log += (
                    "🔄 Extractie gestart...\n"
                )

                # Clear existing files first
                st.session_state.extract_console_log += (
                    "🧹 Bestaande metadata-bestanden verwijderen...\n"
                )
                cleared_files = clear_existing_files()
                if cleared_files:
                    st.session_state.extract_console_log += f"✅ {len(cleared_files)} bestaande bestanden verwijderd{': ' + ', '.join(cleared_files[:3]) + ('...' if len(cleared_files) > 3 else '')}\n"
                else:
                    st.session_state.extract_console_log += (
                        "✅ Geen bestaande bestanden te verwijderen\n"
                    )

                st.session_state.extract_console_log += "📁 Bestandsbeschrijvingen verwerken...\n"

                # Capture stdout from process_txt_folder
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    metadata_dir = get_metadata_dir()
                    json_dir = os.path.join(metadata_dir, "json")
                    extracted_files = ex.process_txt_folder(get_input_dir(), json_output_folder=json_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                dec_matches = [f for f in (extracted_files or []) if "Dec-bestanden" in f]
                if dec_matches:
                    st.session_state.dec_metadata_json = dec_matches[0]
                st.session_state.extract_console_log += (
                    "✅ Bestandsbeschrijvingen verwerkt\n"
                )

                # Write consolidated variable metadata
                st.session_state.extract_console_log += (
                    "📦 Variabelenoverzicht aanmaken...\n"
                )
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex.write_variable_metadata(input_dir=get_input_dir(), json_folder=json_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                st.session_state.extract_console_log += (
                    "✅ Variabelenoverzicht aangemaakt\n"
                )

                st.session_state.extract_console_log += (
                    "📊 Excel-bestanden aanmaken...\n"
                )
                # Capture stdout from process_json_folder
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex.process_json_folder(json_input_folder=json_dir, excel_output_folder=metadata_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                st.session_state.extract_console_log += "✅ Excel-bestanden aangemaakt\n"
                st.session_state.extract_console_log += (
                    "🎉 Extractie succesvol afgerond!\n"
                )

                st.success(
                    "✅ **Extractie voltooid!** U kunt nu de metadata valideren."
                )

                # Rerun to update the validate button state
                st.rerun()

            except Exception as e:
                st.session_state.extract_console_log += f"❌ Fout: {str(e)}\n"
                st.error(
                    "❌ **Extractie mislukt.** Controleer of alle benodigde bestanden aanwezig zijn en probeer het opnieuw. Bekijk het console log hieronder voor meer details."
                )
                with st.expander("🔍 Technische foutdetails"):
                    st.code(str(e))

    # Console Log expander
    with st.expander("📋 Console Log", expanded=True):
        if (
            "extract_console_log" in st.session_state
            and st.session_state.extract_console_log
        ):
            st.code(st.session_state.extract_console_log, language=None)
        else:
            st.info(
                "Nog geen extractieproces gestart. Klik op 'Start met extraheren' om te beginnen."
            )

    # Warning about existing files
    _metadata_dir = get_metadata_dir()
    if os.path.exists(_metadata_dir) and os.listdir(_metadata_dir):
        st.warning(
            "⚠️ Er zijn al eerder geëxtraheerde bestanden aanwezig. Een nieuwe extractie overschrijft deze."
        )

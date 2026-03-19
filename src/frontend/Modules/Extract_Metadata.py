import streamlit as st
import os
import glob
import eencijferho.core.extractor as ex
import io
import contextlib
from typing import List
from config import get_input_dir, get_metadata_dir


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def clear_existing_files() -> List[str]:
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


def get_bestandsbeschrijvingen() -> List[str]:
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
- Opslaan in de map `data/00-metadata/`
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

input_dir = get_input_dir()
if not bestandsbeschrijvingen:
    st.error(f"🚨 **Geen Bestandsbeschrijving-bestanden gevonden in `{input_dir}`**")
else:
    st.success(
        f"✅ **{len(bestandsbeschrijvingen)} Bestandsbeschrijving-bestanden gevonden**"
    )
    st.info("💡 U kunt doorgaan, ook als er fouten zijn - doe dit met voorzichtigheid!")

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
                    "🔄 Starting extraction process...\n"
                )

                # Clear existing files first
                st.session_state.extract_console_log += (
                    "🧹 Clearing existing metadata files...\n"
                )
                cleared_files = clear_existing_files()
                if cleared_files:
                    st.session_state.extract_console_log += f"✅ Cleared {len(cleared_files)} existing files: {', '.join(cleared_files[:3])}{'...' if len(cleared_files) > 3 else ''}\n"
                else:
                    st.session_state.extract_console_log += (
                        "✅ No existing files to clear\n"
                    )

                st.session_state.extract_console_log += "📁 Processing TXT files...\n"

                # Capture stdout from process_txt_folder
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    metadata_dir = get_metadata_dir()
                    json_dir = os.path.join(metadata_dir, "json")
                    ex.process_txt_folder(get_input_dir(), json_output_folder=json_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                st.session_state.extract_console_log += (
                    "✅ TXT files processed successfully\n"
                )

                # Write consolidated variable metadata
                st.session_state.extract_console_log += (
                    "📦 Creating variable_metadata.json...\n"
                )
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex.write_variable_metadata(input_dir=get_input_dir(), json_folder=json_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                st.session_state.extract_console_log += (
                    "✅ variable_metadata.json created\n"
                )

                st.session_state.extract_console_log += (
                    "📊 Converting JSON to Excel...\n"
                )
                # Capture stdout from process_json_folder
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex.process_json_folder(json_input_folder=json_dir, excel_output_folder=metadata_dir)
                st.session_state.extract_console_log += captured_output.getvalue()
                st.session_state.extract_console_log += "✅ JSON conversion completed\n"
                st.session_state.extract_console_log += (
                    "🎉 Extraction completed successfully!\n"
                )

                st.success(
                    "✅ **Extractie voltooid!** U kunt nu de metadata valideren."
                )

                # Rerun to update the validate button state
                st.rerun()

            except Exception as e:
                st.session_state.extract_console_log += f"❌ Error: {str(e)}\n"
                metadata_dir = get_metadata_dir()
                st.error(
                    f"❌ **Extractie mislukt:** {str(e)}"
                )

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
            f"⚠️ Extractie zal bestaande bestanden in `{_metadata_dir}/` overschrijven"
        )
